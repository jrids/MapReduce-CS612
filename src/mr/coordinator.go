package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mTasks        []mapTask
	rTasks        []reduceTask
	pendingMap    map[int]int
	pendingReduce map[int]int
	mapDone       int
	reduceDone    int
	mapLock       sync.Mutex
	reduceLock    sync.Mutex
	// Your definitions here.

}

type mapTask struct {
	done     bool
	worker   int
	filename string
}

type reduceTask struct {
	done   bool
	worker int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetArgs, reply *GetReply) error {
	//for worker getting pending map tasks

	c.mapLock.Lock()
	for id, _ := range c.pendingMap {
		reply.TaskType = 0
		reply.TaskNum = id
		reply.Filename = c.mTasks[id].filename
		reply.Partitions = len(c.rTasks)

		// update lists
		c.mTasks[id].worker = args.WorkerId
		delete(c.pendingMap, id)
		// Run waiting thread
		go waitCheck(c, 0, id)

		c.mapLock.Unlock()
		return nil
	}
	c.mapLock.Unlock()

	// pending map tasks
	if c.mapDone != len(c.mTasks) {
		reply.TaskType = 2
		return nil
	}

	c.reduceLock.Lock()
	// for worker getting reduce tasks
	for id, _ := range c.pendingReduce {
		reply.TaskType = 1
		reply.TaskNum = id
		reply.Partitions = len(c.mTasks)

		// update lists
		c.rTasks[id].worker = args.WorkerId
		delete(c.pendingReduce, id)
		// Run waiting thread
		go waitCheck(c, 1, id)

		c.reduceLock.Unlock()
		return nil
	}
	c.reduceLock.Unlock()

	if c.reduceDone != len(c.rTasks) {
		//pending tasks
		reply.TaskType = 2
		return nil
	} else {
		reply.TaskType = 3
		return nil
	}
}

func (c *Coordinator) FinishTask(args *FinishArgs, reply *FinishReply) error {
	if args.TaskType == 0 {
		// Map task finished
		c.mapLock.Lock()
		if c.mTasks[args.TaskNum].worker == args.WorkerId {
			c.mTasks[args.TaskNum].done = true
			c.mTasks[args.TaskNum].worker = -1
			c.mapDone++
		}
		c.mapLock.Unlock()
		return nil
	} else {
		// Reduce task finished
		c.reduceLock.Lock()
		if c.rTasks[args.TaskNum].worker == args.WorkerId {
			c.rTasks[args.TaskNum].done = true
			c.rTasks[args.TaskNum].worker = -1
			c.reduceDone++
		}
		c.reduceLock.Unlock()
		return nil
	}
}

func waitCheck(c *Coordinator, taskType int, taskNum int) {
	time.Sleep(10 * time.Second)

	if taskType == 0 {
		c.mapLock.Lock()
		if !c.mTasks[taskNum].done {
			defer fmt.Printf("Worker %v took too long to respond for map task %v\n",
				c.mTasks[taskNum].worker, taskNum)
			c.mTasks[taskNum].worker = -1
			c.pendingMap[taskNum] = taskNum
		}
		c.mapLock.Unlock()
	} else {
		c.reduceLock.Lock()
		if !c.rTasks[taskNum].done {
			defer fmt.Printf("Worker %v took too long to respond for reduce task %v\n",
				c.rTasks[taskNum].worker, taskNum)
			c.rTasks[taskNum].worker = -1
			c.pendingReduce[taskNum] = taskNum
		}
		c.reduceLock.Unlock()
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mapLock.Lock()
	c.reduceLock.Lock()
	ret := c.mapDone == len(c.mTasks) &&
		c.reduceDone == len(c.rTasks)
	c.reduceLock.Unlock()
	c.mapLock.Unlock()

	// Your code here.
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// all map tasks
	c.mTasks = make([]mapTask, len(files))
	c.pendingMap = make(map[int]int)
	c.mapDone = 0

	for i, _ := range c.mTasks {
		c.mTasks[i] = mapTask{false, -1, files[i]}
		c.pendingMap[i] = i
	}

	// all reduce tasks
	c.rTasks = make([]reduceTask, nReduce)
	c.pendingReduce = make(map[int]int)
	c.reduceDone = 0

	for i, _ := range c.rTasks {
		c.rTasks[i] = reduceTask{false, -1}
		c.pendingReduce[i] = i
	}

	// Your code here.
	c.server()
	return &c
}
