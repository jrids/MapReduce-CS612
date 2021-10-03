package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	rand.Seed(time.Now().UnixNano())
	workerId := int(rand.Int31())

	for {
		switch reply := callGetTask(workerId); reply.TaskType {
		case 0:

			x := mapFunc(mapf, reply.TaskNum, reply.Partitions, reply.Filename)
			if x {
				callFinishTask(reply.TaskType, workerId, reply.TaskNum)
			}
		case 1:

			x := reduceFunc(reducef, reply.TaskNum, reply.Partitions)
			if x {
				callFinishTask(reply.TaskType, workerId, reply.TaskNum)
			}
		case 3:
			return
		default:
			time.Sleep(3 * time.Second)
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func mapFunc(mapf func(string, string) []KeyValue,
	taskNum int,
	partitions int,
	filename string) bool {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return false
	}
	file.Close()
	kva := mapf(filename, string(content))

	sort.Sort(ByKey(kva))

	intermediate := make(map[int][]KeyValue)

	for _, keyval := range kva {
		keyhash := ihash(keyval.Key) % partitions
		intermediate[keyhash] = append(intermediate[keyhash], keyval)
	}

	for i := 0; i < partitions; i++ {
		oname := "mr-" + strconv.Itoa(taskNum) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		enc.Encode(intermediate[i])
		ofile.Close()
	}

	return true
}

func reduceFunc(reducef func(string, []string) string,
	taskNum int,
	partitions int) bool {

	kva := make(map[string][]string)
	for i := 0; i < partitions; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, taskNum)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return false
		}

		dec := json.NewDecoder(file)
		var kv []KeyValue
		if err := dec.Decode(&kv); err != nil {
			log.Fatalf("cannot read %v", filename)
			return false
		}

		for _, keyval := range kv {
			kva[keyval.Key] = append(kva[keyval.Key], keyval.Value)
		}

		file.Close()
	}

	var output []KeyValue
	for key, val := range kva {
		output = append(output, KeyValue{key, reducef(key, val)})
	}

	filename := fmt.Sprintf("mr-out-%v", taskNum)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return false
	}

	for _, keyval := range output {
		fmt.Fprintf(file, "%v %v\n", keyval.Key, keyval.Value)
	}

	return true
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func callGetTask(workerId int) *GetReply {
	args, reply := GetArgs{workerId}, GetReply{}

	x := call("Coordinator.GetTask", &args, &reply)

	if !x {
		fmt.Printf("Could not reach master\n")
		reply.TaskType = 99
		return &reply
	}

	return &reply
}

func callFinishTask(taskType, workerId, taskId int) {
	args, reply := FinishArgs{taskType, workerId, taskId}, FinishReply{}
	call("Coordinator.FinishTask", &args, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
