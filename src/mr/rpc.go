package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetArgs struct {
	WorkerId int
}

type GetReply struct {
	// 0 - map, 1 - reduce, 2 - pending, 3 - done
	TaskType   int
	TaskNum    int
	Filename   string
	Partitions int
}

type FinishArgs struct {
	// 0 - map, 1 - reduce
	TaskType int
	WorkerId int
	TaskNum  int
}

type FinishReply struct {
}

// Add your RPC definitions here.

func coordinatorSock() string {
	s := "/var/tmp/cs612-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
