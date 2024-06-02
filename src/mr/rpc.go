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

type TaskType int

const (
	NoAvailableTaskType TaskType = iota
	MapTaskType
	ReduceTaskType
)

// Add your RPC definitions here.

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	TaskType       TaskType
	MapTaskBody    MapTaskBody
	ReduceTaskBody ReduceTaskBody
}

type MapTaskBody struct {
	TaskNumber int
	SplitFile  string
	NReduce    int
}

type ReduceTaskBody struct {
	TaskNumber        int
	IntermediateFiles []string
}

type FinishMapTaskArgs struct {
	TaskNumber        int
	IntermediateFiles []string
}

type FinishMapTaskReply struct {
}

type FinishReduceTaskArgs struct {
	TaskNumber int
	OutputFile string
}

type FinishReduceTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
