package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add your RPC definitions here.

type GetTaskReq struct {
	WorkerId int
}

type GetTaskResp struct {
	MapFilename   string
	ReduceSource  []KeyValue
	NReduce       int
	ReduceTaskNum int
}

type SubmitMapReq struct {
	WorkerId      int
	Filename      string
	ReduceSources [][]KeyValue
}

type SubmitReduceReq struct {
	WorkerId     int
	ReduceTask   int
	ReduceResult []KeyValue
}

type NilResp struct {
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
