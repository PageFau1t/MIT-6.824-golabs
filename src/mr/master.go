package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mapTasks      []int
	filenames     []string
	reduceTasks   []int
	reduceSources [][]KeyValue
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) SubmitMap(args *SubmitMapReq, resp interface{}) error {
	for i, source := range args.reduceSources {
		m.reduceSources[i] = append(m.reduceSources[i], source...)
	}
	for i, name := range m.filenames {
		if name == args.filename {
			m.mapTasks[i] = 1
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.mapTasks = make([]int, len(files))
	m.reduceSources = make([][]KeyValue, nReduce)
	m.filenames = files

	m.server()
	return &m
}
