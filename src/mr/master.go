package mr

import (
	"fmt"
	"log"
	"strconv"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mapTasks  []int
	filenames []string

	reduceTasks   []int
	reduceSources [][]KeyValue // row: reduce task.
	nReduce       int

	reduceResults [][]KeyValue
}

const (
	PENDING      = 0
	DONE         = 2
	WorkerOffset = 10000
)

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

func (m *Master) GetTask(args *GetTaskReq, resp *GetTaskResp) error {
	isMapDone := true

	for i := range m.mapTasks {
		if m.mapTasks[i] == PENDING {
			resp.MapFilename = m.filenames[i]
			resp.ReduceSource = nil
			resp.NReduce = m.nReduce
			resp.ReduceTaskNum = 0
			m.mapTasks[i] = WorkerOffset + args.WorkerId

			go func(m *Master) {

			}(m)

			fmt.Printf("MAP task %s assigned to worker %d\n", m.filenames[i], args.WorkerId)
			return nil
		}

		if m.mapTasks[i] != DONE {
			fmt.Printf("MAP %d not done yet\n", i)
			isMapDone = false
		}
	}

	if !isMapDone {
		return nil
	}
	for i := range m.reduceTasks {
		if m.reduceTasks[i] == PENDING {
			resp.MapFilename = ""
			resp.ReduceSource = m.reduceSources[i]
			resp.NReduce = m.nReduce
			resp.ReduceTaskNum = i
			m.reduceTasks[i] = WorkerOffset + args.WorkerId

			fmt.Printf("REDUCE task %d assigned to worker %d\n", i, args.WorkerId)
			return nil
		}
	}
	return nil
}

func (m *Master) SubmitMap(args *SubmitMapReq, resp *NilResp) error {
	for i, name := range m.filenames {
		if name == args.Filename && m.mapTasks[i] == WorkerOffset+args.WorkerId {
			m.mapTasks[i] = DONE
		}
	}
	for i, source := range args.ReduceSources {
		m.reduceSources[i] = append(m.reduceSources[i], source...) // concat reduce tasks
	}
	return nil
}

func (m *Master) SubmitReduce(args *SubmitReduceReq, resp *NilResp) error {
	if m.reduceTasks[args.ReduceTask] == WorkerOffset+args.WorkerId {
		m.reduceTasks[args.ReduceTask] = DONE
	}
	m.reduceResults = append(m.reduceResults, args.ReduceResult)

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
	// Your code here.
	for i := range m.mapTasks {
		if m.mapTasks[i] != DONE {
			fmt.Printf("MAP %d not finished\n", i)
			return false
		}
	}
	for i := range m.reduceTasks {
		if m.reduceTasks[i] != DONE {
			fmt.Printf("REDUCE %d not finished\n", i)
			return false
		}
	}

	for i, result := range m.reduceResults {
		oname := "mr-out-"
		ofile, _ := os.Create(oname + strconv.Itoa(i))
		for _, line := range result {
			fmt.Fprintf(ofile, "%v %v\n", line.Key, line.Value)
		}
		ofile.Close()
	}

	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{nReduce: nReduce}

	// Your code here.

	m.filenames = files
	m.mapTasks = make([]int, len(files))
	m.reduceSources = make([][]KeyValue, nReduce)
	m.reduceTasks = make([]int, nReduce)

	m.server()
	return &m
}
