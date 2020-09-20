package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()

	for {
		task := CallGetTask()
		//fmt.Printf("TASK: file=%s, reduceN=%d\n", task.MapFilename, task.ReduceTaskNum)
		if task.MapFilename != "" {
			nReduce := task.NReduce
			file, err := os.Open(task.MapFilename)
			if err != nil {
				log.Fatalf("cannot open %v", task.MapFilename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.MapFilename)
			}
			file.Close()

			kva := mapf("", string(content))
			result := make([][]KeyValue, nReduce)
			for i := range result {
				result[i] = make([]KeyValue, 0)
			}
			for _, kv := range kva {
				reduceTaskNum := ihash(kv.Key) % nReduce
				result[reduceTaskNum] = append(result[reduceTaskNum], kv)
			}
			CallSubmitMap(task.MapFilename, result)
		} else if task.ReduceSource != nil {
			fmt.Printf("REDUCE TASK %d\n", task.ReduceTaskNum)
			sort.Sort(ByKey(task.ReduceSource))
			var reduceResult []KeyValue

			i := 0
			for i < len(task.ReduceSource) {
				j := i + 1
				for j < len(task.ReduceSource) && task.ReduceSource[j].Key == task.ReduceSource[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, task.ReduceSource[k].Value)
				}
				output := reducef(task.ReduceSource[i].Key, values)
				reduceResult = append(reduceResult, KeyValue{task.ReduceSource[i].Key, output})

				i = j
			}
			fmt.Printf("REDUCE TASK %d DONE\n", task.ReduceTaskNum)

			CallSubmitReduce(task.ReduceTaskNum, reduceResult)

		} else {
			time.Sleep(200 * 1000 * 1000)

		}
	}

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallGetTask() *GetTaskResp {
	req := GetTaskReq{WorkerId: 9}
	resp := GetTaskResp{}
	call("Master.GetTask", &req, &resp)
	return &resp
}

func CallSubmitMap(filename string, reduceSources [][]KeyValue) {
	req := SubmitMapReq{9, filename, reduceSources}
	resp := NilResp{}
	call("Master.SubmitMap", &req, &resp)
}

func CallSubmitReduce(reduceTask int, reduceResult []KeyValue) {
	req := SubmitReduceReq{9, reduceTask, reduceResult}
	resp := NilResp{}
	call("Master.SubmitReduce", &req, &resp)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
