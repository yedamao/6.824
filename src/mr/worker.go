package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	obj := worker{
		mapf:    mapf,
		reducef: reducef,
	}

	for obj.RequestTask() {
		//time.Sleep(1 * time.Second)
	}
}

type worker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *worker) RequestTask() bool {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	if succ := call("Master.RequestTask", &args, &reply); !succ {
		return false
	}

	if reply.TaskType == NoAvailableTaskType {
		time.Sleep(1 * time.Second)
		return true
	}

	fmt.Printf("reply.TaskType %v\n", reply.TaskType)
	fmt.Printf("reply.MapTaskBody %v\n", reply.MapTaskBody)
	fmt.Printf("reply.ReduceTaskBody %v\n", reply.ReduceTaskBody)

	if reply.TaskType == MapTaskType {
		intermediateFiles := w.handleMapTask(reply.MapTaskBody)
		w.FinishMapTask(reply.MapTaskBody.TaskNumber, intermediateFiles)
	}

	if reply.TaskType == ReduceTaskType {
		w.handleReduceTask(reply.ReduceTaskBody)
		w.FinishReduceTask(reply.ReduceTaskBody.TaskNumber)
	}

	return true
}

// handleMapTask
func (w *worker) handleMapTask(t MapTaskBody) []string {

	// read split file
	content, err := ioutil.ReadFile(t.SplitFile)
	if err != nil {
		log.Fatalf("cannot open %v", t.SplitFile)
		return nil
	}

	// map it
	kva := w.mapf(t.SplitFile, string(content))

	// partition the intermediate key/value pairs into R regions
	regions := make([][]KeyValue, t.NReduce)
	for _, kv := range kva {
		reduceTaskNumber := ihash(kv.Key) % t.NReduce
		regions[reduceTaskNumber] = append(regions[reduceTaskNumber], kv)
	}

	var intermediateFiles []string
	for reduceTaskNumber, kvs := range regions {
		f, err := ioutil.TempFile("./", "map-*")
		if err != nil {
			panic(err)
		}

		enc := json.NewEncoder(f)
		for _, kv := range kvs {
			if err := enc.Encode(&kv); err != nil {
				panic(err)
			}
		}

		if err := f.Close(); err != nil {
			panic(err)
		}

		intermediateFileName := fmt.Sprintf("mr-%d-%d", t.TaskNumber, reduceTaskNumber)
		if err := os.Rename(f.Name(), intermediateFileName); err != nil {
			panic(err)
		}

		intermediateFiles = append(intermediateFiles, intermediateFileName)
	}

	fmt.Println(intermediateFiles)

	return intermediateFiles
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (w *worker) handleReduceTask(t ReduceTaskBody) {
	// read intermediate files
	intermediate := []KeyValue{}

	for _, filename := range t.IntermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// sort by key
	sort.Sort(ByKey(intermediate))

	// reduce it
	oname := fmt.Sprintf("mr-out-%d", t.TaskNumber)

	ofile, err := ioutil.TempFile("./", "reduce-*")
	if err != nil {
		panic(err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	if err := os.Rename(ofile.Name(), oname); err != nil {
		panic(err)
	}
}

func (w *worker) FinishMapTask(taskNumber int, intermediateFiles []string) {
	args := FinishMapTaskArgs{
		TaskNumber:        taskNumber,
		IntermediateFiles: intermediateFiles,
	}
	reply := FinishMapTaskReply{}

	call("Master.FinishMapTask", &args, &reply)
}

func (w *worker) FinishReduceTask(taskNumber int) {
	args := FinishReduceTaskArgs{
		TaskNumber: taskNumber,
	}
	reply := FinishReduceTaskReply{}

	call("Master.FinishReduceTask", &args, &reply)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
