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

type taskState int

const (
	Idle taskState = iota
	InProgress
	Completed
)

// Therefore, for each completed map task, the master stores the locations and sizes of the R inter- mediate file regions produced by the map task

type MapTask struct {
	file              string
	status            taskState
	intermediateFiles []string
	startTime         int
}

type ReduceTask struct {
	intermediateFiles []string
	OutputFile        string
	status            taskState
	startTime         int
}

type Master struct {
	nReduce int

	// mu protects the fields below
	mu                      sync.Mutex
	isMapPhraseCompleted    bool
	mapTasks                []MapTask
	isReducePhraseCompleted bool
	reduceTasks             []ReduceTask
}

func (m *Master) checkTimeoutWorker() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			m.checkTaskTimeout()
		}
	}
}

func (m *Master) checkTaskTimeout() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().Second()

	// check map tasks
	for i, task := range m.mapTasks {
		if task.status == InProgress && now-task.startTime > 10 {
			m.mapTasks[i].status = Idle
		}
	}

	// check reduce tasks
	for i, task := range m.reduceTasks {
		if task.status == InProgress && now-task.startTime > 10 {
			m.reduceTasks[i].status = Idle
		}
	}
}

func (m *Master) FinishMapTask(args *FinishMapTaskArgs, reply *FinishMapTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mapTasks[args.TaskNumber].status = Completed
	m.mapTasks[args.TaskNumber].intermediateFiles = args.IntermediateFiles

	fmt.Println("finish map task number: ", args.TaskNumber, "intermediate files: ", args.IntermediateFiles)

	// check if all map tasks are completed
	for _, task := range m.mapTasks {
		if task.status != Completed {
			return nil
		}
	}

	fmt.Println("map phrase completed... Distribute map intermediate files to reduce tasks")

	for i, _ := range m.reduceTasks {
		for _, mapTask := range m.mapTasks {
			m.reduceTasks[i].intermediateFiles = append(m.reduceTasks[i].intermediateFiles, mapTask.intermediateFiles[i])
		}
		fmt.Println("reduce task ", i, " intermediate files: ", m.reduceTasks[i].intermediateFiles)
	}

	m.isMapPhraseCompleted = true
	return nil
}

func (m *Master) FinishReduceTask(args *FinishReduceTaskArgs, reply *FinishReduceTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.reduceTasks[args.TaskNumber].status = Completed
	m.reduceTasks[args.TaskNumber].OutputFile = args.OutputFile

	fmt.Println("finish reduce task number: ", args.TaskNumber)

	// check if all reduce tasks are completed
	for _, task := range m.reduceTasks {
		if task.status != Completed {
			return nil
		}
	}

	fmt.Println("reduce phrase completed....")

	m.isReducePhraseCompleted = true

	return nil
}

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isMapPhraseCompleted {
		// find an idle map task
		for i, task := range m.mapTasks {
			if task.status == Idle {
				fmt.Println("find an idle map task")
				m.mapTasks[i].status = InProgress

				reply.TaskType = MapTaskType

				// construct MapTaskBody reply
				reply.MapTaskBody = MapTaskBody{
					TaskNumber: i,
					SplitFile:  task.file,
					NReduce:    m.nReduce,
				}
				return nil
			}
		}

		fmt.Println("no map task available")
		return nil
	}

	if !m.isReducePhraseCompleted {
		// find an idle reduce task
		for i, task := range m.reduceTasks {
			if task.status == Idle {
				fmt.Println("find an idle reduce task")
				m.reduceTasks[i].status = InProgress

				reply.TaskType = ReduceTaskType

				// construct ReduceTaskBody reply
				reply.ReduceTaskBody = ReduceTaskBody{
					TaskNumber:        i,
					IntermediateFiles: task.intermediateFiles,
				}
				fmt.Println("construct ReduceTaskBody reply: ", i, task.intermediateFiles)
				return nil
			}
		}
		fmt.Println("no reduce task available")
	}

	fmt.Println("no task available. completed...")
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	return m.isReducePhraseCompleted
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{nReduce: nReduce}

	for _, file := range files {
		m.mapTasks = append(m.mapTasks, MapTask{file: file, status: Idle})
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTasks = append(m.reduceTasks, ReduceTask{status: Idle})
	}

	go m.checkTimeoutWorker()

	m.server()
	return &m
}
