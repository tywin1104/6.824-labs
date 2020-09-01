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

type mapTask struct {
	id               int
	filename         string
	completed        bool
	assignedWorkerID string
}

type reduceTask struct {
	id               int
	completed        bool
	assignedWorkerID string
}

type Master struct {
	nReduce     int
	mapTasks    []mapTask
	reduceTasks []reduceTask
	mux         sync.Mutex
}

func (m *Master) FetchMapTask(req *FetchMapTaskRequest, reply *FetchMapTaskResponse) error {
	log.Printf("Receive fetch map task request from worker: %v", req)
	workerID := req.WorkerID
	m.mux.Lock()
	defer m.mux.Unlock()
	index := m.fetchNextMapTask()

	if index != -1 {
		// assign the worker to the task and reply to worker
		m.mapTasks[index].assignedWorkerID = workerID
		task := m.mapTasks[index]
		*reply = FetchMapTaskResponse{Filename: task.filename, ID: task.id, Available: true, NReduce: m.nReduce}

		go func() {
			// wait up to 10 seconds for the assigned worker to finish task
			// otherwise consider it dead and reset the task
			timeout := time.After(10 * time.Second)
			tick := time.Tick(200 * time.Millisecond)
			stop := false
			for stop != true {
				select {
				case <-timeout:
					fmt.Println("Map Task time out. Reset task status")
					m.mux.Lock()
					m.mapTasks[index].assignedWorkerID = ""
					m.mapTasks[index].completed = false
					m.mux.Unlock()
					stop = true
					break
				case <-tick:
					m.mux.Lock()
					fmt.Println("Tick: ", "map task ", index, "== ", m.mapTasks[index].completed)
					if m.mapTasks[index].completed == true {
						stop = true
						m.mux.Unlock()
						break
					}
					m.mux.Unlock()
				}
			}
		}()
	} else {
		*reply = FetchMapTaskResponse{Available: false}
	}
	return nil
}

func (m *Master) TaskCompleted(req *TaskCompletedRequest, reply *TaskCompletedResponse) error {
	log.Printf("Receive task completion from worker: %v", req)
	taskType := req.Type
	workerID := req.WorkerID
	taskID := req.TaskID
	m.mux.Lock()
	defer m.mux.Unlock()

	if taskType == MapTaskType {
		for i, task := range m.mapTasks {
			// if a previously time-out worker completed its old mapTask
			// since the original mapTask timed out and previous association should been cleared
			// this condition should no longer be satisfied
			if task.id == taskID && task.assignedWorkerID == workerID {
				m.mapTasks[i].completed = true
				*reply = TaskCompletedResponse{Acknowledged: true}
			}
		}
	} else if taskType == ReduceTaskType {
		for i, task := range m.reduceTasks {
			if task.id == taskID && task.assignedWorkerID == workerID {
				m.reduceTasks[i].completed = true
				*reply = TaskCompletedResponse{Acknowledged: true}
			}
		}
	} else {
		*reply = TaskCompletedResponse{Acknowledged: false}
	}
	return nil
}

func (m *Master) FetchReduceTask(req *FetchReduceTaskRequest, reply *FetchReduceTaskResponse) error {
	log.Printf("Receive fetch reduce task request from worker: %v", req)
	workerID := req.WorkerID
	m.mux.Lock()
	defer m.mux.Unlock()
	if !m.mapPhaseDone() {
		*reply = FetchReduceTaskResponse{Available: false}
		return nil
	}

	index := m.fetchNextReduceTask()

	if index != -1 {
		// assign the worker to the task and reply to worker
		m.reduceTasks[index].assignedWorkerID = workerID
		task := m.reduceTasks[index]
		*reply = FetchReduceTaskResponse{Available: true, ID: task.id}
		stop := false
		go func() {
			// similiar to maptasks
			timeout := time.After(10 * time.Second)
			tick := time.Tick(200 * time.Millisecond)

			for stop != true {
				select {
				case <-timeout:
					fmt.Println("Reduce Task time out. Reset task status")
					m.mux.Lock()
					m.reduceTasks[index].assignedWorkerID = ""
					m.reduceTasks[index].completed = false
					m.mux.Unlock()
					stop = true
					break
				case <-tick:
					m.mux.Lock()
					fmt.Println("Tick: ", "reduce task ", index, "== ", m.reduceTasks[index].completed)
					if m.reduceTasks[index].completed == true {
						stop = true
						m.mux.Unlock()
						break
					}
					m.mux.Unlock()
				}
			}
		}()
	} else {
		*reply = FetchReduceTaskResponse{Available: false}
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

func (m *Master) fetchNextMapTask() int {
	for i, task := range m.mapTasks {
		if task.completed == false && task.assignedWorkerID == "" {
			return i
		}
	}
	return -1
}

func (m *Master) fetchNextReduceTask() int {
	for i, task := range m.reduceTasks {
		if !task.completed && task.assignedWorkerID == "" {
			return i
		}
	}
	return -1
}

func (m *Master) mapPhaseDone() bool {
	for _, task := range m.mapTasks {
		if !task.completed {
			return false
		}
	}
	return true
}

func (m *Master) reducePhaseDone() bool {
	for _, task := range m.reduceTasks {
		if !task.completed {
			return false
		}
	}
	return true
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mux.Lock()
	defer m.mux.Unlock()
	ret := m.mapPhaseDone() && m.reducePhaseDone()
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	mapTasks := make([]mapTask, len(files))
	for i, filename := range files {
		mapTasks[i] = mapTask{filename: filename, completed: false, id: i + 1}
	}
	reduceTasks := make([]reduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = reduceTask{id: i}
	}

	m := Master{
		nReduce:     nReduce,
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
	}

	m.server()
	return &m
}
