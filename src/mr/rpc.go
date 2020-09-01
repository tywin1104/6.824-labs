package mr

import (
	"os"
	"strconv"
)

type TaskType string

var MapTaskType = TaskType("map")
var ReduceTaskType = TaskType("reduce")

type FetchMapTaskRequest struct {
	WorkerID string
}

// empty filename indicates no available map task atm
type FetchMapTaskResponse struct {
	Available bool
	ID        int
	Filename  string
	NReduce   int
}

type TaskCompletedRequest struct {
	Type     TaskType
	WorkerID string
	TaskID   int
}

type TaskCompletedResponse struct {
	Acknowledged bool
}

type FetchReduceTaskRequest struct {
	WorkerID string
}

type FetchReduceTaskResponse struct {
	Available bool
	ID        int
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
