package mr

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

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

func generateWorkerID() string {
	n := 5
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	s := fmt.Sprintf("%X", b)
	return s
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := generateWorkerID()
	log.Printf("Worker %s started", workerID)

	var reducePhase bool = false
	for {
		if !reducePhase {
			log.Println("Try fetching map task from master")
			err, resp := fetchMapTask(workerID)
			if err != nil {
				log.Printf("Fail to communicate with master: %s, it should have exited. Exit the worker %s now.", err.Error(), workerID)
				break
			}
			log.Printf("Response: %+v", resp)
			if resp.Available {
				// map task assigned to this worker
				executeMap(mapf, workerID, resp.Filename, resp.ID, resp.NReduce)
			}
		}

		// currently no available map tasks
		// reduce phase not necessary started as could be due to
		// map tasks are all executing by other busy workers
		// try fetching reduce task
		log.Println("Try fetching reduce task from master")
		err, resp := fetchReduceTask(workerID)
		log.Printf("Response: %+v", resp)
		if err != nil {
			log.Printf("Fail to communicate with master: %s, it should have exited. Exit the worker %s now.", err.Error(), workerID)
			break
		}
		if resp.Available {
			// here we can be sure that reduce phase has started
			// and worker no longer need to query map tasks from now
			reducePhase = true
			executeReduce(reducef, workerID, resp.ID)
		}

		time.Sleep(3 * time.Second)
	}
}

func executeMap(mapf func(string, string) []KeyValue, workerID string, filename string, maptaskID int, nReduce int) {
	log.Printf("Start executing map task %d for file %s", maptaskID, filename)
	// execute map, write intermediate results to flies
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(content))

	// sort intermediate kvs by their partition group for reduce
	sort.SliceStable(intermediate, func(i, j int) bool {
		return ihash(intermediate[i].Key)%nReduce < ihash(intermediate[j].Key)%nReduce
	})

	// group kvs that destinated to same reduce partition together
	// in order to reduce the number of file operations
	i := 0
	for i < len(intermediate) {
		key := intermediate[i].Key
		j := i + 1
		for j < len(intermediate) && ihash(intermediate[i].Key)%nReduce == ihash(intermediate[j].Key)%nReduce {
			j++
		}
		kvs := []KeyValue{}
		for k := i; k < j; k++ {
			kvs = append(kvs, intermediate[k])
		}
		i = j

		intermediateTempfile, err := ioutil.TempFile("", fmt.Sprintf("mr-temp-%d-%d", maptaskID, ihash(key)%nReduce))
		if err != nil {
			log.Fatal("Unable to write to temp intermerdiate file")
		}
		enc := json.NewEncoder(intermediateTempfile)
		if err != nil {
			log.Fatal("Unable to encode json to intermerdiate temp file")
		}
		for _, kv := range kvs {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatal("Unable to write to intermerdiate temp file")
			}
		}
		// atomic rename
		intermediateFileName := fmt.Sprintf("mr-%d-%d", maptaskID, ihash(key)%nReduce)
		err = os.Rename(intermediateTempfile.Name(), intermediateFileName)
		if err != nil {
			log.Fatal("Unable to rename temp map result file")
		}
	}
	// Inform the master once completes

	args := TaskCompletedRequest{WorkerID: workerID, TaskID: maptaskID, Type: MapTaskType}
	resp := TaskCompletedResponse{}

	log.Printf("current map task completed. Notify master of completion.")
	err = call("Master.TaskCompleted", &args, &resp)
	if err != nil {
		log.Fatal("Unable to notify master for map task completion via rpc")
	}
	log.Printf("Resp: %v", resp)
}

func executeReduce(reducef func(string, []string) string, workerID string, reduceTaskID int) error {
	log.Printf("Start executing reduce task %d", reduceTaskID)
	// find all intermediate location files for the given reduce
	pattern := fmt.Sprintf("mr-*-%d", reduceTaskID)
	filenames, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal(err)
	}
	// aggregate all intermediate kvs from different files into memory
	var intermediate []KeyValue
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
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
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceTaskID)
	ofile, _ := os.Create(oname)

	// group all intermediate kvs by unique key and apply reduce func on each key
	// write final result to the result file for the current reduce task
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// inform the master of completion
	args := TaskCompletedRequest{WorkerID: workerID, TaskID: reduceTaskID, Type: ReduceTaskType}
	resp := TaskCompletedResponse{}

	log.Printf("current reduce task completed. Notify master of completion.")
	err = call("Master.TaskCompleted", &args, &resp)
	if err != nil {
		log.Fatal("Unable to notify master for reduce task completion via rpc")
	}
	log.Printf("Resp: %v", resp)
	return nil
}

func fetchMapTask(workerID string) (error, FetchMapTaskResponse) {

	// declare an argument structure.
	args := FetchMapTaskRequest{}
	args.WorkerID = workerID

	resp := FetchMapTaskResponse{}

	// send the RPC request, wait for the reply.
	err := call("Master.FetchMapTask", &args, &resp)
	if err != nil {
		return err, FetchMapTaskResponse{}
	}
	return nil, resp
}

func fetchReduceTask(workerID string) (error, FetchReduceTaskResponse) {

	args := FetchReduceTaskRequest{}
	args.WorkerID = workerID

	resp := FetchReduceTaskResponse{}

	err := call("Master.FetchReduceTask", &args, &resp)
	if err != nil {
		return err, FetchReduceTaskResponse{}
	}
	return nil, resp
}

func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}

	return nil
}
