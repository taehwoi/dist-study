package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
		reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		task, err := requestTask()
		if err != nil {
			return
		}

		//do some job
		if task.Tasktype == "map" {
			mapTask(mapf, task)
		} else if task.Tasktype == "reduce" {
			reduceTask(reducef, task)
		}


		println(task.Tasktype, task.Filename, task.Number)
		reportDone(task)
	}
}

func mapTask(mapf func(string, string) []KeyValue, task *TaskRequest) {

	filename := task.Filename
	nReduce := task.NReduce
	n := task.Number
	
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kvs := mapf(filename, string(content))

	// group kvs by reduceTaskNumber
	groupedKv := make(map[int][]KeyValue)

	for _, kv := range kvs {
		reduceTaskNumber := ihash(kv.Key) % nReduce
		groupedKv[reduceTaskNumber] = append(groupedKv[reduceTaskNumber], kv)
	}

	//create nReduce tempfiles to write
	tempFiles := []*os.File{}
	for i := 0; i < nReduce; i++ {
		tempFile, err := ioutil.TempFile("./mr-tmp", "")
		if err != nil {
			log.Fatalf("cannot create file %v", tempFile)
		}
		tempFiles = append(tempFiles, tempFile)
		defer tempFile.Close()
	}
		

	for rTaskNumber, kvs := range groupedKv {
		enc := json.NewEncoder(tempFiles[rTaskNumber])
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to file")
			}
		}
		resultName := fmt.Sprintf("mr-%d-%d", n, rTaskNumber)
		//TODO: partially report done
		os.Rename(tempFiles[rTaskNumber].Name(), resultName)
	}

}

func reduceTask(reducef func(string, []string) string, task *TaskRequest) {
	println("got reduce task")
	println(task.Filename)
	println(task.Number)

	files, err := filepath.Glob(task.Filename)

	kvs := make([]KeyValue, 0)

	if err != nil {
		return
	}
	for _, filename := range files {

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file")
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}

	sort.Sort(ByKey(kvs))
	oname := fmt.Sprintf("mr-out-%d", task.Number)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func requestTask() (*TaskRequest, error) {

	// declare an argument structure.
	request := struct{}{}

	// declare a reply structure.
	task := TaskRequest{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &request, &task)
	if ok {
		// reply.Y should be 100.
		return &task, nil
	} else {
		return nil, errors.New("failed to get task")
	}
}

func reportDone(task *TaskRequest) (error) {

	// declare a empty reply structure.
	reply := struct{}{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.TaskDone", &task, &reply)
	if ok {
		return nil
	} else {
		return errors.New("failed to report task")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
