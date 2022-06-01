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
	"sort"
	"strconv"
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

	for {
		task, err := requestTask()
		if err != nil {
			// fmt.Println(err.Error())
			return
		}
		//do some job
		if task.Tasktype == "map" {
			mapTask(mapf, task)
		} else if task.Tasktype == "reduce" {
			reduceTask(reducef, task)
		}
	}
}

func mapTask(mapf func(string, string) []KeyValue, task *TaskRequest) {

	filename := task.Filenames[0]
	nReduce := task.NReduce
	n := task.Key

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

	tempFiles := make(map[int]*os.File, nReduce)
	// create nReduce temp files
	for i := 0; i < nReduce; i++ {
		tempFile, _ := ioutil.TempFile(".", "")
		tempFiles[i] = tempFile
	}

	groupedKv := make(map[int][]KeyValue, nReduce)
	for _, kv := range kvs {
		reduceTaskNumber := ihash(kv.Key) % nReduce
		groupedKv[reduceTaskNumber] = append(groupedKv[reduceTaskNumber], kv)

	}

	for i := 0; i < nReduce; i++ {
		tempFile := tempFiles[i]

		enc := json.NewEncoder(tempFile)
		kvs, ok := groupedKv[i]
		if !ok {
			// report there is an empty reducer
			report := &SuccessReport{
				OutputFilename: "",
				Tasktype:       "pseudo",
				Key:            fmt.Sprintf("%s%d", n, i),
			}
			reportDone(report)
			// continue
		}

		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to file")
			}
		}
		resultName := fmt.Sprintf("mr-%s-%d", n, i)
		os.Rename(tempFile.Name(), resultName)
		tempFile.Close()

		report := &SuccessReport{
			OutputFilename: resultName,
			Tasktype:       "pseudo",
			Key:            fmt.Sprintf("%s%d", n, i),
		}
		reportDone(report)

	}

	report := &SuccessReport{
		OutputFilename: "",
		Tasktype:       "map",
		Key:            n,
	}

	reportDone(report)
}

func reduceTask(reducef func(string, []string) string, task *TaskRequest) {
	filenames := task.Filenames

	files := make([]*os.File, 0)
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file")
		}
		files = append(files, file)
		defer file.Close()
	}

	currentFileNames := map[string]struct{}{}
	for _, file := range files {
		currentFileNames[file.Name()] = struct{}{}
	}

	if len(files) != task.NMap {
		//something is wrong with map, or it is still running
		//either way, we report it
		x, _ := strconv.Atoi(task.Key)
		report := FailureReport{
			FailingMapNumbers:   []int{},
			FailingReduceNumber: x,
		}

		// collect missing files' map task number
		for i := 0; i < task.NMap; i++ {
			filename := fmt.Sprintf("mr-%d-%s", i, task.Key)
			if _, ok := currentFileNames[filename]; !ok {
				report.FailingMapNumbers = append(report.FailingMapNumbers, i)
			}
		}
		reportMapFailure(&report)
		// just stop progressing; coordinator should handle reschedule
		return
	}

	// from here, since we mounted everything on memory, it's okay

	kvs := make([]KeyValue, 0)
	for _, file := range files {
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
	oname := fmt.Sprintf("mr-out-%s", task.Key)
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

	report := &SuccessReport{
		OutputFilename: "",
		Tasktype:       "reduce",
		Key:            task.Key,
	}
	reportDone(report)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func requestTask() (*TaskRequest, error) {

	request := struct{}{}

	task := TaskRequest{}

	ok := call("Coordinator.GetTask", &request, &task)
	if ok {
		return &task, nil
	} else {
		return nil, errors.New("failed to get task")
	}
}

func reportDone(report *SuccessReport) error {

	// declare a empty reply structure.
	reply := struct{}{}

	ok := call("Coordinator.TaskDone", &report, &reply)
	if ok {
		return nil
	} else {
		return errors.New("failed to report task")
	}
}

func reportMapFailure(report *FailureReport) error {

	// declare a empty reply structure.
	reply := struct{}{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.ReportMapFailure", &report, &reply)
	if ok {
		return nil
	} else {
		return errors.New("failed to report mapFailure")
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
	return err == nil
}
