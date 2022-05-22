package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	done chan struct{}
	jobQueue chan *job
	//TODO
	// reduceJobQueue chan *job
	runningJobs sync.Map
	nReduce int
	nMap int

}

type job struct {
	Jobtype string
	Filename string
	Index int
}

// Your code here -- RPC handlers for the worker to call.


func (c *Coordinator) addRunningJob(j *job) {
	c.runningJobs.Store(*j, make(chan struct{}))

	// spawn a goroutine that monitors the running task
	go c.watchJob(j)
}

func (c *Coordinator) watchJob(j *job) {
	val, ok := c.runningJobs.Load(*j)
	jobDoneCh := val.(chan struct{})
	if !ok {
		// job was not properly registered
		return
	}

	select {
	case <-time.After(10*time.Second):
		// resubmit a new job
		println("resubmitting due to timeout")
		c.jobQueue <- j

		// no need to a running job as done
		// c.runningJobs.Delete(j)
	case <-jobDoneCh:
		c.runningJobs.Delete(j)
	}
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTask(request *struct{}, task *TaskRequest) error {
	// this operation should be function

	j := c.scheduleJob()

	task.Filename = j.Filename
	task.Tasktype = j.Jobtype
	task.Number = j.Index
	task.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) scheduleJob() *job {

	j := <- c.jobQueue

	c.addRunningJob(j)

	return j
}


//TODO
func (c *Coordinator) onTaskFinish(task *TaskRequest) error {

	if task.Tasktype == "map" {
		c.dispatchReduceTask()
	} else if task.Tasktype == "reduce" {
		c.notifyIfDone()
	}

	job := &job{task.Tasktype, task.Filename, task.Number}
	// remove from running jobs
	val, ok := c.runningJobs.LoadAndDelete(*job)

	if !ok { // already finished
		println("not existing in runningJobs")
		return nil
	}

	println("finished:", job.Jobtype, job.Filename)
	jobDoneCh := val.(chan struct{})
	close(jobDoneCh)

	return nil
}

func (c *Coordinator) dispatchReduceTask() {
	// hack: check if n finished files exist for r
	for i := 0; i < c.nReduce; i++ {
		files, err := filepath.Glob("mr-*-" + strconv.Itoa(i))
		if err != nil {
			return
		}

		if (len(files) == c.nMap) {
			// can dispatch
			j := &job{"reduce", "mr-*-" + strconv.Itoa(i), i}
			c.jobQueue <- j
		}
	}
}

func (c * Coordinator) notifyIfDone() {

	files, _ := filepath.Glob("mr-out-*")

	if (len(files) == c.nReduce) {
		close(c.done)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) TaskDone(task *TaskRequest, empty *struct{}) error {
	// if the finished task was a map type, submit a reduce task to the queue
	go c.onTaskFinish(task)

	// go c.notifyIfDone()

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() <-chan struct{} {
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		make(chan struct{}),
		// correct way to determine the size buffered channel?
		make(chan *job, 1000),
		sync.Map{},
		nReduce,
		len(files)}

	for idx, name := range files {

		newJob := job{"map", name, idx}
		c.jobQueue <- &newJob
	}

	// Your code here.

	c.server()
	return &c
}
