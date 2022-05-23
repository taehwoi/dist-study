package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	doneCh   chan struct{}
	jobQueue *JobQueue
	nReduce  int
	nMap     int

	files []string
}

// Your code here -- RPC handlers for the worker to call.

//
// RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTask(request *struct{}, task *TaskRequest) error {
	j := c.jobQueue.GetJob()

	task.Filename = j.Filename
	task.Tasktype = j.Jobtype
	task.Number = j.Number
	task.NReduce = c.nReduce
	task.NMap = c.nMap
	return nil
}

//
// RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) TaskDone(task *TaskRequest, empty *struct{}) error {
	go c.onTaskFinish(task)

	return nil
}

func (c *Coordinator) initialize() {
	c.jobQueue.SubmitJobs(c.allMapTasks())
	go c.submitReduce()
}

func (c *Coordinator) allMapTasks() []*Job {
	res := make([]*Job, 0, len(c.files))

	for idx, name := range c.files {

		job := Job{
			Jobtype:  "map",
			Filename: name,
			Number:   idx,
		}
		res = append(res, &job)
	}

	return res
}

func (c *Coordinator) allReduceTasks() []*Job {
	res := make([]*Job, 0, c.nReduce)

	for i := 0; i < c.nReduce; i++ {
		job := Job{
			Jobtype: "reduce",
			//TODO: come up with a better glob
			//don't use a star: it clashes with mr-worker... in job test
			Filename: fmt.Sprintf("mr-[0-9]-%d", i),
			Number:   i,
		}
		res = append(res, &job)
	}

	return res
}

func (c *Coordinator) submitReduce() {

	c.jobQueue.WaitJobs(c.allMapTasks())
	c.jobQueue.SubmitJobs(c.allReduceTasks())

	go c.waitForDone()
}

//
// RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) ReportMapFailure(report *FailureReport, empty *struct{}) error {
	// insert the map task in to the queue again
	// reduce tasks that reported fail will not proceed; and will be automatically retried
	failingMapJobs := report.FailingMapNumbers

	jobs := make([]*Job, 0, len(failingMapJobs))
	for _, v := range failingMapJobs {
		job := Job{
			Jobtype:  "map",
			Filename: c.files[v],
			Number:   v,
		}
		jobs = append(jobs, &job)
	}
	c.jobQueue.SubmitJobs(jobs)
	c.jobQueue.WaitJobs(jobs)

	failingReducerJob := report.FailingReduceNumber

	reduceJob := Job{
		Jobtype:  "reduce",
		Filename: fmt.Sprintf("mr-[0-9]-%d", failingReducerJob),
		Number:   failingReducerJob,
	}
	c.jobQueue.RemoveJob(&reduceJob)
	c.jobQueue.SubmitJob(&reduceJob)

	return nil
}

func (c *Coordinator) waitForDone() {
	c.jobQueue.WaitJobs(c.allReduceTasks())

	close(c.doneCh)
}

func (c *Coordinator) onTaskFinish(task *TaskRequest) error {

	job := &Job{
		Jobtype:  task.Tasktype,
		Filename: task.Filename,
		Number:   task.Number,
	}
	// remove from running jobs
	c.jobQueue.RemoveJob(job)

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
// main/mrcoordinator.go calls Done() once
//
func (c *Coordinator) Done() <-chan struct{} {
	return c.doneCh
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// initialize map tasks

	// correct way to determine the size of jobQueue
	jobQueue := NewJobQueue(1000, 10*time.Second)

	c := Coordinator{
		doneCh:   make(chan struct{}),
		jobQueue: jobQueue,
		nReduce:  nReduce,
		nMap:     len(files),
		files:    files,
	}

	c.initialize()

	// Your code here.

	c.server()
	return &c
}
