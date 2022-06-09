package mr

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	doneCh   chan struct{}
	jobQueue *JobQueue
	nReduce  int

	files []string
}

// Your code here -- RPC handlers for the worker to call.

//
// RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTask(request *struct{}, task *TaskRequest) error {
	var j *Job

	for {
		j = c.jobQueue.GetJob()

		// don't send psuedo tasks to workers
		switch j.Jobtype {
		case "pseudo":
			continue
		case "done":
			close(c.doneCh)
		default:
		}
		break
	}

	go c.watchJob(context.TODO(), j)

	if j.Jobtype == "reduce" {
		for _, depJob := range j.Deps {
			task.Filenames = append(task.Filenames, depJob.Output)
		}
	} else if j.Jobtype == "map" {
		idx, _ := strconv.Atoi(j.Key)
		task.Filenames = []string{c.files[idx]}
	}

	task.Tasktype = j.Jobtype
	task.Key = j.Key
	task.NReduce = c.nReduce
	task.NMap = len(c.files)
	return nil
}

func (c *Coordinator) watchJob(ctx context.Context, j *Job) {

	select {
	case <-time.After(10 * time.Second):
		println("retry due to timeout", j.Jobtype, j.Key)
		j.Retry()
	case <-j.Done:
	}

}

//
// RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) TaskDone(report *SuccessReport, empty *struct{}) error {
	go c.onTaskFinish(report)

	return nil
}

func (c *Coordinator) initialize() {

	mapJobs := make([]*Job, 0, len(c.files))

	for idx := range c.files {

		job := NewJob("map", strconv.Itoa(idx), []*Job{})
		mapJobs = append(mapJobs, job)
	}

	c.jobQueue.SubmitJobs(mapJobs)

	reduceJobs := make([]*Job, 0, c.nReduce)

	for i := 0; i < c.nReduce; i++ {
		deps := c.intermediateTasks(i)
		c.jobQueue.SubmitJobs(deps)

		reduceJob := NewJob("reduce", strconv.Itoa(i), deps)

		reduceJobs = append(reduceJobs, reduceJob)
	}
	c.jobQueue.SubmitJobs(reduceJobs)

	doneTask := Job{
		Jobtype: "done",
		Key:     "done",
		Deps:    reduceJobs,
	}
	c.jobQueue.SubmitJob(&doneTask)
}

// returns all the intermediate tasks reducer depends on
func (c *Coordinator) intermediateTasks(reducerId int) []*Job {
	res := make([]*Job, 0, len(c.files))

	for idx := range c.files {

		job := NewJob("pseudo", fmt.Sprintf("%d%d", idx, reducerId), []*Job{})
		res = append(res, job)
	}

	return res
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
			Jobtype: "map",
			Key:     strconv.Itoa(v),
		}
		jobs = append(jobs, &job)
	}
	c.jobQueue.SubmitJobs(jobs)
	// c.jobQueue.WaitJobs(jobs)

	failingReducerJob := report.FailingReduceNumber

	reduceJob := Job{
		Jobtype: "reduce",
		Key:     strconv.Itoa(failingReducerJob),
	}
	// c.jobQueue.RemoveJob(&reduceJob)
	c.jobQueue.SubmitJob(&reduceJob)

	return nil
}

func (c *Coordinator) onTaskFinish(report *SuccessReport) error {

	job := &Job{
		Jobtype: report.Tasktype,
		Key:     report.Key,
		Output:  report.OutputFilename,
	}
	// remove from running jobs
	c.jobQueue.Finish(job)

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
	jobQueue := NewJobQueue(1000)

	c := Coordinator{
		doneCh:   make(chan struct{}),
		jobQueue: jobQueue,
		nReduce:  nReduce,
		files:    files,
	}

	c.initialize()

	// Your code here.

	c.server()
	return &c
}
