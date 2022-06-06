package mr

import (
	"sync"
)

type Job struct {
	Jobtype string
	Key     string
	Done    chan struct{} // channel for monitoring job status. closed when done
	Deps    []*Job        // dependent jobs; job will only be scheduled when deps are done

	Inputs []string

	Output string

	queue *JobQueue // the pointer to the queue this job is running on

}

func NewJob(jobType string, key string, deps []*Job) *Job {
	return &Job{
		Jobtype: jobType,
		Key:     key,
		Deps:    deps,
		Done:    make(chan struct{}),
	}

}

func (j *Job) id() string {

	return j.Jobtype + j.Key
}

// Retry the task, adding it to the back of the queue.
func (j *Job) Retry() {

	// never been submitted
	if j.queue == nil {
		return
	}

	//resubmit
	j.queue.SubmitJob(j)
}

type JobQueue struct {
	jobCh       chan *Job
	runningJobs sync.Map // map[job.id()][*job]
}

func NewJobQueue(size int) *JobQueue {
	jobCh := make(chan *Job, size)

	return &JobQueue{
		jobCh:       jobCh,
		runningJobs: sync.Map{},
	}
}

func (jq *JobQueue) SubmitJob(j *Job) {

	go jq.scheduleJob(j)
}

// blocks if job queue is full
func (jq *JobQueue) scheduleJob(job *Job) {

	// wait until deps have been finished before scheduling the job
	wg := sync.WaitGroup{}
	for _, depJob := range job.Deps {
		wg.Add(1)
		go func(job *Job) {
			defer wg.Done()
			<-job.Done
		}(depJob)
	}
	wg.Wait()

	job.queue = jq
	jq.jobCh <- job
	jq.runningJobs.Store(job.id(), job)
}

// blocks if job queue is full
func (jq *JobQueue) SubmitJobs(jobs []*Job) {
	for _, job := range jobs {
		jq.SubmitJob(job)
	}
}

// blocks if job queue is empty
func (jq *JobQueue) GetJob() *Job {
	j := <-jq.jobCh

	return j
}

func (jq *JobQueue) Finish(j *Job) error {

	// remove from running jobs
	val, loaded := jq.runningJobs.LoadAndDelete(j.id())

	if !loaded { // already finished, or was never in the queue
		return nil
	}

	job := val.(*Job)
	job.Output = j.Output
	// job.Done <- j.Output
	close(job.Done)

	return nil
}
