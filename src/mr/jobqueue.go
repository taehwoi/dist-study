package mr

import (
	"sync"
	"time"
)

type JobQueue struct {
	// Your definitions here.
	jobCh       chan *Job
	runningJobs sync.Map // map[job][chan struct{}]
	timeout     time.Duration
}

type Job struct {
	Jobtype  string
	Filename string
	Number   int
}

func NewJobQueue(size int, timeout time.Duration) *JobQueue {
	jobCh := make(chan *Job, size)

	return &JobQueue{
		jobCh:       jobCh,
		runningJobs: sync.Map{},
		timeout:     timeout,
	}
}

// blocks if job queue is full
func (jq *JobQueue) SubmitJob(j *Job) {
	jq.jobCh <- j
	jq.runningJobs.Store(*j, make(chan struct{}))
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

	go jq.watchJob(j)

	return j
}

// blocks until job is finished via RemoveJob, if it is running
func (jq *JobQueue) WaitJob(j *Job) {
	val, ok := jq.runningJobs.Load(*j)
	if !ok {
		// job was not properly registered
		return
	}
	jobDoneCh := val.(chan struct{})
	<-jobDoneCh
}

// blocks until all jobs is finished via RemoveJob, if it is running
func (jq *JobQueue) WaitJobs(jobs []*Job) {
	wg := sync.WaitGroup{}

	for _, job := range jobs {
		if val, ok := jq.runningJobs.Load(*job); ok {
			jobDoneCh := val.(chan struct{})

			wg.Add(1)
			go func() {
				defer wg.Done()
				<-jobDoneCh
			}()
		}
	}

	wg.Wait()
}

func (jq *JobQueue) RemoveJob(j *Job) error {

	// remove from running jobs
	val, loaded := jq.runningJobs.LoadAndDelete(*j)

	if !loaded { // already finished, or was never in the queue
		return nil
	}

	jobDoneCh := val.(chan struct{})
	close(jobDoneCh)

	return nil
}

func (jq *JobQueue) watchJob(j *Job) {
	val, ok := jq.runningJobs.Load(*j)
	if !ok {
		// job was not properly registered
		return
	}
	jobDoneCh := val.(chan struct{})

	select {
	case <-time.After(jq.timeout):
		// resubmit a new job
		println("resubmitting due to timeout", j.Filename, j.Jobtype, j.Number)
		jq.jobCh <- j
		// shouldn't remove from running; processes waiting for the job to finish will be falsely notified
	case <-jobDoneCh:
		jq.runningJobs.Delete(*j)
	}
}
