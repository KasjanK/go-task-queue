package broker

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Job struct {
    ID         string  		  `json:"id"`
    Type       string  		  `json:"type"`
    Payload    map[string]any `json:"payload"`
	Status     string 		  `json:"status"`
	MaxRetries int			  `json:"max_retries"`
	RetryCount int			  `json:"retry_count"`
	EnqueuedAt time.Time 	  `json:"enqueued_at"`
	StartedAt  time.Time	  `json:"started_at"`
	FinishedAt time.Time      `json:"finished_at"`
	Duration   float64        `json:"duration"`
}

type Queue struct {
	ID   string     `json:"id"`
	Jobs []*Job     `json:"jobs"`
}

type Broker struct {
	Mu 	            sync.RWMutex
	Queues          map[string]*Queue `json:"queues"`
	Dlq             []*Job 			`json:"dlq"`
	CompletedJobs   []*Job			`json:"completed_jobs"`
	jobCh 			chan *Job
	PendingCh       chan *Job
	TotalEnqueued   int
	TotalPending    int
	TasksInProgress int
	TasksSucceeded  int
	TotalFailed     int
	JobsByType      map[string]int `json:"jobs_by_type"`
	TotalRetries    int
}

func NewBroker(buffer int) *Broker {
	return &Broker{
		Queues: make(map[string]*Queue),
		JobsByType: make(map[string]int),
		jobCh: make(chan *Job, buffer),
		PendingCh: make(chan *Job, buffer),
	}
}

func (b *Broker) GetAllJobs() map[string]*Queue {
	b.Mu.RLock()
    defer b.Mu.RUnlock()

    return b.Queues
}

func (b *Broker) Jobs() <-chan *Job {
    return b.jobCh
}

func (b *Broker) GetJob(id string) (*Job, error) {
    b.Mu.RLock()
    defer b.Mu.RUnlock()

    for _, q := range b.Queues {
        for _, job := range q.Jobs {
            if job.ID == id {
                return job, nil
            }
        }
    }

	for _, job := range b.CompletedJobs {
		if job.ID == id {
			return job, nil
		}
	}

	for _, job := range b.Dlq {
		if job.ID == id {
			return job, nil
		}
	}
    
    return nil, fmt.Errorf("job %s not found", id)
}

func (b *Broker) Enqueue(job Job) *Job {
	b.Mu.Lock()
	
	if b.Queues[job.Type] == nil {
		b.Queues[job.Type] = &Queue{
			ID: job.Type, 
			Jobs: make([]*Job, 0),
		}
	}
	
	newJob := &Job{
		ID:         uuid.New().String(),
        Type:       job.Type,
        Payload:    job.Payload,
        Status:     "pending",
        MaxRetries: 3,
        EnqueuedAt: time.Now(),
	}

	queue := b.Queues[job.Type]
	queue.Jobs = append(queue.Jobs, newJob)
	
	b.TotalEnqueued++
	b.TotalPending++
	b.JobsByType[job.Type]++

	b.Mu.Unlock()

	b.PendingCh <- newJob

	return newJob
}

func (b *Broker) CompleteJob(job *Job) {
	b.Mu.Lock()

	job.Status = "completed"
	job.FinishedAt = time.Now()
	job.Duration = job.FinishedAt.Sub(job.StartedAt).Seconds()

	b.removeJobFromQueue(job)

	b.CompletedJobs = append(b.CompletedJobs, job)
	b.TasksSucceeded++
	b.TasksInProgress--

	b.Mu.Unlock()
}

func (b *Broker) FailJob(job *Job) {
	b.Mu.Lock()

	job.RetryCount++
	job.FinishedAt = time.Now()

	if job.RetryCount >= job.MaxRetries {
		job.Status = "failed"
		b.TotalFailed++
		b.TasksInProgress--
		
		b.removeJobFromQueue(job)

		b.Dlq = append(b.Dlq, job)
		b.Mu.Unlock()
		return
	}

	job.Status = "retrying"
	b.TotalRetries++
	b.TasksInProgress--
	b.Mu.Unlock()
	
	go b.retryLater(job)
}

func (b *Broker) retryLater(job *Job) {
	delay := backoff(job.RetryCount)

	time.Sleep(delay)

	b.Mu.Lock()

	job.Status = "pending"
	job.EnqueuedAt = time.Now()

	b.TotalPending++

	b.Mu.Unlock()

	b.PendingCh <- job
}

func backoff(retry int) time.Duration {
	base := 100 * time.Millisecond
	jitter := time.Duration(rand.Intn(100)) * time.Millisecond
	fmt.Println("RETRYING IN ", time.Duration(1<<retry) * base)
	return time.Duration(1<<retry) * base + jitter
}

func (b *Broker) GetDLQ() []*Job {
	b.Mu.RLock()
	defer b.Mu.RUnlock()

	return b.Dlq
}

func (b *Broker) StartDispatcher(ctx context.Context, rate int) {
	ticker := time.NewTicker(time.Second / time.Duration(rate))

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case job := <-b.PendingCh:
					b.jobCh <- job
				default:
					// nothing to dispatch
				}
			}
		}
	}()
}

func (b *Broker) removeJobFromQueue(job *Job) {
	queue, ok := b.Queues[job.Type]
	if !ok {
		return
	}

	for i := range queue.Jobs{
		if queue.Jobs[i] == job {
			queue.Jobs[i] = queue.Jobs[len(queue.Jobs)-1]
			queue.Jobs[len(queue.Jobs)-1] = nil
			queue.Jobs = queue.Jobs[:len(queue.Jobs)-1]
			return	
		}
	}
}

func (b *Broker) GetCompletedJobs() []*Job {
	b.Mu.RLock()
	defer b.Mu.RUnlock()

	return b.CompletedJobs
}

func (b *Broker) GetQueueLength(queueName string) int {
    b.Mu.RLock()
    defer b.Mu.RUnlock()

    queue, exists := b.Queues[queueName]
    if !exists {
        return 0
    }
    return len(queue.Jobs)
}

func (b *Broker) DeleteQueue(queueName string) error {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	_, ok := b.Queues[queueName] 
	if !ok {
		return fmt.Errorf("Queue not found")
	}

	if len(b.Queues[queueName].Jobs) > 0 {
		return fmt.Errorf("Cannot delete queues while jobs are pending!")
	}

	delete(b.Queues, queueName)	
	return nil
}
