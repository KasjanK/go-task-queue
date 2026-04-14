package broker

import (
	"errors"
	"fmt"
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
	MemoryUsageBytes uint64   `json:"memory_usage_bytes"`
}

type Broker struct {
	mu 	sync.Mutex
	jobs []*Job
	dlq  []*Job
	completedJobs []*Job
}

func NewBroker() *Broker {
	return &Broker{
		jobs: []*Job{
			{
				ID:   "seed-1",
				Type: "email",
				Payload: map[string]any{
				},
				Status: "pending",
				MaxRetries: 3,
				RetryCount: 0,
				EnqueuedAt: time.Now(),
			},
			{
				ID:   "seed-2",
				Type: "email",
				Payload: map[string]any{
				},
				Status: "pending",
				MaxRetries: 3,
				RetryCount: 0,
				EnqueuedAt: time.Now(),
			},
			{
				ID:   "seed-3",
				Type: "email",
				Payload: map[string]any{
				},
				Status: "pending",
				MaxRetries: 3,
				RetryCount: 0,
				EnqueuedAt: time.Now(),
			},
		},
	}
}

func (b *Broker) GetAllJobs() []*Job {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.jobs
}

func (b *Broker) GetJob(id string) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, job := range b.jobs {
		if id == job.ID {
			return job, nil
		}
	}
	return nil, fmt.Errorf("Job not found")
}

func (b *Broker) Enqueue(job Job) Job {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	job.ID = uuid.New().String()
	job.Status = "pending"
	job.MaxRetries = 3
	job.EnqueuedAt = time.Now()

	newJob := job
	b.jobs = append(b.jobs, &newJob)
	
	return job
}

func (b *Broker) Dequeue() (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range b.jobs {
		if b.jobs[i].Status == "pending" {
			b.jobs[i].Status = "in-progress"
			b.jobs[i].StartedAt = time.Now()
			return b.jobs[i], nil
		}
	}

	return nil, errors.New("no jobs pending")
}

func (b *Broker) CompleteJob(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range b.jobs {
        if b.jobs[i].ID == id {
            b.jobs[i].Status = "completed"
			b.jobs[i].FinishedAt = time.Now()
			b.jobs[i].Duration = b.jobs[i].FinishedAt.Sub(b.jobs[i].StartedAt).Seconds()
			b.completedJobs = append(b.completedJobs, b.jobs[i])
			b.jobs[i] = b.jobs[len(b.jobs) - 1] 
			b.jobs[len(b.jobs)-1] = nil
			b.jobs = b.jobs[:len(b.jobs) - 1]
            return nil
        }
    }

	return fmt.Errorf("job not found")
}

func (b *Broker) FailJob(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range b.jobs {
		if b.jobs[i].ID == id {
			b.jobs[i].RetryCount++

			if b.jobs[i].RetryCount < b.jobs[i].MaxRetries {
				b.jobs[i].Status = "pending"
			} else {
				b.jobs[i].Status = "failed"
				b.dlq = append(b.dlq, b.jobs[i])
				b.jobs[i] = b.jobs[len(b.jobs) - 1] 
				b.jobs[len(b.jobs)-1] = nil
				b.jobs = b.jobs[:len(b.jobs) - 1]
			}

			return nil
		}
	}

	return fmt.Errorf("job not found")
}

func (b *Broker) GetDLQ() []*Job {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.dlq
}

func (b *Broker) GetCompletedJobs() []*Job {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.completedJobs
}
