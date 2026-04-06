package broker

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type Job struct {
    ID      string  		`json:"id"`
    Type    string  		`json:"type"`
    Payload map[string]any  `json:"payload"`
	Status  string 			`json:"status"`
}

type Broker struct {
	mu 	sync.Mutex
	jobs []Job
}

func NewBroker() *Broker {
	return &Broker{
		jobs: []Job{
			{
				ID:   "seed-1",
				Type: "email",
				Payload: map[string]any{
					"to": "test@example.com",
				},
				Status: "pending",
			},
		},
	}
}

func (b *Broker) GetAllJobs() []Job {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.jobs
}

func (b *Broker) GetJob(id string) (Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, j := range b.jobs {
		if id == j.ID {
			return j, nil
		}
	}
	return Job{}, fmt.Errorf("Job not found")
}

func (b *Broker) Enqueue(job Job) Job {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	job.ID = uuid.New().String()
	job.Status = "pending"
	b.jobs = append(b.jobs, job)
	
	return job
}

func (b *Broker) Dequeue() (Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range b.jobs {
		if b.jobs[i].Status == "pending" {
			b.jobs[i].Status = "in-progress"
			return b.jobs[i], nil
		}
	}

	return Job{}, errors.New("no jobs pending")
}

func (b *Broker) CompleteJob(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range b.jobs {
        if b.jobs[i].ID == id {
            b.jobs[i].Status = "completed"
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
			b.jobs[i].Status = "failed"
			return nil
		}
	}

	return fmt.Errorf("job not found")
}
