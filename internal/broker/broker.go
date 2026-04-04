package broker

import (
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
