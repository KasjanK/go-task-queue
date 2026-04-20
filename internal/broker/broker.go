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

type Queue struct {
	mu 	 sync.Mutex
	ID   string     `json:"id"`
	Jobs []*Job     `json:"jobs"`
}

type Broker struct {
	mu 	          sync.Mutex
	Queues        map[string]*Queue `json:"queues"`
	Dlq           []*Job 			`json:"dlq"`
	CompletedJobs []*Job			`json:"completed_jobs"`
}

func NewBroker() *Broker {
	return &Broker{
		Queues: make(map[string]*Queue),
	}
}

func (b *Broker) GetAllQueues() map[string]*Queue {
	b.mu.Lock()
    defer b.mu.Unlock()

    return b.Queues
}

func (b *Broker) GetJob(id string) (*Job, error) {
    b.mu.Lock()
    defer b.mu.Unlock()

    for _, q := range b.Queues {
        for _, job := range q.Jobs {
            if job.ID == id {
                return job, nil
            }
        }
    }
    
    return nil, fmt.Errorf("job %s not found", id)
}

func (b *Broker) Enqueue(job Job) *Job {
	b.mu.Lock()
	defer b.mu.Unlock()
	
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

	fmt.Println(b.Queues["email"])
	
	return newJob
}

func (b *Broker) Dequeue(queueName string) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	queue, ok := b.Queues[queueName]
	if !ok {
		return nil, fmt.Errorf("Queue not found")
	}

	for i := range queue.Jobs {
		if queue.Jobs[i].Status == "pending" {
			queue.Jobs[i].Status = "in-progress"
			queue.Jobs[i].StartedAt = time.Now()
			return queue.Jobs[i], nil
		}
	}

	return nil, errors.New("no Jobs pending")
}

func (b *Broker) CompleteJob(id, queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	queue, ok := b.Queues[queueName]
	if !ok {
		return fmt.Errorf("Queue not found")
	}

	for i := range queue.Jobs {
		if queue.Jobs[i].ID == id {
            queue.Jobs[i].Status = "completed"
			queue.Jobs[i].FinishedAt = time.Now()
			queue.Jobs[i].Duration = queue.Jobs[i].FinishedAt.Sub(queue.Jobs[i].StartedAt).Seconds()
			b.CompletedJobs = append(b.CompletedJobs, queue.Jobs[i])
			queue.Jobs[i] = queue.Jobs[len(queue.Jobs) - 1] 
			queue.Jobs[len(queue.Jobs)-1] = nil
			queue.Jobs = queue.Jobs[:len(queue.Jobs) - 1]
			return nil
		}
	}

	return fmt.Errorf("job not found")
}

func (b *Broker) FailJob(id, queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	queue := b.Queues[queueName]

	for i := range queue.Jobs {
		if queue.Jobs[i].ID == id {
			queue.Jobs[i].RetryCount++

			if queue.Jobs[i].RetryCount < queue.Jobs[i].MaxRetries {
				queue.Jobs[i].Status = "pending"
			} else {
				queue.Jobs[i].Status = "failed"
				b.Dlq = append(b.Dlq, queue.Jobs[i])
				queue.Jobs[i] = queue.Jobs[len(queue.Jobs) - 1] 
				queue.Jobs[len(queue.Jobs)-1] = nil
				queue.Jobs = queue.Jobs[:len(queue.Jobs) - 1]
			}

			return nil
		}
	}

	return fmt.Errorf("job not found")
}

func (b *Broker) GetDLQ() []*Job {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.Dlq
}

func (b *Broker) GetCompletedJobs() []*Job {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.CompletedJobs
}

func (b *Broker) GetQueueLength(queueName string) int {
    b.mu.Lock()
    defer b.mu.Unlock()

    queue, exists := b.Queues[queueName]
    if !exists {
        return 0
    }
    return len(queue.Jobs)
}

func (b *Broker) DeleteQueue(queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

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
