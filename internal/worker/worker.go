package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/KasjanK/go-task-queue/internal/broker"
	"github.com/google/uuid"
)

type Worker struct {
	ID 		  string
	Broker    *broker.Broker
	StartedAt time.Time
	IsRunning bool
	QueueName string
}

type JobStats struct {
	CPUStartTime int64
	MemAllocated uint64 
}

func NewWorker(b *broker.Broker, queueName string) *Worker {
    return &Worker{
        ID:        uuid.New().String(),
        Broker:    b,
        StartedAt: time.Now(),
        IsRunning: true,
		QueueName: queueName,
    }
}

func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Worker %s shutting down...\n", w.ID)
			return
		default:
			stats := JobStats{}
			job, err := w.Broker.Dequeue(w.QueueName)
			if err != nil {
				continue
			}

			switch job.Type {
			case "email":
				var m1, m2 runtime.MemStats
				runtime.ReadMemStats(&m1)
				err := w.SendEmail(job.Payload)
				runtime.ReadMemStats(&m2)
				stats.MemAllocated = m2.TotalAlloc - m1.TotalAlloc
				job.MemoryUsageBytes = stats.MemAllocated
				if err != nil {
					w.Broker.FailJob(job.ID, w.QueueName) 	 // nack
				} else {
					w.Broker.CompleteJob(job.ID, w.QueueName) // ack
				}
			case "resizer":
				var m1, m2 runtime.MemStats
				runtime.ReadMemStats(&m1)
				err := w.ResizeImage(job.Payload)
				runtime.ReadMemStats(&m2)
				stats.MemAllocated = m2.TotalAlloc - m1.TotalAlloc
				job.MemoryUsageBytes = stats.MemAllocated
				if err != nil {
					w.Broker.FailJob(job.ID, w.QueueName) 	 // nack
				} else {
					w.Broker.CompleteJob(job.ID, w.QueueName) // ack
				}
			case "fail":
				var m1, m2 runtime.MemStats
				runtime.ReadMemStats(&m1)
				err := w.Fail(job.Payload)
				runtime.ReadMemStats(&m2)
				stats.MemAllocated = m2.TotalAlloc - m1.TotalAlloc
				job.MemoryUsageBytes = stats.MemAllocated
				if err != nil {
					w.Broker.FailJob(job.ID, w.QueueName) 	 // nack
				} else {
					w.Broker.CompleteJob(job.ID, w.QueueName) // ack
				}
			}
		}
	}
}

func (w *Worker) SendEmail(payload map[string]any) error {
	dummyBody := make([]byte, 1024 * 1024)
    for i := range dummyBody {
        dummyBody[i] = 'A'
    }
    
    fmt.Printf("Sending 1MB email to %s...\n", payload["to"])

	return nil
}

func (w *Worker) ResizeImage(payload map[string]any) error {
	dummyBody := make([]byte, 1024 * 1024)
    for i := range dummyBody {
        dummyBody[i] = 'A'
    }
    
	fmt.Printf("Resizing image..., size: %v\n", payload["size"])

	return nil
}

func (w *Worker) Fail(payload map[string]any) error {
	dummyBody := make([]byte, 1024 * 1024)
    for i := range dummyBody {
        dummyBody[i] = 'A'
    }
    
	fmt.Printf("Resizing image..., size: %v\n", payload["size"])

	return errors.New("failed")
}
