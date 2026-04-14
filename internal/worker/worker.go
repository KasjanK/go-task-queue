package worker

import (
	"context"
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
}

type JobStats struct {
	CPUStartTime int64
	MemAllocated uint64 
}

func NewWorker(b *broker.Broker) *Worker {
    return &Worker{
        ID:        uuid.New().String(),
        Broker:    b,
        StartedAt: time.Now(),
        IsRunning: true,
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
			job, err := w.Broker.Dequeue()
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
					w.Broker.FailJob(job.ID) 	 // nack
				} else {
					w.Broker.CompleteJob(job.ID) // ack
				}
			}
		}
	}
}

func (w *Worker) SendEmail(payload map[string]any) error {
/*	to := payload["to"]
	if to == nil {
		return fmt.Errorf("no recipient")
	}

	subject := payload["subject"]
	if subject == nil {
		subject = "no subject"
	}

	body := payload["body"]
	if body == nil {
		body = "no body"
	}*/

	dummyBody := make([]byte, 1024 * 1024)
    for i := range dummyBody {
        dummyBody[i] = 'A'
    }
    
    fmt.Printf("Sending 1MB email to %s...\n", payload["to"])

	//fmt.Printf("[WORKER ID: %v] Sending email to %v, Subject: %v, Body: %v\n", w.ID, to, subject, body)
//	time.Sleep(2 * time.Second)
	return nil
}
