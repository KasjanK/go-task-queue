package worker

import (
	"context"
	"fmt"
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
			job, err := w.Broker.Dequeue()
			if err != nil {
				continue
			}

			switch job.Type {
			case "email":
				err := w.SendEmail(job.Payload)
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
	to := payload["to"]
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
	}

	fmt.Printf("[WORKER ID: %v] Sending email to %v, Subject: %v, Body: %v\n", w.ID, to, subject, body)
	time.Sleep(2 * time.Second)
	return nil
}
