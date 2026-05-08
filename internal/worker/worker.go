package worker

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/KasjanK/go-task-queue/internal/broker"
	"github.com/google/uuid"
)

type Worker struct {
	ID 		  string
	Broker    *broker.Broker
	StartedAt time.Time
	Handlers  map[string]TaskHandler
	Busy 	  atomic.Bool
}

type TaskHandler func(payload map[string]any) error

func NewWorker(b *broker.Broker) *Worker {
    return &Worker{
        ID:        uuid.New().String(),
        Broker:    b,
        StartedAt: time.Now(),
		Handlers: make(map[string]TaskHandler),
    }
}

func (w *Worker) Run(ctx context.Context, jobs <-chan *broker.Job) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}

			w.Busy.Store(true)

			job.StartedAt = time.Now()
			job.Status = "in-progress"

			handler, exists := w.Handlers[job.Type]
			if !exists {
				w.Broker.FailJob(job)
				w.Busy.Store(false)
				continue
			}
			err := handler(job.Payload)

			if err != nil {
				w.Broker.FailJob(job)
			} else {
				w.Broker.CompleteJob(job)
			}
			w.Busy.Store(false)
		}
	}
}
