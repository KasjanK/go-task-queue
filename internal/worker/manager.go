package worker

import (
	"context"

	"github.com/KasjanK/go-task-queue/internal/broker"
)

type Manager struct {
	Broker     *broker.Broker
	MaxWorkers int
}

func NewManager(b *broker.Broker, poolSize int) *Manager {
	return &Manager{
		Broker: b,
		MaxWorkers: poolSize,
	}
}

func (m *Manager) StartPool(ctx context.Context, handlers map[string]TaskHandler) {
	jobStream := m.Broker.Jobs()
	for i := 0; i < m.MaxWorkers; i++ {
		w := NewWorker(m.Broker)
		w.Handlers = handlers
		go w.Run(ctx, jobStream)
	}
}
