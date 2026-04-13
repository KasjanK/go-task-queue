package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/KasjanK/go-task-queue/internal/broker"
)

type Manager struct {
	Broker     *broker.Broker
	Workers    map[string]context.CancelFunc
	MaxWorkers int
	mu 		   sync.Mutex
	cancel 	   context.CancelFunc
}

func NewManager(b *broker.Broker) *Manager {
	return &Manager{
		Broker: b,
		Workers: map[string]context.CancelFunc{},
		MaxWorkers: 10,
		mu: sync.Mutex{},
	}
}

func (m *Manager) StartWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	worker := NewWorker(m.Broker)

	m.mu.Lock()
	m.Workers[worker.ID] = cancel
	m.mu.Unlock()

	fmt.Println("Starting worker...")
	go worker.Run(ctx)
}

func (m *Manager) StopWorker() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	for id, cancel := range m.Workers {
		cancel()
		delete(m.Workers, id)
		fmt.Printf("Worker %v decomissioned", id)
		return
	}
}

func (m *Manager) Watch(ctx context.Context) {
	var managerCtx context.Context
    managerCtx, m.cancel = context.WithCancel(ctx)

	ticker := time.NewTicker(5 * time.Second)

	m.StartWorker()

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				metrics := m.Broker.GetMetrics()
				fmt.Printf("Got metrics, %v", metrics)
				
				if metrics.TasksPending > 10 && len(m.Workers) < m.MaxWorkers {
					m.StartWorker()
				}
				if metrics.TasksPending == 0 && len(m.Workers) > 0 {
					m.StopWorker()
				}
			case <-managerCtx.Done():
				fmt.Println("Goroutine stopped!") 
				return 
			}
		}
	}()
}

func (m *Manager) Shutdown() {
	if m.cancel != nil {
		m.cancel()
	}
}
