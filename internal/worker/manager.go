package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/KasjanK/go-task-queue/internal/broker"
)

type WorkerInstance struct {
	Worker *Worker
	Cancel context.CancelFunc
}

type Manager struct {
	Broker     *broker.Broker
	MaxWorkers int
	Mu 		   sync.Mutex
	Workers    map[string]*WorkerInstance
}

func NewManager(b *broker.Broker, poolSize int) *Manager {
	return &Manager{
		Broker: b,
		MaxWorkers: poolSize,
		Workers: make(map[string]*WorkerInstance),
	}
}

func (m *Manager) StartPool(ctx context.Context, handlers map[string]TaskHandler) {
	m.ScaleUp(ctx, handlers, m.MaxWorkers)
}

func (m *Manager) ScaleUp(parentCtx context.Context, handlers map[string]TaskHandler, count int) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	for i := 0; i < count; i++ {
		workerCtx, cancel := context.WithCancel(parentCtx)

		w := NewWorker(m.Broker)
		w.Handlers = handlers

		instance := &WorkerInstance{
			Worker: w,
			Cancel: cancel,
		}

		m.Workers[w.ID] = instance

		go w.Run(workerCtx, m.Broker.Jobs())

		fmt.Printf("scaled UP worker %s\n", w.ID)
	}
}

func (m *Manager) ScaleDown(count int) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	removed := 0

	for id, instance := range m.Workers {
		if !instance.Worker.Busy.Load() {
			instance.Cancel()
		}

		delete(m.Workers, id)

		fmt.Printf("scaled DOWN worker %s\n", id)

		removed++

		if removed >= count {
			break
		}
	}
}

func (m *Manager) WorkerCount() int {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	return len(m.Workers)
}

func (m *Manager) AutoScale(ctx context.Context, handlers map[string]TaskHandler) {
	ticker := time.NewTicker(2 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			queueLen := len(m.Broker.PendingCh)

			workers := m.WorkerCount()

			fmt.Printf("queue=%d workers=%d\n", queueLen, workers)

			switch {
			case queueLen > 5000 && workers < 100:
				m.ScaleUp(ctx, handlers, 10)
			case queueLen > 1000 && workers < 50:
				m.ScaleUp(ctx, handlers, 5)
			case queueLen < 100 && workers > 10:
				m.ScaleDown(2)
			}
		}
	}
}

func (m *Manager) Wait() {
	for {
		allIdle := true
		m.Mu.Lock()
		for _, instance := range m.Workers {
			if instance.Worker.Busy.Load() {
				allIdle = false
				break
			}
		}
		m.Mu.Unlock()
		if allIdle {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
