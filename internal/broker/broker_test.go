package broker

import (
	"sync"
	"testing"
)

func TestQueueIsolation(t *testing.T) {
    b := NewBroker()
    b.Enqueue(Job{Type: "email"})
    b.Enqueue(Job{Type: "image"})

    _, err := b.Dequeue("sms")
    if err == nil {
        t.Error("Expected error for empty/non-existent queue")
    }

    job, _ := b.Dequeue("email")
    if job.Type != "email" {
        t.Errorf("Expected email job, got %s", job.Type)
    }
}

func TestRetryLogic(t *testing.T) {
    b := NewBroker()
    job := b.Enqueue(Job{Type: "flaky"})
    
    for i := 0; i < 3; i++ {
        b.Dequeue("flaky")
        b.FailJob(job.ID, "flaky")
    }

    dlq := b.GetDLQ()
    if len(dlq) != 1 {
        t.Errorf("Expected 1 job in DLQ, got %d", len(dlq))
    }
    if dlq[0].Status != "failed" {
        t.Errorf("Expected status failed, got %s", dlq[0].Status)
    }
}

func TestJobLifecycle(t *testing.T) {
    b := NewBroker()
    
    job := Job{Type: "email", Payload: map[string]any{"to": "test@test.com"}}
    enqueued := b.Enqueue(job)
    
    if enqueued.ID == "" {
        t.Fatal("Expected job ID to be generated")
    }

    dequeued, err := b.Dequeue("email")
    if err != nil {
        t.Fatalf("Failed to dequeue: %v", err)
    }
    if dequeued.Status != "in-progress" {
        t.Errorf("Expected status in-progress, got %s", dequeued.Status)
    }

    err = b.CompleteJob(dequeued.ID, "email")
    if err != nil {
        t.Fatalf("Failed to complete job: %v", err)
    }
}

func TestConcurrency(t *testing.T) {
    b := NewBroker()
    wg := sync.WaitGroup{}
    iterations := 100

    for i := 0; i < iterations; i++ {
        wg.Add(2)
        go func() {
            defer wg.Done()
            b.Enqueue(Job{Type: "heavy"})
        }()
        go func() {
            defer wg.Done()
            b.GetMetrics()
        }()
    }
    wg.Wait()
}
