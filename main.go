package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/KasjanK/go-task-queue/internal/broker"
	"github.com/KasjanK/go-task-queue/internal/producer"
	"github.com/KasjanK/go-task-queue/internal/worker"
	"github.com/gin-gonic/gin"
)

// TODO:
// - FailJob
// - batching or job limiting
// - memory usage?, error rates
// - schedule tasks
// - dashboard, configuration
// - add real life things to show functionality

func sendEmail(payload map[string]any) error {
	dummyBody := make([]byte, 1024 * 1024)
    for i := range dummyBody {
        dummyBody[i] = 'A'
    }
    
	return nil
}

func resizeImage(payload map[string]any) error {
	dummyBody := make([]byte, 1024 * 1024)
    for i := range dummyBody {
        dummyBody[i] = 'A'
    }

	return nil
}

func failJob(payload map[string]any) error {
	dummyBody := make([]byte, 1024 * 1024)
    for i := range dummyBody {
        dummyBody[i] = 'A'
    }

	return errors.New("failed")
}

func main() {
	broker := broker.NewBroker(1000)
	server := producer.NewServer(broker)
	manager := worker.NewManager(broker, 20)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	handlers := map[string]worker.TaskHandler{
		"email": sendEmail,
		"resizer": resizeImage,
		"fail": failJob,
	}

	fmt.Println("Starting background worker pool...")
	manager.StartPool(ctx, handlers)

	r := gin.Default()

	r.GET("/jobs/:id", server.GetJobByID)
	r.GET("/metrics", server.Metrics)
	r.GET("/dlq", server.GetDLQ)
	r.GET("/completed_jobs", server.GetCompletedJobs)
	r.GET("/queues", server.GetQueues)
	r.POST("/jobs", server.PostJob)
	r.DELETE("/queues/delete/:queuename", server.DeleteQueue)

	srv := &http.Server{
		Addr: ":8080",
		Handler: r,
	}

	go func() {
		log.Println("Server running on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	go func() {
		log.Println("Profiler running on localhost:6060")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Printf("profiler error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv.Shutdown(shutdownCtx)
}

