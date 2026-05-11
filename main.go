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
	"github.com/KasjanK/go-task-queue/internal/config"
	"github.com/KasjanK/go-task-queue/internal/producer"
	"github.com/KasjanK/go-task-queue/internal/worker"
	"github.com/gin-gonic/gin"
)

// TODO:
// - log http shutdown error
// - persistance: db, redis
// - schedule tasks
// - dashboard, configuration
// - add real life things to show functionality

func sendEmail(payload map[string]any) error {
    time.Sleep(50 * time.Millisecond) 
	return nil
}

func resizeImage(payload map[string]any) error {
    time.Sleep(50 * time.Millisecond) 
	return nil
}

func failJob(payload map[string]any) error {
    time.Sleep(50 * time.Millisecond) 
	return errors.New("failed")
}

func main() {
	cfg := config.Config{
		BufferSize: 1000,    // buffersize for the queue
		PoolSize: 20,        // set worker pool size
		DispatchRate: 1000,  // a rate at which the dispatcher limits the load
		ScaleUpThreshold: 5000,
		ScaleDownThreshold: 100,
		MinWorkers: 5,
	}

	broker := broker.NewBroker(cfg.BufferSize)
	server := producer.NewServer(broker)
	manager := worker.NewManager(broker, cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	handlers := map[string]worker.TaskHandler{
		"email": sendEmail,
		"resizer": resizeImage,
		"fail": failJob,
	}

	fmt.Println("Starting background worker pool...")
	manager.StartPool(ctx, handlers)
	broker.StartDispatcher(ctx, cfg.DispatchRate)
	go manager.AutoScale(ctx, handlers)

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
	manager.Wait()
	log.Println("Shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv.Shutdown(shutdownCtx)
}

