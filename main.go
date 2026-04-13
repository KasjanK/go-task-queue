package main

import (
	"context"
	"fmt"
	"log"

	"github.com/KasjanK/go-task-queue/internal/broker"
	"github.com/KasjanK/go-task-queue/internal/producer"
	"github.com/KasjanK/go-task-queue/internal/worker"
	"github.com/gin-gonic/gin"
)

// TODO:
// - task performance, memory usage?, error rates
// - schedule tasks
// - dashboard, configuration, worker manager
// - add real life things to show functionality

func main() {
	broker := broker.NewBroker()
	server := producer.NewServer(broker)
	manager := worker.NewManager(broker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("Starting background worker manager...")
	manager.Watch(ctx)

	r := gin.Default()

	r.GET("/jobs", server.GetJobs)
	r.GET("/jobs/:id", server.GetJobByID)
	r.GET("/metrics", server.Metrics)
	r.GET("/dlq", server.GetDLQ)
	r.POST("/jobs", server.PostJob)
	r.POST("/jobs/dequeue", server.DequeueJob)

	if err := r.Run(); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}

	fmt.Println("shutting down manager")
	manager.Shutdown()
}

