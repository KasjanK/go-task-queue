package main

import (
	"context"
	"fmt"
	"log"

	"net/http"
	_ "net/http/pprof"

	"github.com/KasjanK/go-task-queue/internal/broker"
	"github.com/KasjanK/go-task-queue/internal/producer"
	"github.com/KasjanK/go-task-queue/internal/worker"
	"github.com/gin-gonic/gin"
)

// TODO:
// - deletion of queues
// - optimize manager and metrics
// - change stopworker to specific queue instead of random
// - manager logic to assign workers evenly between queues
// - memory usage?, error rates
// - schedule tasks
// - dashboard, configuration
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

	go func() {
		fmt.Println("Profiler running on localhost:6060")
		http.ListenAndServe("localhost:6060", nil)
	}()

	//r.GET("/jobs", server.GetJobs)
	r.GET("/jobs/:id", server.GetJobByID)
	r.GET("/metrics", server.Metrics)
	r.GET("/dlq", server.GetDLQ)
	r.GET("/completed_jobs", server.GetCompletedJobs)
	r.GET("/queues", server.GetQueues)
	r.POST("/jobs", server.PostJob)
	r.POST("/jobs/dequeue/:queuename", server.DequeueJob)

	if err := r.Run(); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}

	fmt.Println("shutting down manager")
	manager.Shutdown()
}

