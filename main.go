package main

import (
	"log"

	"github.com/KasjanK/go-task-queue/internal/broker"
	"github.com/KasjanK/go-task-queue/internal/producer"
	"github.com/KasjanK/go-task-queue/internal/worker"
	"github.com/gin-gonic/gin"
)

// TODO:
// - task performance, memory usage?, error rates
// - make a separate slice with dead jobs
// - schedule tasks
// - dashboard, configuration, worker manager
// - add real life things to show functionality

func main() {
	broker := broker.NewBroker()
	server := producer.NewServer(broker)
	w := worker.NewWorker(broker)
	w1 := worker.NewWorker(broker)
	w2 := worker.NewWorker(broker)

	go w.Run(broker)
	go w1.Run(broker)
	go w2.Run(broker)

	r := gin.Default()

	r.GET("/jobs", server.GetJobs)
	r.GET("/jobs/:id", server.GetJobByID)
	r.GET("/metrics", server.Metrics)
	r.POST("/jobs", server.PostJob)
	r.POST("/jobs/dequeue", server.DequeueJob)

	if err := r.Run(); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}

