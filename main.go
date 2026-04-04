package main

import (
	"log"

	"github.com/KasjanK/go-task-queue/internal/broker"
	"github.com/KasjanK/go-task-queue/internal/producer"
	"github.com/gin-gonic/gin"
)


func main() {
	broker := broker.NewBroker()
	server := producer.NewServer(broker)

	r := gin.Default()

	r.GET("/jobs", server.GetJobs)
	r.GET("/jobs/:id", server.GetJobByID)
	r.POST("/jobs", server.PostJob)

	if err := r.Run(); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}

