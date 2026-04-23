package producer

import (
	"net/http"

	"github.com/KasjanK/go-task-queue/internal/broker"
	"github.com/gin-gonic/gin"
)

type Server struct {
	Broker *broker.Broker
}

func NewServer(b *broker.Broker) *Server {
	return &Server{
		Broker: b,
	}
}

func (s *Server) GetJobByID(c *gin.Context) {
	id := c.Param("id")
	job, err := s.Broker.GetJob(id)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": err.Error()})
		return
	}

	c.IndentedJSON(http.StatusOK, job)
}

func (s *Server) PostJob(c *gin.Context) {
	var newJob broker.Job
	if err := c.BindJSON(&newJob); err != nil {
		c.IndentedJSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	createdJob := s.Broker.Enqueue(newJob)	
	c.IndentedJSON(http.StatusCreated, createdJob)
}

func (s *Server) DequeueJob(c *gin.Context) {
	queueName := c.Param("queuename")
	job, err := s.Broker.Dequeue(queueName)
	if err != nil {
		c.IndentedJSON(http.StatusNoContent, gin.H{"message": err.Error()})
		return
	}

	c.IndentedJSON(http.StatusOK, job)
}

func (s *Server) Metrics(c *gin.Context) {
	metrics := s.Broker.GetMetricsNew()
	c.IndentedJSON(http.StatusOK, metrics)
}

func (s *Server) GetDLQ(c *gin.Context) {
	dlq := s.Broker.GetDLQ()
	c.IndentedJSON(http.StatusOK, dlq)
}

func (s *Server) GetCompletedJobs(c *gin.Context) {
	completedJobs := s.Broker.GetCompletedJobs()
	c.IndentedJSON(http.StatusOK, completedJobs)
}

func (s *Server) GetQueues(c *gin.Context) {
    queues := s.Broker.GetAllJobs()
    
    c.IndentedJSON(http.StatusOK, queues)
}

func (s *Server) DeleteQueue(c *gin.Context) {
	queueName := c.Param("queuename")
	queues := s.Broker.GetAllJobs()	

	err := s.Broker.DeleteQueue(queueName)
	if err != nil {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": err.Error()})
		return 
	}

	c.IndentedJSON(http.StatusOK, queues)
}
