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

func (s *Server) GetJobs(c *gin.Context) {
	jobs := s.Broker.GetAllJobs()
	c.IndentedJSON(http.StatusOK, jobs)
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
