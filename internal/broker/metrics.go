package broker

import "fmt"

type Metrics struct {
	TotalProcessed  int  		   `json:"total_processed"`
	TotalEnqueued   int 		   `json:"total_enqueued"`

	TasksPending    int			   `json:"tasks_pending"`
	TasksCompleted  int			   `json:"tasks_succeeded"`
	TasksInProgress int 		   `json:"tasks_in_progress"`
	TasksFailed     int 		   `json:"tasks_failed"`
	TotalRetries    int			   `json:"total_retried"`

	AvgDuration     float64        `json:"avg_duration"`
	JobsByType      map[string]int `json:"jobs_by_type"`
}

func (b *Broker) GetMetrics() Metrics {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	var m Metrics
	m.JobsByType = make(map[string]int)
	var totalDuration float64

	queue, exists := b.Queues["email"]
	if !exists {
		fmt.Println("No queues to fetch")
		return Metrics{}
	}

	for _, job := range queue.Jobs {
		switch job.Status {
		case "pending":
			m.TasksPending++
		case "in-progress":
			m.TasksInProgress++
		}
		m.JobsByType[job.Type]++
		m.TotalRetries += job.RetryCount
	}

	for _, job := range b.CompletedJobs {
		m.TasksCompleted++	
		totalDuration += job.Duration

		m.JobsByType[job.Type]++
	}

	for _, job := range b.Dlq {
		m.TasksFailed++

		m.JobsByType[job.Type]++
		m.TotalRetries += job.RetryCount
	}

	m.TotalProcessed = m.TasksFailed + m.TasksCompleted + m.TasksPending
	//m.TotalEnqueued = len(b.Jobs)

	if m.TasksCompleted > 0 {
		m.AvgDuration = (totalDuration / float64(m.TasksCompleted)) * 1000
	}

	return m
}
