package broker

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

func (b *Broker) GetMetricsNew() Metrics {
	var totalDuration float64
	for _, job := range b.CompletedJobs {
		totalDuration += job.Duration

	}
	return Metrics{
		TotalEnqueued: b.TotalEnqueued,
		TasksPending: b.TotalPending,
		TasksInProgress: b.TasksInProgress,
		TasksCompleted: b.TasksSucceeded,
		TotalProcessed: b.TotalFailed + b.TasksSucceeded,
		TasksFailed: b.TotalFailed,	
		AvgDuration: (totalDuration / float64(b.TasksSucceeded)) * 1000,
		JobsByType: b.JobsByType,
		TotalRetries: b.TotalRetries,
	}
}
