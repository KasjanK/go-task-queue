# Go Task Queue

A distributed task queue built in Go with Redis persistence, 
automatic worker scaling, and retry logic.

## Features
- Redis-backed job queue with AOF persistence
- Configurable worker pool with autoscaling
- Exponential backoff retry with jitter
- Dead letter queue for failed jobs
- HTTP API for job management and monitoring

## Getting Started

### Prerequisites
- Go
- Docker

### Running
\```bash
docker compose up --build
\```

## API Reference
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /jobs | Enqueue a job |
| GET | /jobs/:id | Get job by ID |
| GET | /metrics | Queue metrics |
| GET | /dlq | Dead letter queue |
| GET | /completed_jobs | Completed jobs |
| GET | /queues | All queues |
| DELETE | /queues/delete/:name | Delete a queue |

## Configuration
Use the config struct in internal/config/config.go to configure your queue.

 - BufferSize: the buffer of the queue
 - PoolSize: how many workers?
 - ScaleUpThreshold: at how many jobs should the autoscaler increase the amount of workers
 - ScaleDownThreshold: at how many jobs in queue should the autoscaler decrease the amount of workers
 - MinWorkers: the minimum amount of workers available
