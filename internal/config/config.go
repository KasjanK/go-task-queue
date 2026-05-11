package config

type Config struct {
	BufferSize   int
	PoolSize     int
	DispatchRate int
	ScaleUpThreshold int
	ScaleDownThreshold int
	MinWorkers int
}
