package redisdb

import (
	"os"

	"github.com/redis/go-redis/v9"
)

func NewClient() *redis.Client {
    addr := os.Getenv("REDIS_ADDR")
    if addr == "" {
        addr = "localhost:6379"
    }
    return redis.NewClient(&redis.Options{
        Addr: addr,
    })
}
