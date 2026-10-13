package redis

import (
	"context"
	"os"

	"github.com/redis/go-redis/v9"
)

func NewRedisClient(context context.Context) *redis.Client {
	db := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: os.Getenv("REDIS_PASSWORD"),
		Username: os.Getenv("REDIS_USERNAME"),
		DB:       0,
	})

	return db
}
