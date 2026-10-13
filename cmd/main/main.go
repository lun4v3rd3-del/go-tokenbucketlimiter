package main

import (
	"awesomeProject12/internal/usecase"
	"awesomeProject12/internal/utils"
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func someAPIEndpoint() {}

func main() {
	ctx := context.Background()
	logger := log.New(os.Stdout, "", log.LstdFlags)

	shardHosts := []string{
		"redis_shard_1:6379",
		"redis_shard_2:6379",
		"redis_shard_3:6379",
		"redis_shard_4:6379",
		"redis_shard_5:6379",
	}

	redisUser := os.Getenv("REDIS_USER")
	redisPassword := os.Getenv("REDIS_USER_PASSWORD")

	shards := make([]*usecase.Shard, len(shardHosts))
	for i, host := range shardHosts {
		client := redis.NewClient(&redis.Options{
			Addr:     host,
			Username: redisUser,
			Password: redisPassword,
		})

		if err := client.Ping(ctx).Err(); err != nil {
			panic(fmt.Sprintf("Ошибка подключения к шарду %d (%s): %v", i, host, err))
		}

		shards[i] = &usecase.Shard{
			Host:   host,
			Client: client,
		}
	}

	ui := []usecase.Identifier{
		usecase.NewVipIdentifier("vip"),
		usecase.NewDefaultIdentifier("default"),
	}
	userService := usecase.NewUserService(ui)

	limiter := usecase.NewTokenBucketLimiter(userService, shards)

	_ = limiter.Register("default", []usecase.FuncConfigPair{
		{
			Func:   someAPIEndpoint,
			Config: usecase.Config{Rate: 0.1, Capacity: 3.0},
		},
	}, ctx)
	_ = limiter.Register("vip", []usecase.FuncConfigPair{
		{
			Func:   someAPIEndpoint,
			Config: usecase.Config{Rate: 1, Capacity: 10.0},
		},
	}, ctx)

	var countConsumers = 200
	var iterations = 10
	var wg sync.WaitGroup

	for _, shard := range shards {
		shard.Client.FlushDB(ctx)
	}

	for i := 0; i < countConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			req, _ := http.NewRequest("GET", "/api/data", nil)
			req.RemoteAddr = fmt.Sprintf("192.168.1.%d", consumerID)

			if consumerID%2 == 0 {
				req.Header.Set("Authorization", fmt.Sprintf("Bearer vip-token-secret-%d", consumerID))
			} else {
				req.Header.Set("Authorization", "Bearer regular-token")
			}

			for j := 0; j < iterations; j++ {
				limiter.Process(req, someAPIEndpoint, ctx)
				time.Sleep(time.Duration(rand.IntN(100)+50) * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("\nВсе горутины завершили работу. Сбор статистики...")

	buckets, err := utils.CountActiveBuckets(context.Background(), limiter)
	if err != nil {
		logger.Printf("Ошибка сбора статистики: %v", err)
		return
	}

	utils.DrawShardLoadChart(limiter.Getshards(), buckets)
}
