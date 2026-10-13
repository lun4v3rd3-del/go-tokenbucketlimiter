package main

import (
	"awesomeProject12/internal/usecase"
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type vipIdentifier struct {
	secret string
}

type defaultIdentifier struct {
	group string
}

type userServiceImpl struct {
	identifiers []usecase.Identifier
}

func (s *userServiceImpl) GetUserKey(req interface{}) (string, error) {
	httpReq, ok := req.(*http.Request)
	if !ok {
		return "", fmt.Errorf("invalid request type")
	}
	auth := httpReq.Header.Get("Authorization")
	if strings.Contains(auth, "vip") {
		return "user_vip_" + httpReq.RemoteAddr, nil
	}
	return "user_default_" + httpReq.RemoteAddr, nil
}

func (s *userServiceImpl) GetGroupName(userKey string) string {
	if strings.Contains(userKey, "vip") {
		return "vip"
	}
	return "default"
}

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

	userService := usecase.NewUserService([]usecase.Identifier{
		usecase.NewVipIdentifier("vip"),
		usecase.NewDefaultIdentifier("default"),
	})

	limiter := usecase.NewTokenBucketLimiter(userService, shards)

	err := limiter.Register("default", []usecase.FuncConfigPair{
		{
			Func:   someAPIEndpoint,
			Config: usecase.Config{Rate: 0.1, Capacity: 3.0},
		},
	}, ctx)
	if err != nil {
		panic(err)
	}

	err = limiter.Register("vip", []usecase.FuncConfigPair{
		{
			Func:   someAPIEndpoint,
			Config: usecase.Config{Rate: 1, Capacity: 10.0},
		},
	}, ctx)
	if err != nil {
		panic(err)
	}

	var countConsumers = 2
	var iterations = 1000
	var wg sync.WaitGroup

	for i := 0; i < countConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			req, _ := http.NewRequest("GET", "/api/data", nil)
			req.RemoteAddr = fmt.Sprintf("192.168.1.%d", consumerID)

			if consumerID%2 == 0 {
				req.Header.Set("Authorization", "Bearer vip-token-secret")
			} else {
				req.Header.Set("Authorization", "Bearer regular-token")
			}

			for j := 0; j < iterations; j++ {
				if limiter.Process(req, someAPIEndpoint, ctx) {
					logger.Printf("+ [Consumer #%d] шаг %d: УСПЕХ\n", consumerID, j)
				} else {
					logger.Printf("- [Consumer #%d] шаг %d: ОТКЛОНЕНО\n", consumerID, j)
				}
				time.Sleep(time.Duration(rand.IntN(100)+50) * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("Все горутины завершили работу.")
}
