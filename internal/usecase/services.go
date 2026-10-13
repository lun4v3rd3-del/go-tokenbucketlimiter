package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const ConfigsHashKey = "rate_limits:configs"
const BucketHashKey = "rate_limits:buckets"
const maxRetries = 5

type Config struct {
	Rate     float64 `json:"rate"`
	Capacity float64 `json:"capacity"`
}

type UserConfig struct {
	FuncMap map[string]*Config `json:"funcMap"`
}

type Shard struct {
	Host   string
	Client *redis.Client
}

type TokenBucketLimiter struct {
	userService *UserService
	shards      []*Shard
	configMu    sync.RWMutex
	configCache map[string]UserConfig
}

func NewTokenBucketLimiter(userService *UserService, shards []*Shard) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		userService: userService,
		shards:      shards,
		configCache: make(map[string]UserConfig),
	}
}

func (l *TokenBucketLimiter) getShardId(key string) int {
	w := fnv.New64()
	_, _ = w.Write([]byte(key))
	id := w.Sum64()
	return int(id % uint64(len(l.shards)))
}

type FuncConfigPair struct {
	Func   interface{}
	Config Config
}

func (l *TokenBucketLimiter) Register(groupName string, funcMap []FuncConfigPair, ctx context.Context) error {
	parsedFuncMap := make(map[string]*Config, len(funcMap))

	for _, pair := range funcMap {
		name := getFuncName(pair.Func)

		configCopy := pair.Config
		parsedFuncMap[name] = &configCopy
	}

	cfg := UserConfig{
		FuncMap: parsedFuncMap,
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	for _, shard := range l.shards {
		err = shard.Client.HSet(ctx, ConfigsHashKey, groupName, data).Err()
		if err != nil {
			return err
		}
	}

	l.configMu.Lock()
	if l.configCache == nil {
		l.configCache = make(map[string]UserConfig)
	}
	l.configCache[groupName] = cfg
	l.configMu.Unlock()

	return nil
}

func (l *TokenBucketLimiter) getGroupConfig(ctx context.Context, groupName string) (UserConfig, error) {
	l.configMu.RLock()
	cfg, ok := l.configCache[groupName]
	l.configMu.RUnlock()

	if ok {
		return cfg, nil
	}

	if len(l.shards) == 0 {
		return UserConfig{}, errors.New("no shards available")
	}

	shardId := l.getShardId(groupName)
	shard := l.shards[shardId]

	dataStr, err := shard.Client.HGet(ctx, ConfigsHashKey, groupName).Result()
	if errors.Is(err, redis.Nil) || dataStr == "" {
		return UserConfig{}, redis.Nil
	}

	err = json.Unmarshal([]byte(dataStr), &cfg)
	if err != nil {
		return UserConfig{}, err
	}

	l.configMu.Lock()
	l.configCache[groupName] = cfg
	l.configMu.Unlock()
	return cfg, nil
}

func (l *TokenBucketLimiter) Process(req interface{}, f interface{}, ctx context.Context) bool {
	userKey, groupName, err := l.userService.GetUserKey(req)
	if err != nil {
		return false
	}

	funcName := getFuncName(f)

	//logger := log.New(os.Stdout, "", log.LstdFlags)
	//logger.Printf("userKey: %s", userKey)
	//logger.Printf("funcName: %s", funcName)

	userCfg, err := l.getGroupConfig(ctx, groupName)
	if err != nil {
		return false
	}
	//logger.Printf("funcMap keys: %s", strings.Join(slices.Collect(maps.Keys(userCfg.FuncMap)), ", "))

	cfg, exists := userCfg.FuncMap[funcName]
	if !exists {
		return false
	}

	bucketKey := fmt.Sprintf("%s:%s:%s", BucketHashKey, userKey, funcName)

	shardId := l.getShardId(bucketKey)
	shard := l.shards[shardId]

	for i := 0; i < maxRetries; i++ {
		var localAllowed bool

		err = shard.Client.Watch(ctx, func(tx *redis.Tx) error {
			now := time.Now()
			nowSeconds := float64(now.UnixNano()) / float64(time.Second)

			data, err := tx.HMGet(ctx, bucketKey, "tokens", "last_time").Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}

			var tokens float64
			var lastTimeSeconds float64

			if len(data) < 2 || data[0] == nil || data[1] == nil {
				tokens = cfg.Capacity
				lastTimeSeconds = nowSeconds
			} else {
				tokens, _ = strconv.ParseFloat(data[0].(string), 64)
				lastTimeSeconds, _ = strconv.ParseFloat(data[1].(string), 64)

				elapsed := nowSeconds - lastTimeSeconds
				if elapsed > 0 {
					tokens = tokens + (elapsed * cfg.Rate)
					if tokens > cfg.Capacity {
						tokens = cfg.Capacity
					}
					lastTimeSeconds = nowSeconds
				}
			}

			if tokens >= 1 {
				tokens = tokens - 1
				localAllowed = true
			} else {
				localAllowed = false
			}

			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.HSet(ctx, bucketKey, map[string]interface{}{
					"tokens":    tokens,
					"last_time": lastTimeSeconds,
				})
				pipe.Expire(ctx, bucketKey, 24*time.Hour)
				return nil
			})

			return err
		}, bucketKey)

		if err == nil {
			return localAllowed
		}

		if errors.Is(err, redis.TxFailedErr) {
			continue
		}

		return false
	}

	return false
}

func (l *TokenBucketLimiter) Getshards() []*Shard {
	return l.shards
}
