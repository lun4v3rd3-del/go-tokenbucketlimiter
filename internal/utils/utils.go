package utils

import (
	"awesomeProject12/internal/usecase"
	"context"
	"fmt"
	"strings"
)

func CountActiveBuckets(ctx context.Context, l *usecase.TokenBucketLimiter) ([]int, error) {
	shards := l.Getshards()
	shardsCount := make([]int, len(shards))
	matchPattern := fmt.Sprintf("%s:*", usecase.BucketHashKey)

	for i, shard := range shards {
		var cursor uint64
		for {
			keys, nextCursor, err := shard.Client.Scan(ctx, cursor, matchPattern, 1000).Result()
			if err != nil {
				return nil, fmt.Errorf("ошибка сканирования шарда %s: %w", shard.Host, err)
			}

			shardsCount[i] += len(keys)
			cursor = nextCursor

			if cursor == 0 {
				break
			}
		}
	}

	return shardsCount, nil
}

func DrawShardLoadChart(shards []*usecase.Shard, counts []int) {
	var totalBuckets int
	var maxBuckets int
	for _, c := range counts {
		totalBuckets += c
		if c > maxBuckets {
			maxBuckets = c
		}
	}

	fmt.Println("\n================ ДИАГРАММА ЗАГРУЖЕННОСТИ ШАРДОВ ================")
	fmt.Printf("Всего активных бакетов (ключей): %d\n\n", totalBuckets)

	const maxBarLength = 40

	for i, shard := range shards {
		count := counts[i]

		percentage := 0.0
		barLength := 0
		if totalBuckets > 0 {
			percentage = (float64(count) / float64(totalBuckets)) * 100
			barLength = int((float64(count) / float64(maxBuckets)) * maxBarLength)
		}

		bar := strings.Repeat("█", barLength) + strings.Repeat("░", maxBarLength-barLength)

		fmt.Printf("[%d] %-20s | %s | %3d бак. (%5.1f%%)\n",
			i+1, shard.Host, bar, count, percentage)
	}
	fmt.Println("================================================================\n")
}
