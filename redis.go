/**
 * @Author: lidonglin
 * @Description:
 * @File:  redis.go
 * @Version: 1.0.0
 * @Date: 2023/11/15 21:43
 */

package tdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(ctx context.Context, address, password string, poolSize int) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       0,
		PoolSize: poolSize,
	})

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	go func() {
		ticker := time.NewTicker(time.Second)

		for range ticker.C {
			poolStats := client.PoolStats()

			RedisPoolOpGauge.Set(float64(poolStats.Hits), "hit")
			RedisPoolOpGauge.Set(float64(poolStats.Misses), "miss")
			RedisPoolOpGauge.Set(float64(poolStats.Timeouts), "timeout")
			RedisPoolOpGauge.Set(float64(poolStats.StaleConns), "stale")
			RedisConnStatusGauge.Set(float64(poolStats.IdleConns), "idle")
			RedisConnStatusGauge.Set(float64(poolStats.TotalConns-poolStats.IdleConns), "active")
		}
	}()

	redisClient := &RedisClient{
		client: client,
	}

	return redisClient, nil
}

func (p *RedisClient) Client(ctx context.Context) *redis.Client {
	return p.client
}
