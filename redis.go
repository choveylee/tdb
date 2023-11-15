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

	"github.com/go-redis/redis/v8"
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

	redisClient := &RedisClient{
		client: client,
	}

	return redisClient, nil
}
