package tdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient owns a go-redis client and its registration in the pool metrics reporter.
type RedisClient struct {
	client    *redis.Client
	closeOnce sync.Once
	closeErr  error
}

var redisPools = struct {
	sync.Mutex
	clientsMap map[*redis.Client]struct{}
	stop       chan struct{}
}{
	clientsMap: make(map[*redis.Client]struct{}),
}

// NewRedisClient connects to logical database 0 and verifies connectivity with Ping.
func NewRedisClient(ctx context.Context, address, password string, poolSize int) (*RedisClient, error) {
	return newRedisClient(ctx, address, password, 0, poolSize)
}

// NewRedisClientEx connects to the requested Redis logical database.
func NewRedisClientEx(ctx context.Context, address, password string, db int, poolSize int) (*RedisClient, error) {
	return newRedisClient(ctx, address, password, db, poolSize)
}

// Close deregisters the pool and releases its connections. Concurrent callers
// receive the result of the same underlying close.
func (client *RedisClient) Close() error {
	if client == nil || client.client == nil {
		return nil
	}

	client.closeOnce.Do(func() {
		redisPools.Lock()
		delete(redisPools.clientsMap, client.client)
		if len(redisPools.clientsMap) == 0 {
			close(redisPools.stop)
			redisPools.stop = nil
		}

		reportRedisPoolMetrics()
		redisPools.Unlock()

		if err := client.client.Close(); err != nil {
			client.closeErr = fmt.Errorf("tdb: close Redis client: %w", err)
		}
	})

	return client.closeErr
}

// Client returns the underlying client for Redis commands.
func (client *RedisClient) Client() *redis.Client {
	return client.client
}

// newRedisClient registers a pool only after a successful, bounded health check.
func newRedisClient(ctx context.Context, address, password string, db int, poolSize int) (*RedisClient, error) {
	if ctx == nil {
		return nil, errors.New("tdb: context must not be nil")
	}

	if strings.TrimSpace(address) == "" {
		return nil, errors.New("tdb: Redis address must not be empty")
	}

	if db < 0 || poolSize < 0 {
		return nil, errors.New("tdb: Redis database index and pool size must be non-negative")
	}

	options := &redis.Options{
		Addr:                  address,
		Password:              password,
		DB:                    db,
		PoolSize:              poolSize,
		ContextTimeoutEnabled: true,
	}
	client := redis.NewClient(options)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()

		return nil, fmt.Errorf("tdb: ping Redis: %w", err)
	}

	redisPools.Lock()
	redisPools.clientsMap[client] = struct{}{}
	if redisPools.stop == nil {
		stop := make(chan struct{})
		redisPools.stop = stop
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					redisPools.Lock()
					if redisPools.stop != stop {
						redisPools.Unlock()

						return
					}

					reportRedisPoolMetrics()
					redisPools.Unlock()
				}
			}
		}()
	}

	reportRedisPoolMetrics()
	redisPools.Unlock()

	redisClient := &RedisClient{client: client}

	return redisClient, nil
}

// reportRedisPoolMetrics combines samples from the live pools. Callers hold redisPools.
func reportRedisPoolMetrics() {
	var hits, misses, timeouts, stale, idle, active uint64
	for client := range redisPools.clientsMap {
		stats := client.PoolStats()
		hits += uint64(stats.Hits)
		misses += uint64(stats.Misses)
		timeouts += uint64(stats.Timeouts)
		stale += uint64(stats.StaleConns)
		idle += uint64(stats.IdleConns)
		if stats.TotalConns > stats.IdleConns {
			active += uint64(stats.TotalConns - stats.IdleConns)
		}
	}

	if RedisPoolOpGauge != nil {
		_ = RedisPoolOpGauge.Set(float64(hits), "hit")
		_ = RedisPoolOpGauge.Set(float64(misses), "miss")
		_ = RedisPoolOpGauge.Set(float64(timeouts), "timeout")
		_ = RedisPoolOpGauge.Set(float64(stale), "stale")
	}

	if RedisConnStatusGauge != nil {
		_ = RedisConnStatusGauge.Set(float64(idle), "idle")
		_ = RedisConnStatusGauge.Set(float64(active), "active")
	}
}
