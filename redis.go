package tdb

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient wraps [redis.Client] and periodically exports connection-pool gauges from a background goroutine.
// Call [RedisClient.Close] when the client is no longer needed to stop the reporter and release connections.
type RedisClient struct {
	client *redis.Client

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewRedisClient creates a client for logical database 0, verifies connectivity with Ping, and starts the metrics reporter goroutine.
func NewRedisClient(ctx context.Context, address, password string, poolSize int) (*RedisClient, error) {
	return newRedisClient(ctx, address, password, 0, poolSize)
}

// NewRedisClientEx behaves like [NewRedisClient] but selects the Redis logical database identified by db.
func NewRedisClientEx(ctx context.Context, address, password string, db int, poolSize int) (*RedisClient, error) {
	return newRedisClient(ctx, address, password, db, poolSize)
}

func newRedisClient(ctx context.Context, address, password string, db int, poolSize int) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       db,
		PoolSize: poolSize,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := client.Ping(pingCtx).Result()
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	redisClient := &RedisClient{
		client: client,
		stop:   make(chan struct{}),
	}

	redisClient.wg.Add(1)

	go redisClient.runPoolMetricsReporter()

	return redisClient, nil
}

func (p *RedisClient) runPoolMetricsReporter() {
	defer p.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			reportRedisPoolMetrics(p.client)
		}
	}
}

func reportRedisPoolMetrics(client *redis.Client) {
	poolStats := client.PoolStats()

	RedisPoolOpGauge.Set(float64(poolStats.Hits), "hit")
	RedisPoolOpGauge.Set(float64(poolStats.Misses), "miss")
	RedisPoolOpGauge.Set(float64(poolStats.Timeouts), "timeout")
	RedisPoolOpGauge.Set(float64(poolStats.StaleConns), "stale")
	RedisConnStatusGauge.Set(float64(poolStats.IdleConns), "idle")
	RedisConnStatusGauge.Set(float64(poolStats.TotalConns-poolStats.IdleConns), "active")
}

// Close stops the metrics reporter, closes the underlying Redis client, and may be called safely more than once.
func (p *RedisClient) Close() error {
	var err error

	p.closeOnce.Do(func() {
		close(p.stop)

		p.wg.Wait()

		err = p.client.Close()
	})

	return err
}

// Client returns the underlying [redis.Client].
func (p *RedisClient) Client() *redis.Client {
	return p.client
}
