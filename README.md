# tdb

`tdb` is a Go library that provides reusable infrastructure components for backend services. It includes **MySQL** access built on GORM with OpenTelemetry integration and SQL latency metrics, **Redis** clients with connection-pool metrics, **Kafka** producers and consumer groups built on Sarama, optional **monthly table sharding** for GORM models, and **Prometheus-style** metrics via `tmetric`.

## Requirements

- Go **1.27.4** or newer (see `go.mod`).

## Installation

```bash
go get github.com/choveylee/tdb
```

## Documentation

Inspect the full package reference locally with:

```bash
go doc -all github.com/choveylee/tdb
```

The top-level package overview is defined in [`doc.go`](doc.go) and is written for compatibility with both `go doc` and pkg.go.dev.

## Overview

| Area | Types / entry points |
|------|----------------------|
| MySQL | [`MysqlClient`](mysql.go), [`NewMysqlClient`](mysql.go), [`NewMysqlClientWithLog`](mysql.go), run modes [`DebugMode`](const.go) / [`ReleaseMode`](const.go) |
| Redis | [`RedisClient`](redis.go), [`NewRedisClient`](redis.go), [`NewRedisClientEx`](redis.go), [`RedisClient.Close`](redis.go) |
| Kafka | [`KafkaAsyncSender`](kafka_producer.go), [`KafkaSyncSender`](kafka_producer.go), [`KafkaReceiver`](kafka_consumer.go) |
| Sharding | [`MonthlyShardingByOid`](sharding.go), [`MonthlyShardingByTime`](sharding.go) |
| Metrics | [`MysqlHistogram`](metric.go), [`RedisPoolOpGauge`](metric.go), [`RedisConnStatusGauge`](metric.go) |

## Operational Notes

- MySQL startup uses the caller's context and a five-second maximum. Close the pool with `MysqlClient.Close`. SQL logs keep placeholders for bound arguments. Bind sensitive values as parameters.
- Redis pool gauges aggregate live clients. Closing a client removes its pool from the next sample and updates the gauges immediately.
- `KafkaReceiver.Start` waits for the first consumer-group session and rejects a second concurrent start. That context controls the consume loop. Each handler attempt receives the session context and its own copy of the original message. Cancellation stops retries without marking the interrupted message. A `PermanentError` explicitly acknowledges the message without retrying. The receiver enables and logs Sarama's consumer-group error stream.
- Kafka `Close` begins shutdown once. The supplied context bounds the caller's wait, and a later `Close` can wait for the same shutdown. Callers should let callbacks and receiver handlers return before expecting shutdown to finish.
- Kafka constructors check context before opening connections. Configure Sarama's network timeouts and retry settings to bound broker discovery.
- Async `Send` respects context cancellation while waiting for the input queue. Use `NewKafkaAsyncSenderWithCallback` for delivery notifications. A sync `Send` checks context before dispatch and uses Sarama's network timeouts and retry settings once dispatched.
- Register `MonthlyShardingByOid` or `MonthlyShardingByTime` with `db.Use(...)`. Both return a `*tdb.MonthlySharding` plugin. Prepare each physical table before writing to it. Supply an ID explicitly or define auto-increment on each physical shard. Use explicit IDs when records need globally unique IDs across shards.
- Logical-table operations require a single target month. Bind the sharding key with an equality predicate for reads, updates, and deletes. Insert batches must share a UTC month. Quote identifiers in raw SQL. For joins or other unsupported SQL shapes, address the physical tables explicitly. To deliberately access the original table, use `db.Set("sharding_ignore", true)` for that operation.

## Examples

**MySQL Session**

```go
client, err := tdb.NewMysqlClient(ctx, dsn)
if err != nil {
    return err
}
defer func() { _ = client.Close() }()
database := client.DB(ctx, tdb.ReleaseMode)
```

**Redis Client**

```go
rdb, err := tdb.NewRedisClient(ctx, "127.0.0.1:6379", "", 10)
if err != nil {
    return err
}
defer func() { _ = rdb.Close() }()
_ = rdb.Client() // *redis.Client
```

**Kafka Consumer**

```go
recv, err := tdb.NewKafkaReceiver(ctx, brokers, cfg, "my-group", "my-topic",
    func(ctx context.Context, value []byte) error {
        return nil
    })
if err != nil {
    return err
}
defer func() {
    closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    _ = recv.Close(closeCtx)
}()
if err := recv.Start(ctx); err != nil {
    return err
}
```

## Contributing

Follow standard Go conventions, including `gofmt`, focused commits, and clear exported API documentation. Package comments and exported identifiers should remain documented in **English** to preserve compatibility with `go doc`.
