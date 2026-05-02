# tdb

`tdb` is a Go library that provides reusable infrastructure components for backend services. It includes **MySQL** access built on GORM with OpenTelemetry integration and SQL latency metrics, **Redis** clients with connection-pool metrics, **Kafka** producers and consumer groups built on Sarama, optional **monthly table sharding** for GORM models, and **Prometheus-style** metrics via `tmetric`.

## Requirements

- Go **1.25** or newer (see `go.mod`).

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

- `KafkaReceiver.Start` blocks until the first consumer-group session is established. If startup fails before the initial session is ready, the method returns the corresponding error instead of blocking indefinitely.
- `KafkaReceiver` passes the consumer-session context to message handlers so application code can stop promptly during shutdown or rebalance.
- `MonthlyShardingByOid` and `MonthlyShardingByTime` derive monthly suffixes in `UTC`, which keeps shard selection deterministic across deployment time zones.
- `NewKafkaAsyncSender` disables Sarama success and error result channels internally. Use `NewKafkaAsyncSenderWithCallback` when callback-based delivery notifications are required.

## Examples

**MySQL Session**

```go
client, err := tdb.NewMysqlClient(ctx, dsn)
if err != nil {
    return err
}
db := client.DB(ctx, tdb.ReleaseMode)
// Execute application queries with db.
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
if err := recv.Start(ctx); err != nil {
    return err
}
defer func() { _ = recv.Close(ctx) }()
```

## Contributing

Follow standard Go conventions, including `gofmt`, focused commits, and clear exported API documentation. Package comments and exported identifiers should remain documented in **English** to preserve compatibility with `go doc`.
