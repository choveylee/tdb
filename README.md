# tdb

`tdb` is a Go library that bundles common service infrastructure: **MySQL** (GORM with OpenTelemetry and SQL latency metrics), **Redis** (go-redis with pool metrics), **Kafka** (Sarama async/sync producers and consumer groups with backoff), optional **monthly table sharding** (gorm.io/sharding with MongoDB ObjectID or `time.Time` keys), and **Prometheus-style** metrics via `tmetric`.

## Requirements

- Go **1.25** or newer (see `go.mod`).

## Installation

```bash
go get github.com/choveylee/tdb
```

## Documentation

Browse package documentation locally:

```bash
go doc -all github.com/choveylee/tdb
```

The [`doc.go`](doc.go) file contains the top-level package overview with linked symbols for `go doc` / pkg.go.dev.

## Overview

| Area | Types / entry points |
|------|----------------------|
| MySQL | [`MysqlClient`](mysql.go), [`NewMysqlClient`](mysql.go), [`NewMysqlClientWithLog`](mysql.go), run modes [`DebugMode`](const.go) / [`ReleaseMode`](const.go) |
| Redis | [`RedisClient`](redis.go), [`NewRedisClient`](redis.go), [`NewRedisClientE`](redis.go), [`RedisClient.Close`](redis.go) |
| Kafka | [`KafkaAsyncSender`](kafka_producer.go), [`KafkaSyncSender`](kafka_producer.go), [`KafkaReceiver`](kafka_consumer.go) |
| Sharding | [`MonthlyShardingByOid`](sharding.go), [`MonthlyShardingByTime`](sharding.go) |
| Metrics | [`MysqlHistogram`](metric.go), [`RedisPoolOpGauge`](metric.go), [`RedisConnStatusGauge`](metric.go) |

## Examples

**MySQL (GORM session)**

```go
client, err := tdb.NewMysqlClient(ctx, dsn)
if err != nil {
    return err
}
db := client.DB(ctx, tdb.ReleaseMode)
// use db for queries ...
```

**Redis**

```go
rdb, err := tdb.NewRedisClient(ctx, "127.0.0.1:6379", "", 10)
if err != nil {
    return err
}
defer func() { _ = rdb.Close() }()
_ = rdb.Client() // *redis.Client
```

**Kafka consumer**

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
defer recv.Close(ctx)
```

## Contributing

Follow standard Go style (`gofmt`, meaningful commit messages). Package comments and exported APIs should remain documented in **English** for `go doc` compatibility.
