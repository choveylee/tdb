// Package tdb provides shared infrastructure for services: MySQL via GORM, Redis, Kafka
// producers and consumers, optional monthly table sharding, and Prometheus-style metrics.
//
// # MySQL
//
// Use [MysqlClient] with GORM. Open a connection with [NewMysqlClient] or [NewMysqlClientWithLog],
// then use [MysqlClient.DB] or [MysqlClient.Tx] with [DebugMode] or [ReleaseMode] to control SQL logging verbosity.
//
// # Redis
//
// [RedisClient] wraps go-redis. [NewRedisClient] uses logical database 0; [NewRedisClientEx] selects an arbitrary DB index.
// Call [RedisClient.Close] to stop the pool metrics goroutine and release connections.
//
// # Kafka
//
// [KafkaAsyncSender] and [KafkaSyncSender] publish JSON-encoded payloads to a fixed topic.
// [KafkaReceiver] joins a consumer group and delivers payloads through [ReceiverHandler].
//
// # Sharding
//
// [MonthlyShardingByOid] and [MonthlyShardingByTime] register month-based sharding with gorm.io/sharding.
//
// # Metrics
//
// SQL latency and Redis pool metrics are registered on [MysqlHistogram], [RedisPoolOpGauge], and [RedisConnStatusGauge];
// see each variable for metric names and label dimensions.
package tdb
