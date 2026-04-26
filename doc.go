// Package tdb provides reusable infrastructure components for Go services, including MySQL access via GORM,
// Redis clients, Kafka producers and consumers, optional monthly table sharding, and Prometheus-style metrics.
//
// # MySQL
//
// Use [MysqlClient] to manage GORM-backed MySQL access. Initialize a client with [NewMysqlClient]
// or [NewMysqlClientWithLog], then obtain a request-scoped session with [MysqlClient.DB] or
// [MysqlClient.Tx]. Use [DebugMode] or [ReleaseMode] to control SQL logging verbosity.
//
// # Redis
//
// [RedisClient] wraps go-redis and periodically exports pool metrics. [NewRedisClient] connects
// to logical database 0, while [NewRedisClientEx] allows an explicit logical database index.
// Call [RedisClient.Close] to stop the background reporter and release the underlying connections.
//
// # Kafka
//
// [KafkaAsyncSender] and [KafkaSyncSender] publish JSON-encoded payloads to a fixed topic.
// [KafkaReceiver] joins a consumer group, waits for the initial session to be established, and
// delivers payloads through [ReceiverHandler]. Message handlers receive the consumer-session context
// so they can react promptly to shutdown and rebalance signals.
//
// # Sharding
//
// [MonthlyShardingByOid] and [MonthlyShardingByTime] register UTC-based monthly sharding rules with
// gorm.io/sharding, ensuring consistent routing across deployment time zones.
//
// # Metrics
//
// SQL latency and Redis pool metrics are registered on [MysqlHistogram], [RedisPoolOpGauge], and
// [RedisConnStatusGauge]. Refer to each variable for metric names and label dimensions.
package tdb
