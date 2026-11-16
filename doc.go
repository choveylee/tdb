// Package tdb provides reusable infrastructure components for Go services, including MySQL access via GORM,
// Redis clients, Kafka producers and consumers, optional monthly table sharding, and Prometheus-style metrics.
//
// # MySQL
//
// Use [MysqlClient] to manage GORM-backed MySQL access. Initialize a client with [NewMysqlClient]
// or [NewMysqlClientWithLog], then obtain a request-scoped session with [MysqlClient.DB] or
// [MysqlClient.Tx]. Use [DebugMode] or [ReleaseMode] to control SQL logging verbosity.
// Call [MysqlClient.Close] to release the connection pool.
//
// # Redis
//
// [RedisClient] wraps go-redis and contributes to aggregate metrics for live pools. [NewRedisClient] connects
// to logical database 0, while [NewRedisClientEx] allows an explicit logical database index.
// Call [RedisClient.Close] to stop the background reporter and release the underlying connections.
//
// # Kafka
//
// [KafkaAsyncSender] and [KafkaSyncSender] publish JSON-encoded payloads to fixed topics.
// [KafkaReceiver] joins a consumer group, waits for the initial session to be established, and
// delivers payloads through [ReceiverHandler]. Message handlers receive the consumer-session context
// so they can react promptly to shutdown and rebalance signals. Kafka Close methods honor the
// caller's wait deadline and allow later callers to wait for the same shutdown.
//
// # Sharding
//
// Register [MonthlyShardingByOid] or [MonthlyShardingByTime] with gorm.DB.Use
// to route a logical table to one UTC month. Create the physical shards in
// advance and supply a primary key or configure auto-increment on each shard.
// Address physical tables directly for joins or other SQL shapes that need them.
//
// # Metrics
//
// SQL latency and Redis pool metrics are registered on [MysqlHistogram], [RedisPoolOpGauge], and
// [RedisConnStatusGauge]. Refer to each variable for metric names and label dimensions.
package tdb
