package tdb

import (
	"github.com/choveylee/tmetric"
)

var (
	// MysqlHistogram records SQL statement latency in milliseconds, labeled by table name, primary clause, and execution outcome.
	MysqlHistogram, _ = tmetric.NewHistogramVec(
		"mysql_latency",
		"SQL statement latency in milliseconds, labeled by table, primary clause, and outcome.",
		[]string{"sql_table", "sql_operation", "sql_status"},
	)
)

var (
	// RedisPoolOpGauge reports Redis connection-pool counters, including hits, misses, timeouts, and stale connections.
	RedisPoolOpGauge, _ = tmetric.NewGaugeVec(
		"redis_pool_op",
		"Redis client pool operation counters (hits, misses, timeouts, stale connections).",
		[]string{"redis_pool_op"},
	)

	// RedisConnStatusGauge reports the number of idle and active Redis connections.
	RedisConnStatusGauge, _ = tmetric.NewGaugeVec(
		"redis_conn_status",
		"Redis connection counts by state (idle versus active).",
		[]string{"redis_conn_status"},
	)
)
