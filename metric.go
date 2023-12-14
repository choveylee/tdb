/**
 * @Author: lidonglin
 * @Description:
 * @File:  metric.go
 * @Version: 1.0.0
 * @Date: 2023/12/13 18:06
 */

package tdb

import (
	"github.com/choveylee/tmetric"
)

var (
	MysqlHistogram, _ = tmetric.NewHistogramVec(
		"mysql_latency",
		"histogram of sql latency (milliseconds)",
		[]string{"sql_table", "sql_operation", "sql_status"},
	)
)

var (
	RedisPoolOpGauge, _ = tmetric.NewGaugeVec(
		"redis_pool_op",
		"counter of redis pool op",
		[]string{"redis_pool_op"},
	)

	RedisConnStatusGauge, _ = tmetric.NewGaugeVec(
		"redis_conn_status",
		"counter of redis conn status",
		[]string{"redis_conn_status"},
	)
)
