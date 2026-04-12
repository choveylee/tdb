package tdb

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"gorm.io/sharding"
)

// monthlyShardingAlgorithmByOid derives a table suffix from a MongoDB ObjectID hex string using its embedded timestamp (YYYYMM in local time).
func monthlyShardingAlgorithmByOid(value interface{}) (suffix string, err error) {
	srcObjectId, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("sharding key must be string")
	}

	objectId, err := bson.ObjectIDFromHex(srcObjectId)
	if err != nil {
		return "", fmt.Errorf("invalid object id: %s, %w", srcObjectId, err)
	}

	return "_" + objectId.Timestamp().In(time.Local).Format("200601"), nil
}

// monthlyShardingAlgorithmByTime formats the sharding key time.Time into a YYYYMM table suffix in local time.
func monthlyShardingAlgorithmByTime(value interface{}) (suffix string, err error) {
	curTime, ok := value.(time.Time)
	if !ok {
		return "", fmt.Errorf("sharding key must be time.Time")
	}

	return "_" + curTime.In(time.Local).Format("200601"), nil
}

// MonthlyShardingByOid registers month-based sharding for tables; the sharding key must be an ObjectID hex string.
func MonthlyShardingByOid(shardingKey string, tables []string) *sharding.Sharding {
	return sharding.Register(sharding.Config{
		ShardingKey:         shardingKey,
		ShardingAlgorithm:   monthlyShardingAlgorithmByOid,
		PrimaryKeyGenerator: sharding.PKMySQLSequence,
	}, tables)
}

// MonthlyShardingByTime registers month-based sharding for tables; the sharding key must be time.Time.
func MonthlyShardingByTime(shardingKey string, tables []string) *sharding.Sharding {
	return sharding.Register(sharding.Config{
		ShardingKey:         shardingKey,
		ShardingAlgorithm:   monthlyShardingAlgorithmByTime,
		PrimaryKeyGenerator: sharding.PKMySQLSequence,
	}, tables)
}

// listTables returns all table names in the current database, sorted lexicographically.
func (p *MysqlClient) listTables(ctx context.Context) ([]string, error) {
	tables := make([]string, 0)

	retGorm := p.db.WithContext(ctx).Raw(`SHOW TABLES`).Scan(&tables)
	if retGorm.Error != nil {
		return nil, fmt.Errorf("show tables: %w", retGorm.Error)
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i] < tables[j]
	})

	return tables, nil
}
