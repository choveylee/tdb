package tdb

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"gorm.io/sharding"
)

// monthlyShardingAlgorithmByOid derives a table suffix from a MongoDB ObjectID hex string using its embedded timestamp (YYYYMM in UTC).
func monthlyShardingAlgorithmByOid(value interface{}) (suffix string, err error) {
	srcObjectId, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("invalid sharding key type %T: expected string containing MongoDB ObjectID hex value", value)
	}

	objectId, err := bson.ObjectIDFromHex(srcObjectId)
	if err != nil {
		return "", fmt.Errorf("invalid sharding key value %q: expected MongoDB ObjectID hex string: %w", srcObjectId, err)
	}

	return "_" + objectId.Timestamp().UTC().Format("200601"), nil
}

// monthlyShardingAlgorithmByTime formats the sharding key time.Time into a YYYYMM table suffix in UTC.
func monthlyShardingAlgorithmByTime(value interface{}) (suffix string, err error) {
	curTime, ok := value.(time.Time)
	if !ok {
		return "", fmt.Errorf("invalid sharding key type %T: expected time.Time", value)
	}

	return "_" + curTime.UTC().Format("200601"), nil
}

// MonthlyShardingByOid registers UTC-based monthly sharding for the provided tables; the sharding key must be a MongoDB ObjectID hex string.
func MonthlyShardingByOid(shardingKey string, tables []string) *sharding.Sharding {
	return sharding.Register(sharding.Config{
		ShardingKey:         shardingKey,
		ShardingAlgorithm:   monthlyShardingAlgorithmByOid,
		PrimaryKeyGenerator: sharding.PKMySQLSequence,
	}, tables)
}

// MonthlyShardingByTime registers UTC-based monthly sharding for the provided tables; the sharding key must be a [time.Time] value.
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
		return nil, fmt.Errorf("list database tables: %w", retGorm.Error)
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i] < tables[j]
	})

	return tables, nil
}
