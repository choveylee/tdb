/**
 * @Author: lidonglin
 * @Description:
 * @File:  sharding.go
 * @Version: 1.0.0
 * @Date: 2025/6/9 07:07:25
 */

package tdb

import (
	`context`
	`fmt`
	`sort`
	`time`

	"go.mongodb.org/mongo-driver/bson/primitive"
	"gorm.io/sharding"
)

func monthlyShardingAlgorithmByOid(value interface{}) (suffix string, err error) {
	srcObjectId, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("sharding key must be string")
	}

	objectId, err := primitive.ObjectIDFromHex(srcObjectId)
	if err != nil {
		return "", fmt.Errorf("invalid object id: %s, %w", srcObjectId, err)
	}

	return "_" + objectId.Timestamp().In(time.Local).Format("200601"), nil
}

func monthlyShardingAlgorithmByTime(value interface{}) (suffix string, err error) {
	curTime, ok := value.(time.Time)
	if !ok {
		return "", fmt.Errorf("sharding key must be time.Time")
	}

	return "_" + curTime.In(time.Local).Format("200601"), nil
}

func MonthlyShardingByOid(shardingKey string, tables []string) *sharding.Sharding {
	return sharding.Register(sharding.Config{
		ShardingKey:         shardingKey,
		ShardingAlgorithm:   monthlyShardingAlgorithmByOid,
		PrimaryKeyGenerator: sharding.PKMySQLSequence,
	}, tables)
}

func MonthlyShardingByTime(shardingKey string, tables []string) *sharding.Sharding {
	return sharding.Register(sharding.Config{
		ShardingKey:         shardingKey,
		ShardingAlgorithm:   monthlyShardingAlgorithmByTime,
		PrimaryKeyGenerator: sharding.PKMySQLSequence,
	}, tables)
}

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
