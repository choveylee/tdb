package tdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"gorm.io/gorm"
)

const shardingIgnoreKey = "sharding_ignore"

// MonthlySharding is a GORM plugin that routes configured tables by UTC month.
type MonthlySharding struct {
	key       string
	tablesMap map[string]struct{}
	suffix    func(any) (string, error)
	lastQuery atomic.Pointer[string]
}

// MonthlyShardingByOid routes each configured table by the UTC month in its ObjectID.
// Supply the primary key in the insert or configure auto-increment on the shard.
func MonthlyShardingByOid(shardingKey string, tables []string) *MonthlySharding {
	return registerMonthlySharding(shardingKey, tables, monthlyShardingAlgorithmByOid)
}

// MonthlyShardingByTime routes each configured table by the UTC month of its time key.
// Times that could round into another month at second precision are rejected.
// Supply the primary key in the insert or configure auto-increment on the shard.
func MonthlyShardingByTime(shardingKey string, tables []string) *MonthlySharding {
	return registerMonthlySharding(shardingKey, tables, monthlyShardingAlgorithmByTime)
}

// Name returns the GORM plugin name.
func (plugin *MonthlySharding) Name() string {
	return "gorm:sharding"
}

// Initialize wraps only the callbacks that execute SQL. The GORM callbacks that
// open and finish transactions retain the original pool or transaction.
func (plugin *MonthlySharding) Initialize(database *gorm.DB) error {
	if len(plugin.tablesMap) == 0 {
		return nil
	}

	callbacks := database.Callback()
	operations := []struct {
		name    string
		get     func(string) func(*gorm.DB)
		replace func(string, func(*gorm.DB)) error
	}{
		{"gorm:create", callbacks.Create().Get, callbacks.Create().Replace},
		{"gorm:query", callbacks.Query().Get, callbacks.Query().Replace},
		{"gorm:update", callbacks.Update().Get, callbacks.Update().Replace},
		{"gorm:delete", callbacks.Delete().Get, callbacks.Delete().Replace},
		{"gorm:row", callbacks.Row().Get, callbacks.Row().Replace},
		{"gorm:raw", callbacks.Raw().Get, callbacks.Raw().Replace},
	}

	for _, operation := range operations {
		execute := operation.get(operation.name)
		if execute == nil {
			continue
		}

		err := operation.replace(operation.name, func(statementDB *gorm.DB) {
			if _, ignored := statementDB.Get(shardingIgnoreKey); ignored {
				execute(statementDB)

				return
			}

			statement := statementDB.Statement
			originalPool := statement.ConnPool
			pool := &monthlyShardingPool{ConnPool: originalPool, plugin: plugin}

			statement.ConnPool = pool
			defer func() {
				statement.ConnPool = originalPool
			}()

			execute(statementDB)
		})
		if err != nil {
			return fmt.Errorf("tdb: attach monthly sharding to %s: %w", operation.name, err)
		}
	}

	return nil
}

// LastQuery returns the most recent SQL submitted through the router, with its bind placeholders.
func (plugin *MonthlySharding) LastQuery() string {
	if query := plugin.lastQuery.Load(); query != nil {
		return *query
	}

	return ""
}

func registerMonthlySharding(shardingKey string, tables []string, algorithm func(any) (string, error)) *MonthlySharding {
	tablesMap := make(map[string]struct{}, len(tables))
	for _, tableName := range tables {
		tablesMap[strings.ToLower(tableName)] = struct{}{}
	}

	plugin := &MonthlySharding{key: shardingKey, tablesMap: tablesMap, suffix: algorithm}

	return plugin
}

// monthlyShardingAlgorithmByOid derives the UTC month from an ObjectID hex string.
func monthlyShardingAlgorithmByOid(value any) (string, error) {
	objectIdText, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("tdb: ObjectID sharding key must be a hex string, got %T", value)
	}

	objectId, err := bson.ObjectIDFromHex(objectIdText)
	if err != nil {
		return "", fmt.Errorf("tdb: decode sharding ObjectID: %w", err)
	}

	return "_" + objectId.Timestamp().UTC().Format("200601"), nil
}

// monthlyShardingAlgorithmByTime derives the UTC month from a bound time value.
func monthlyShardingAlgorithmByTime(value any) (string, error) {
	shardingTime, ok := value.(time.Time)
	if !ok {
		return "", fmt.Errorf("tdb: time sharding key must be a time.Time, got %T", value)
	}

	utcTime := shardingTime.UTC()
	roundedTime := utcTime.Round(time.Second)
	// DATETIME without fractional precision can round up into another month.
	// Reject that boundary when the column precision and SQL mode are unknown.
	if utcTime.Year() != roundedTime.Year() || utcTime.Month() != roundedTime.Month() {
		return "", errors.New("tdb: time sharding key must not round into another month at second precision")
	}

	return "_" + utcTime.Format("200601"), nil
}

// monthlyShardingPool rewrites SQL on the active GORM connection or transaction.
// GORM remains the only SQL logger.
type monthlyShardingPool struct {
	gorm.ConnPool
	plugin *MonthlySharding
}

func (pool *monthlyShardingPool) ExecContext(ctx context.Context, query string, arguments ...any) (sql.Result, error) {
	routedQuery, err := pool.plugin.route(query, arguments)
	if err != nil {
		return nil, err
	}

	return pool.ConnPool.ExecContext(ctx, routedQuery, arguments...)
}

func (pool *monthlyShardingPool) QueryContext(ctx context.Context, query string, arguments ...any) (*sql.Rows, error) {
	routedQuery, err := pool.plugin.route(query, arguments)
	if err != nil {
		return nil, err
	}

	return pool.ConnPool.QueryContext(ctx, routedQuery, arguments...)
}

// QueryRowContext returns routing errors through Row.Err or Row.Scan. Its
// signature requires a *sql.Row even when the statement cannot be sent.
func (pool *monthlyShardingPool) QueryRowContext(ctx context.Context, query string, arguments ...any) *sql.Row {
	routedQuery, err := pool.plugin.route(query, arguments)
	if err != nil {
		connector := shardingErrorConnector{err: err}

		errorDB := sql.OpenDB(connector)
		defer errorDB.Close()

		return errorDB.QueryRowContext(ctx, "")
	}

	return pool.ConnPool.QueryRowContext(ctx, routedQuery, arguments...)
}

// shardingErrorConnector lets database/sql construct a Row with a routing error
// without opening a MySQL connection or executing SQL.
type shardingErrorConnector struct {
	err error
}

func (connector shardingErrorConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, connector.err
}

func (connector shardingErrorConnector) Driver() driver.Driver {
	return connector
}

func (connector shardingErrorConnector) Open(string) (driver.Conn, error) {
	return nil, connector.err
}

// listTables returns all table names in the current database in lexicographic order.
func (client *MysqlClient) listTables(ctx context.Context) ([]string, error) {
	tables := make([]string, 0)

	database := client.db.WithContext(ctx).Raw(`SHOW TABLES`).Scan(&tables)
	if database.Error != nil {
		return nil, fmt.Errorf("tdb: list database tables: %w", database.Error)
	}

	slices.Sort(tables)

	return tables, nil
}
