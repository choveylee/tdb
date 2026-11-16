package tdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/choveylee/tlog"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/uptrace/opentelemetry-go-extra/otelgorm"
	gormMysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const mysqlMetricStartKey = "tdb:mysql:metric_start"

// dbLogger forwards GORM logs without interpolating bound SQL parameters.
type dbLogger struct {
	logLevel logger.LogLevel
}

var _ logger.Interface = (*dbLogger)(nil)
var _ gorm.ParamsFilter = (*dbLogger)(nil)

// LogMode returns a logger copy with the requested level.
func (databaseLogger *dbLogger) LogMode(level logger.LogLevel) logger.Interface {
	configuredLogger := *databaseLogger
	configuredLogger.logLevel = level

	return &configuredLogger
}

func (databaseLogger *dbLogger) Info(ctx context.Context, message string, arguments ...any) {
	if databaseLogger.logLevel >= logger.Info {
		tlog.I(ctx).Msgf(message, arguments...)
	}
}

func (databaseLogger *dbLogger) Warn(ctx context.Context, message string, arguments ...any) {
	if databaseLogger.logLevel >= logger.Warn {
		tlog.W(ctx).Msgf(message, arguments...)
	}
}

func (databaseLogger *dbLogger) Error(ctx context.Context, message string, arguments ...any) {
	if databaseLogger.logLevel >= logger.Error {
		tlog.E(ctx).Msgf(message, arguments...)
	}
}

// ParamsFilter keeps placeholders in statements rendered by the GORM trace hook.
func (databaseLogger *dbLogger) ParamsFilter(_ context.Context, statement string, _ ...any) (string, []any) {
	return statement, nil
}

// Trace emits one log for a statement error, a slow statement, or a debug trace.
// MySQL error messages may contain values, so the log records the error type and number.
func (databaseLogger *dbLogger) Trace(ctx context.Context, startedAt time.Time, renderStatement func() (string, int64), err error) {
	if databaseLogger.logLevel == logger.Silent {
		return
	}

	elapsed := time.Since(startedAt)
	switch {
	case err != nil && !errors.Is(err, gorm.ErrRecordNotFound):
		statement, _ := renderStatement()
		event := tlog.E(ctx).Detailf("error type=%T", err)
		var mysqlErr *mysqlDriver.MySQLError
		if errors.As(err, &mysqlErr) {
			event.Detailf("MySQL error number=%d", mysqlErr.Number)
		}

		event.Msgf("Execute MySQL statement error (sql=%s, elapsed=%s)", statement, elapsed)
	case databaseLogger.logLevel >= logger.Warn && elapsed > 500*time.Millisecond:
		statement, _ := renderStatement()
		tlog.W(ctx).Msgf("Execute slow MySQL statement (sql=%s, elapsed=%s)", statement, elapsed)
	case databaseLogger.logLevel >= logger.Info:
		statement, _ := renderStatement()
		tlog.D(ctx).Msgf("Execute MySQL statement (sql=%s, elapsed=%s)", statement, elapsed)
	}
}

// mysqlStartupPool binds GORM's server-version probe to the constructor's context.
type mysqlStartupPool struct {
	*sql.DB
	ctx context.Context
}

func (pool *mysqlStartupPool) QueryRowContext(_ context.Context, query string, arguments ...any) *sql.Row {
	return pool.DB.QueryRowContext(pool.ctx, query, arguments...)
}

// openDB validates the DSN and applies the startup deadline without changing the
// connection settings or context of later database sessions.
func openDB(ctx context.Context, dsn string, logLevel logger.LogLevel) (*gorm.DB, error) {
	if ctx == nil {
		return nil, errors.New("tdb: context must not be nil")
	}

	dsnConfig, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("tdb: parse MySQL DSN: %w", err)
	}

	connector, err := mysqlDriver.NewConnector(dsnConfig)
	if err != nil {
		return nil, fmt.Errorf("tdb: create MySQL connector: %w", err)
	}

	startupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	sqlDB := sql.OpenDB(connector)
	opened := false
	defer func() {
		if !opened {
			_ = sqlDB.Close()
		}
	}()

	if err := sqlDB.PingContext(startupCtx); err != nil {
		return nil, fmt.Errorf("tdb: ping MySQL: %w", err)
	}

	startupPool := &mysqlStartupPool{DB: sqlDB, ctx: startupCtx}
	dialectConfig := gormMysql.Config{DSNConfig: dsnConfig, Conn: startupPool}
	dialector := gormMysql.New(dialectConfig)
	otelPlugin := otelgorm.NewPlugin(
		otelgorm.WithDBName(dsnConfig.DBName),
		otelgorm.WithoutQueryVariables(),
		otelgorm.WithoutMetrics(),
	)
	databaseLogger := &dbLogger{logLevel: logLevel}
	pluginsMap := map[string]gorm.Plugin{otelPlugin.Name(): otelPlugin}
	databaseConfig := &gorm.Config{
		Logger:               databaseLogger,
		Plugins:              pluginsMap,
		DisableAutomaticPing: true,
	}

	database, err := gorm.Open(dialector, databaseConfig)
	if err != nil {
		return nil, fmt.Errorf("tdb: initialize MySQL database: %w", err)
	}

	// The version probe is complete. Later statements must use their own contexts.
	dialector.(*gormMysql.Dialector).Conn = sqlDB
	database.ConnPool = sqlDB
	database.Statement.ConnPool = sqlDB

	callbacks := database.Callback()
	for _, operation := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
		// Use the operation selected at registration. Hooks can clear BuildClauses
		// before observation, and transaction commit errors arrive after execution.
		before := func(statementDB *gorm.DB) {
			statementDB.Statement.Settings.Store(mysqlMetricStartKey, time.Now())
		}
		after := func(statementDB *gorm.DB) {
			rawStart, ok := statementDB.Statement.Settings.LoadAndDelete(mysqlMetricStartKey)
			startedAt, valid := rawStart.(time.Time)
			if !ok || !valid || statementDB.Statement.Schema == nil || MysqlHistogram == nil {
				return
			}

			status := "SUCCESS"
			if statementDB.Error != nil {
				status = "FAILED"
			}

			if err := MysqlHistogram.Observe(float64(time.Since(startedAt).Milliseconds()), statementDB.Statement.Schema.Table, operation, status); err != nil {
				tlog.W(statementDB.Statement.Context).Err(err).Msg("Record MySQL latency error")
			}
		}

		var registerBefore, registerAfter func(string, func(*gorm.DB)) error
		switch operation {
		case "SELECT":
			registerBefore = callbacks.Query().Before("*").Register
			registerAfter = callbacks.Query().After("*").Register
		case "INSERT":
			registerBefore = callbacks.Create().Before("*").Register
			registerAfter = callbacks.Create().After("*").Register
		case "UPDATE":
			registerBefore = callbacks.Update().Before("*").Register
			registerAfter = callbacks.Update().After("*").Register
		case "DELETE":
			registerBefore = callbacks.Delete().Before("*").Register
			registerAfter = callbacks.Delete().After("*").Register
		}

		err = errors.Join(registerBefore("tdb:before_metric", before), registerAfter("tdb:after_metric", after))
		if err != nil {
			return nil, fmt.Errorf("tdb: register MySQL metrics for %s: %w", operation, err)
		}
	}

	opened = true

	return database, nil
}

// MysqlClient wraps a GORM-backed MySQL connection together with SQL latency instrumentation.
type MysqlClient struct {
	db *gorm.DB
}

// NewMysqlClient returns a client configured with GORM log level Error, suitable for production workloads that prefer lower log volume.
func NewMysqlClient(ctx context.Context, dsn string) (*MysqlClient, error) {
	db, err := openDB(ctx, dsn, logger.Error)
	if err != nil {
		return nil, err
	}

	mysqlClient := &MysqlClient{
		db: db,
	}

	return mysqlClient, nil
}

// NewMysqlClientWithLog returns a client configured with GORM log level Info for detailed SQL tracing.
func NewMysqlClientWithLog(ctx context.Context, dsn string) (*MysqlClient, error) {
	gormDB, err := openDB(ctx, dsn, logger.Info)
	if err != nil {
		return nil, err
	}

	mysqlClient := &MysqlClient{
		db: gormDB,
	}

	return mysqlClient, nil
}

// DB returns a context-bound [gorm.DB]. When runMode is [DebugMode], the returned session has GORM Debug logging enabled.
func (client *MysqlClient) DB(ctx context.Context, runMode string) *gorm.DB {
	if runMode == DebugMode {
		return client.db.WithContext(ctx).Debug()
	}

	return client.db.WithContext(ctx)
}

// Tx begins a transaction and returns a [gorm.DB]. The caller must Commit or Rollback the returned session.
// When runMode is [DebugMode], the transaction is created with GORM Debug logging enabled.
func (client *MysqlClient) Tx(ctx context.Context, runMode string) *gorm.DB {
	return client.DB(ctx, runMode).Begin()
}

// SetMaxOpenConns sets the maximum number of open connections on the underlying sql.DB.
func (client *MysqlClient) SetMaxOpenConns(maxOpenConns int) error {
	sqlDB, err := client.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetMaxOpenConns(maxOpenConns)

	return nil
}

// SetMaxIdleConns sets the maximum number of idle connections on the underlying sql.DB.
func (client *MysqlClient) SetMaxIdleConns(maxIdleConns int) error {
	sqlDB, err := client.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetMaxIdleConns(maxIdleConns)

	return nil
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
func (client *MysqlClient) SetConnMaxLifetime(connMaxLifetime time.Duration) error {
	sqlDB, err := client.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetConnMaxLifetime(connMaxLifetime)

	return nil
}

// SetConnMaxIdleTime sets how long an idle connection may remain in the pool.
func (client *MysqlClient) SetConnMaxIdleTime(connMaxIdleTime time.Duration) error {
	sqlDB, err := client.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetConnMaxIdleTime(connMaxIdleTime)

	return nil
}

// Close releases the database connection pool owned by the client.
func (client *MysqlClient) Close() error {
	if client == nil || client.db == nil {
		return nil
	}

	sqlDB, err := client.db.DB()
	if err != nil {
		return fmt.Errorf("tdb: get MySQL connection pool: %w", err)
	}

	if err := sqlDB.Close(); err != nil {
		return fmt.Errorf("tdb: close MySQL connection pool: %w", err)
	}

	return nil
}
