package tdb

import (
	"context"
	"time"

	"github.com/uptrace/opentelemetry-go-extra/otelgorm"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	"github.com/choveylee/tlog"
)

// dbLogger implements [logger.Interface], forwarding GORM log events to tlog and highlighting statements slower than 500ms.
type dbLogger struct {
	LogLevel logger.LogLevel
}

// LogMode returns a new logger.Interface with the given log level.
func (l *dbLogger) LogMode(level logger.LogLevel) logger.Interface {
	newLogger := *l

	newLogger.LogLevel = level

	return &newLogger
}

// Info logs a formatted message when the configured level is at least Info.
func (l *dbLogger) Info(ctx context.Context, msg string, args ...interface{}) {
	if l.LogLevel >= logger.Info {
		tlog.I(ctx).Msgf(msg, args...)
	}
}

// Warn logs a formatted message when the configured level is at least Warn.
func (l *dbLogger) Warn(ctx context.Context, msg string, args ...interface{}) {
	if l.LogLevel >= logger.Warn {
		tlog.W(ctx).Msgf(msg, args...)
	}
}

// Error logs a formatted message when the configured level is at least Error.
func (l *dbLogger) Error(ctx context.Context, msg string, args ...interface{}) {
	if l.LogLevel >= logger.Error {
		tlog.E(ctx).Msgf(msg, args...)
	}
}

// Trace records per-statement latency. At Info level it logs executed SQL; statements slower than 500ms are reported as slow queries.
// The callback fc may be invoked more than once when multiple branches apply.
func (l *dbLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	latency := time.Since(begin)

	if latency > time.Millisecond*500 {
		rawSql, _ := fc()

		tlog.I(ctx).Msgf("Detected slow SQL statement: sql=%s latency=%s", rawSql, latency)
	}

	if l.LogLevel == logger.Info {
		rawSql, _ := fc()

		tlog.D(ctx).Msgf("Executed SQL statement: sql=%s latency=%s", rawSql, latency)
	}
}

var _ logger.Interface = &dbLogger{}

// beforeMetricHook stores the statement start time for use by afterMetricHook.
func beforeMetricHook(db *gorm.DB) {
	db.Set("metric_start_time", time.Now())
}

// afterMetricHook observes elapsed time into [MysqlHistogram] after each statement.
func afterMetricHook(db *gorm.DB) {
	if db.Statement.Schema == nil || len(db.Statement.BuildClauses) == 0 {
		return
	}

	sqlStatus := "SUCCESS"
	if db.Statement.Error != nil {
		sqlStatus = "FAILED"
	}

	srcStartTime, ok := db.Get("metric_start_time")
	if !ok {
		return
	}

	startTime, ok := srcStartTime.(time.Time)
	if !ok {
		return
	}

	MysqlHistogram.Observe(
		float64(time.Since(startTime).Milliseconds()),
		db.Statement.Schema.Table,
		db.Statement.BuildClauses[0],
		sqlStatus,
	)
}

// openDB opens a GORM DB from dsn, registers the OpenTelemetry plugin, and wires metric hooks on CRUD callbacks.
func openDB(ctx context.Context, dsn string, logLevel logger.LogLevel) (*gorm.DB, error) {
	dialector := mysql.Open(dsn)

	otelPlugin := otelgorm.NewPlugin(
		otelgorm.WithDBName(dialector.(*mysql.Dialector).DSNConfig.DBName),
		otelgorm.WithoutQueryVariables(),
		otelgorm.WithoutMetrics(),
	)

	gormDB, err := gorm.Open(dialector, &gorm.Config{
		Logger: &dbLogger{
			LogLevel: logLevel,
		},
		NamingStrategy: schema.NamingStrategy{
			SingularTable: false,
		},
		Plugins: map[string]gorm.Plugin{
			otelPlugin.Name(): otelPlugin,
		},
	})
	if err != nil {
		return nil, err
	}

	_ = gormDB.Callback().Query().Before("gorm:query").Register("before_query_hook", beforeMetricHook)
	_ = gormDB.Callback().Create().Before("gorm:create").Register("before_create_hook", beforeMetricHook)
	_ = gormDB.Callback().Update().Before("gorm:update").Register("before_update_hook", beforeMetricHook)
	_ = gormDB.Callback().Delete().Before("gorm:delete").Register("before_delete_hook", beforeMetricHook)
	_ = gormDB.Callback().Query().After("gorm:query").Register("after_query_hook", afterMetricHook)
	_ = gormDB.Callback().Create().After("gorm:create").Register("after_create_hook", afterMetricHook)
	_ = gormDB.Callback().Update().After("gorm:update").Register("after_update_hook", afterMetricHook)
	_ = gormDB.Callback().Delete().After("gorm:delete").Register("after_delete_hook", afterMetricHook)

	return gormDB, nil
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
	gormDb, err := openDB(ctx, dsn, logger.Info)
	if err != nil {
		return nil, err
	}

	mysqlClient := &MysqlClient{
		db: gormDb,
	}

	return mysqlClient, nil
}

// DB returns a context-bound [gorm.DB]. When runMode is [DebugMode], the returned session has GORM Debug logging enabled.
func (p *MysqlClient) DB(ctx context.Context, runMode string) *gorm.DB {
	if runMode == DebugMode {
		return p.db.WithContext(ctx).Debug()
	}

	return p.db.WithContext(ctx)
}

// Tx begins a transaction and returns a [gorm.DB]. The caller must Commit or Rollback the returned session.
// When runMode is [DebugMode], the transaction is created with GORM Debug logging enabled.
func (p *MysqlClient) Tx(ctx context.Context, runMode string) *gorm.DB {
	if runMode == DebugMode {
		return p.db.WithContext(ctx).Debug().Begin()
	}

	return p.db.WithContext(ctx).Begin()
}

// SetMaxOpenConns sets the maximum number of open connections on the underlying sql.DB.
func (p *MysqlClient) SetMaxOpenConns(maxOpenConns int) error {
	sqlDB, err := p.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetMaxOpenConns(maxOpenConns)

	return nil
}

// SetMaxIdleConns sets the maximum number of idle connections on the underlying sql.DB.
func (p *MysqlClient) SetMaxIdleConns(maxIdleConns int) error {
	sqlDB, err := p.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetMaxIdleConns(maxIdleConns)

	return nil
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
func (p *MysqlClient) SetConnMaxLifetime(connMaxLifetime time.Duration) error {
	sqlDB, err := p.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetConnMaxLifetime(connMaxLifetime)

	return nil
}

// SetConnMaxIdleTime sets how long an idle connection may remain in the pool.
func (p *MysqlClient) SetConnMaxIdleTime(connMaxIdleTime time.Duration) error {
	sqlDB, err := p.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetConnMaxIdleTime(connMaxIdleTime)

	return nil
}
