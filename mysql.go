/**
 * @Author: lidonglin
 * @Description:
 * @File:  mysql
 * @Version: 1.0.0
 * @Date: 2023/11/15 10:23
 */

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

type dbLogger struct {
	LogLevel logger.LogLevel
}

func (l *dbLogger) LogMode(level logger.LogLevel) logger.Interface {
	newLogger := *l

	newLogger.LogLevel = level

	return &newLogger
}

func (l *dbLogger) Info(ctx context.Context, msg string, args ...interface{}) {
	if l.LogLevel >= logger.Info {
		tlog.I(ctx).Msgf(msg, args...)
	}
}

func (l *dbLogger) Warn(ctx context.Context, msg string, args ...interface{}) {
	if l.LogLevel >= logger.Warn {
		tlog.W(ctx).Msgf(msg, args...)
	}
}

func (l *dbLogger) Error(ctx context.Context, msg string, args ...interface{}) {
	if l.LogLevel >= logger.Error {
		tlog.E(ctx).Msgf(msg, args...)
	}
}

func (l *dbLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	latency := time.Since(begin)

	if latency > time.Millisecond*500 {
		rawSql, _ := fc()

		tlog.I(ctx).Msgf("slow query sql: %s, latency: %s", rawSql, latency)
	}

	if l.LogLevel == logger.Info {
		rawSql, _ := fc()

		tlog.D(ctx).Msgf("raw sql: %s, latency: %s", rawSql, latency)
	}
}

var _ logger.Interface = &dbLogger{}

func beforeMetricHook(db *gorm.DB) {
	db.Set("metric_start_time", time.Now())
}

func afterMetricHook(db *gorm.DB) {
	if db.Statement.Schema == nil || len(db.Statement.BuildClauses) == 0 {
		return
	}

	sqlStatus := "SUCCESS"
	if db.Statement.Error != nil {
		sqlStatus = "FAILED"
	}

	if start, ok := db.Get("metric_start_time"); ok {
		MysqlHistogram.Observe(
			float64(time.Since(start.(time.Time)).Milliseconds()),
			db.Statement.Schema.Table,
			db.Statement.BuildClauses[0],
			sqlStatus,
		)
	}
}

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

type MysqlClient struct {
	db *gorm.DB
}

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

func (p *MysqlClient) DB(ctx context.Context, runMode string) *gorm.DB {
	if runMode == DebugMode {
		return p.db.WithContext(ctx).Debug()
	}

	return p.db.WithContext(ctx)
}

func (p *MysqlClient) Tx(ctx context.Context, runMode string) *gorm.DB {
	if runMode == DebugMode {
		return p.db.WithContext(ctx).Debug().Begin()
	}

	return p.db.WithContext(ctx).Begin()
}

func (p *MysqlClient) SetMaxOpenConns(maxOpenConns int) error {
	sqlDB, err := p.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetMaxOpenConns(maxOpenConns)

	return nil
}

func (p *MysqlClient) SetMaxIdleConns(maxIdleConns int) error {
	sqlDB, err := p.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetMaxIdleConns(maxIdleConns)

	return nil
}

func (p *MysqlClient) SetConnMaxLifetime(connMaxLifetime time.Duration) error {
	sqlDB, err := p.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetConnMaxLifetime(connMaxLifetime)

	return nil
}

func (p *MysqlClient) SetConnMaxIdleTime(connMaxIdleTime time.Duration) error {
	sqlDB, err := p.db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetConnMaxIdleTime(connMaxIdleTime)

	return nil
}
