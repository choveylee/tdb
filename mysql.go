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
	EnableLog bool
}

func (l dbLogger) LogMode(level logger.LogLevel) logger.Interface {
	return l
}

func (l dbLogger) Info(ctx context.Context, msg string, args ...interface{}) {
	if l.EnableLog {
		tlog.I(ctx).Msgf(msg, args...)
	}
}

func (l dbLogger) Warn(ctx context.Context, msg string, args ...interface{}) {
	if l.EnableLog {
		tlog.W(ctx).Msgf(msg, args...)
	}
}

func (l dbLogger) Error(ctx context.Context, msg string, args ...interface{}) {
	tlog.E(ctx).Msgf(msg, args...)
}

func (l dbLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	latency := time.Since(begin)

	if latency > time.Millisecond*500 {
		rawSql, _ := fc()

		tlog.I(ctx).Msgf("slow query sql: %s, latency: %s", rawSql, latency)
	}
}

var _ logger.Interface = &dbLogger{}

func openDB(ctx context.Context, dsn string, enableLog bool) (*gorm.DB, error) {
	dialector := mysql.Open(dsn)

	otelPlugin := otelgorm.NewPlugin(
		otelgorm.WithDBName(dialector.(*mysql.Dialector).DSNConfig.DBName),
		otelgorm.WithoutQueryVariables(),
		otelgorm.WithoutMetrics(),
	)

	gormDB, err := gorm.Open(dialector, &gorm.Config{
		Logger: &dbLogger{
			EnableLog: enableLog,
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

	return gormDB, nil
}

type MysqlClient struct {
	db *gorm.DB
}

func NewMysqlClient(ctx context.Context, dsn string) (*MysqlClient, error) {
	db, err := openDB(ctx, dsn, false)
	if err != nil {
		return nil, err
	}

	mysqlClient := &MysqlClient{
		db: db,
	}

	return mysqlClient, nil
}

func NewMysqlClientWithLog(ctx context.Context, dsn string) (*MysqlClient, error) {
	gormDb, err := openDB(ctx, dsn, true)
	if err != nil {
		return nil, err
	}

	mysqlClient := &MysqlClient{
		db: gormDb,
	}

	return mysqlClient, nil
}

func (p *MysqlClient) DB(ctx context.Context) *gorm.DB {
	return p.db.WithContext(ctx)
}

func (p *MysqlClient) DebugDB(ctx context.Context) *gorm.DB {
	return p.db.WithContext(ctx).Debug()
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
