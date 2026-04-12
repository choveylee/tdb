package tdb

const (
	// DebugMode selects verbose SQL logging: [MysqlClient.DB] and [MysqlClient.Tx] run the underlying GORM session in Debug mode.
	DebugMode = "debug"

	// ReleaseMode is the default run mode without forcing GORM Debug on every statement.
	ReleaseMode = "release"
)
