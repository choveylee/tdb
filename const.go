package tdb

const (
	// DebugMode enables verbose SQL logging. [MysqlClient.DB] and [MysqlClient.Tx] return GORM sessions with Debug logging enabled.
	DebugMode = "debug"

	// ReleaseMode uses the standard GORM session without forcing Debug logging for every statement.
	ReleaseMode = "release"
)
