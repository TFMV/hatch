// Package handlers contains Flight SQL protocol handlers.
package handlers

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

// QueryHandler handles query-related operations.
type QueryHandler interface {
	// ExecuteStatement executes a SQL statement and returns results.
	ExecuteStatement(ctx context.Context, query string, transactionID string) (*arrow.Schema, <-chan flight.StreamChunk, error)

	// ExecuteUpdate executes a SQL update and returns affected rows.
	ExecuteUpdate(ctx context.Context, query string, transactionID string) (int64, error)

	// GetFlightInfo returns flight information for a statement.
	GetFlightInfo(ctx context.Context, query string) (*flight.FlightInfo, error)
}

// MetadataHandler handles metadata discovery operations.
type MetadataHandler interface {
	// GetCatalogs returns available catalogs.
	GetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error)

	// GetSchemas returns schemas matching the filter.
	GetSchemas(ctx context.Context, catalog *string, schemaPattern *string) (*arrow.Schema, <-chan flight.StreamChunk, error)

	// GetTables returns tables matching the filter.
	GetTables(ctx context.Context, catalog *string, schemaPattern *string, tablePattern *string, tableTypes []string, includeSchema bool) (*arrow.Schema, <-chan flight.StreamChunk, error)

	// GetTableTypes returns available table types.
	GetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error)

	// GetPrimaryKeys returns primary keys for a table.
	GetPrimaryKeys(ctx context.Context, catalog *string, schema *string, table string) (*arrow.Schema, <-chan flight.StreamChunk, error)

	// GetImportedKeys returns imported foreign keys for a table.
	GetImportedKeys(ctx context.Context, catalog *string, schema *string, table string) (*arrow.Schema, <-chan flight.StreamChunk, error)

	// GetExportedKeys returns exported foreign keys for a table.
	GetExportedKeys(ctx context.Context, catalog *string, schema *string, table string) (*arrow.Schema, <-chan flight.StreamChunk, error)

	// GetXdbcTypeInfo returns XDBC type information.
	GetXdbcTypeInfo(ctx context.Context, dataType *int32) (*arrow.Schema, <-chan flight.StreamChunk, error)

	// GetSqlInfo returns SQL server information.
	GetSqlInfo(ctx context.Context, info []uint32) (*arrow.Schema, <-chan flight.StreamChunk, error)
}

// TransactionHandler handles transaction operations.
type TransactionHandler interface {
	// Begin starts a new transaction.
	Begin(ctx context.Context, readOnly bool) (string, error)

	// Commit commits a transaction.
	Commit(ctx context.Context, transactionID string) error

	// Rollback rolls back a transaction.
	Rollback(ctx context.Context, transactionID string) error
}

// PreparedStatementHandler handles prepared statement operations.
type PreparedStatementHandler interface {
	// Create creates a new prepared statement.
	Create(ctx context.Context, query string, transactionID string) (string, *arrow.Schema, error)

	// Close closes a prepared statement.
	Close(ctx context.Context, handle string) error

	// ExecuteQuery executes a prepared query statement.
	ExecuteQuery(ctx context.Context, handle string, params arrow.Record) (*arrow.Schema, <-chan flight.StreamChunk, error)

	// ExecuteUpdate executes a prepared update statement.
	ExecuteUpdate(ctx context.Context, handle string, params arrow.Record) (int64, error)

	// GetSchema returns the schema for a prepared statement.
	GetSchema(ctx context.Context, handle string) (*arrow.Schema, error)

	// GetParameterSchema returns the parameter schema for a prepared statement.
	GetParameterSchema(ctx context.Context, handle string) (*arrow.Schema, error)
}

// Logger defines the logging interface.
type Logger interface {
	Debug(msg string, keysAndValues ...interface{})
	Info(msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

// MetricsCollector defines the metrics interface.
type MetricsCollector interface {
	IncrementCounter(name string, tags ...string)
	RecordHistogram(name string, value float64, tags ...string)
	RecordGauge(name string, value float64, tags ...string)
	StartTimer(name string) Timer
}

// Timer represents a timing measurement.
type Timer interface {
	Stop()
}
