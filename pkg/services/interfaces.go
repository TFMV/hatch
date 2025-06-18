// Package services contains business logic implementations.
package services

import (
	"context"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/TFMV/hatch/pkg/models"
	"github.com/TFMV/hatch/pkg/repositories"
)

// QueryService defines query operations.
type QueryService interface {
	ExecuteQuery(ctx context.Context, req *models.QueryRequest) (*models.QueryResult, error)
	ExecuteUpdate(ctx context.Context, req *models.UpdateRequest) (*models.UpdateResult, error)
	ValidateQuery(ctx context.Context, query string) error
	GetStatementType(query string) StatementType
	IsUpdateStatement(query string) bool
	IsQueryStatement(query string) bool
}

// MetadataService defines metadata operations.
type MetadataService interface {
	GetCatalogs(ctx context.Context) ([]models.Catalog, error)
	GetSchemas(ctx context.Context, catalog string, pattern string) ([]models.Schema, error)
	GetTables(ctx context.Context, opts models.GetTablesOptions) ([]models.Table, error)
	GetTableTypes(ctx context.Context) ([]string, error)
	GetColumns(ctx context.Context, table models.TableRef) ([]models.Column, error)
	GetTableSchema(ctx context.Context, table models.TableRef) (*arrow.Schema, error)
	GetPrimaryKeys(ctx context.Context, table models.TableRef) ([]models.Key, error)
	GetImportedKeys(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error)
	GetExportedKeys(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error)
	GetCrossReference(ctx context.Context, ref models.CrossTableRef) ([]models.ForeignKey, error)
	GetTypeInfo(ctx context.Context, dataType *int32) ([]models.XdbcTypeInfo, error)
	GetSQLInfo(ctx context.Context, infoTypes []uint32) ([]models.SQLInfo, error)
}

// TransactionService defines transaction operations.
type TransactionService interface {
	Begin(ctx context.Context, opts models.TransactionOptions) (string, error)
	Get(ctx context.Context, id string) (repositories.Transaction, error)
	Commit(ctx context.Context, id string) error
	Rollback(ctx context.Context, id string) error
	List(ctx context.Context) ([]repositories.Transaction, error)
	CleanupInactive(ctx context.Context) error
}

// PreparedStatementService defines prepared statement operations.
type PreparedStatementService interface {
	Create(ctx context.Context, query string, transactionID string) (*models.PreparedStatement, error)
	Get(ctx context.Context, handle string) (*models.PreparedStatement, error)
	Close(ctx context.Context, handle string) error
	ExecuteQuery(ctx context.Context, handle string, params [][]interface{}) (*models.QueryResult, error)
	ExecuteUpdate(ctx context.Context, handle string, params [][]interface{}) (*models.UpdateResult, error)
	List(ctx context.Context, transactionID string) ([]*models.PreparedStatement, error)
	SetParameters(ctx context.Context, handle string, params [][]interface{}) error
}

// Logger defines logging interface.
type Logger interface {
	Debug(msg string, keysAndValues ...interface{})
	Info(msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

// MetricsCollector defines metrics collection interface.
type MetricsCollector interface {
	IncrementCounter(name string, labels ...string)
	RecordHistogram(name string, value float64, labels ...string)
	RecordGauge(name string, value float64, labels ...string)
	StartTimer(name string) Timer
}

// Timer represents a timing measurement.
type Timer interface {
	Stop() time.Duration
}
