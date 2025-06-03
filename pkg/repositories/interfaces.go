// Package repositories defines interfaces for data access operations.
package repositories

import (
	"context"
	"database/sql"

	"github.com/TFMV/hatch/pkg/models"
)

// Transaction represents a database transaction with Flight SQL semantics.
type Transaction interface {
	// ID returns the unique transaction identifier.
	ID() string
	// Commit commits the transaction.
	Commit(ctx context.Context) error
	// Rollback rolls back the transaction.
	Rollback(ctx context.Context) error
	// IsActive returns true if the transaction is still active.
	IsActive() bool
	// GetDBTx returns the underlying database transaction.
	GetDBTx() *sql.Tx
}

// QueryRepository defines query operations.
type QueryRepository interface {
	// ExecuteQuery executes a query and returns results.
	ExecuteQuery(ctx context.Context, query string, txn Transaction, args ...interface{}) (*models.QueryResult, error)
	// ExecuteUpdate executes an update statement and returns affected rows.
	ExecuteUpdate(ctx context.Context, query string, txn Transaction, args ...interface{}) (*models.UpdateResult, error)
	// Prepare prepares a statement for later execution.
	Prepare(ctx context.Context, query string, txn Transaction) (*sql.Stmt, error)
}

// MetadataRepository defines metadata operations.
type MetadataRepository interface {
	// GetCatalogs returns all available catalogs.
	GetCatalogs(ctx context.Context) ([]models.Catalog, error)
	// GetSchemas returns schemas matching the filter.
	GetSchemas(ctx context.Context, catalog string, pattern string) ([]models.Schema, error)
	// GetTables returns tables matching the options.
	GetTables(ctx context.Context, opts models.GetTablesOptions) ([]models.Table, error)
	// GetTableTypes returns all available table types.
	GetTableTypes(ctx context.Context) ([]string, error)
	// GetColumns returns columns for a specific table.
	GetColumns(ctx context.Context, table models.TableRef) ([]models.Column, error)
	// GetPrimaryKeys returns primary keys for a table.
	GetPrimaryKeys(ctx context.Context, table models.TableRef) ([]models.Key, error)
	// GetImportedKeys returns foreign keys that reference a table.
	GetImportedKeys(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error)
	// GetExportedKeys returns foreign keys from a table.
	GetExportedKeys(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error)
	// GetCrossReference returns foreign key relationships between two tables.
	GetCrossReference(ctx context.Context, ref models.CrossTableRef) ([]models.ForeignKey, error)
	// GetTypeInfo returns database type information.
	GetTypeInfo(ctx context.Context, dataType *int32) ([]models.XdbcTypeInfo, error)
	// GetSQLInfo returns SQL feature information.
	GetSQLInfo(ctx context.Context, infoTypes []uint32) ([]models.SQLInfo, error)
}

// TransactionRepository defines transaction operations.
type TransactionRepository interface {
	// Begin starts a new transaction.
	Begin(ctx context.Context, opts models.TransactionOptions) (Transaction, error)
	// Get retrieves an existing transaction by ID.
	Get(ctx context.Context, id string) (Transaction, error)
	// List returns all active transactions.
	List(ctx context.Context) ([]Transaction, error)
	// Remove removes a transaction from the repository.
	Remove(ctx context.Context, id string) error
}

// PreparedStatementRepository defines prepared statement operations.
type PreparedStatementRepository interface {
	// Store stores a prepared statement.
	Store(ctx context.Context, stmt *models.PreparedStatement) error
	// Get retrieves a prepared statement by handle.
	Get(ctx context.Context, handle string) (*models.PreparedStatement, error)
	// Remove removes a prepared statement.
	Remove(ctx context.Context, handle string) error
	// List returns all prepared statements for a transaction.
	List(ctx context.Context, transactionID string) ([]*models.PreparedStatement, error)
	// ExecuteQuery executes a prepared query statement.
	ExecuteQuery(ctx context.Context, handle string, params [][]interface{}) (*models.QueryResult, error)
	// ExecuteUpdate executes a prepared update statement.
	ExecuteUpdate(ctx context.Context, handle string, params [][]interface{}) (*models.UpdateResult, error)
}
