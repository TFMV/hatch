// Package duckdb provides DuckDB-specific repository implementations.
package duckdb

import (
	"context"
	"database/sql"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	"github.com/TFMV/flight/pkg/errors"
	"github.com/TFMV/flight/pkg/infrastructure/converter"
	"github.com/TFMV/flight/pkg/infrastructure/pool"
	"github.com/TFMV/flight/pkg/models"
	"github.com/TFMV/flight/pkg/repositories"
)

// queryRepository implements repositories.QueryRepository for DuckDB.
type queryRepository struct {
	pool      pool.ConnectionPool
	allocator memory.Allocator
	logger    zerolog.Logger
}

// NewQueryRepository creates a new DuckDB query repository.
func NewQueryRepository(pool pool.ConnectionPool, allocator memory.Allocator, logger zerolog.Logger) repositories.QueryRepository {
	return &queryRepository{
		pool:      pool,
		allocator: allocator,
		logger:    logger,
	}
}

// ExecuteQuery executes a query and returns results.
func (r *queryRepository) ExecuteQuery(ctx context.Context, query string, txn repositories.Transaction, args ...interface{}) (*models.QueryResult, error) {
	r.logger.Debug().
		Str("query", query).
		Bool("has_transaction", txn != nil).
		Int("args_count", len(args)).
		Msg("Executing query")

	// Get database connection or transaction
	var rows *sql.Rows

	if txn != nil {
		// Use transaction
		dbTx := txn.GetDBTx()
		if dbTx == nil {
			return nil, errors.New(errors.CodeTransactionFailed, "transaction has no underlying database transaction")
		}

		var err error
		if len(args) > 0 {
			rows, err = dbTx.QueryContext(ctx, query, args...)
		} else {
			rows, err = dbTx.QueryContext(ctx, query)
		}
		if err != nil {
			return nil, errors.Wrapf(err, errors.CodeQueryFailed, "failed to execute query with transaction: %s", query)
		}
	} else {
		// Use connection pool
		db, err := r.pool.Get(ctx)
		if err != nil {
			return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get connection from pool")
		}

		if len(args) > 0 {
			rows, err = db.QueryContext(ctx, query, args...)
		} else {
			rows, err = db.QueryContext(ctx, query)
		}
		if err != nil {
			return nil, errors.Wrapf(err, errors.CodeQueryFailed, "failed to execute query: %s", query)
		}
	}

	// Create batch reader
	reader, err := converter.NewBatchReader(r.allocator, rows, r.logger)
	if err != nil {
		// rows are already closed by NewBatchReader on error
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to create batch reader")
	}

	// Create record channel
	records := make(chan arrow.Record, 10)

	// Start goroutine to read batches
	go func() {
		defer close(records)
		defer reader.Release()

		totalRows := int64(0)

		for reader.Next() {
			record := reader.Record()
			if record == nil {
				continue
			}

			// Retain the record before sending
			record.Retain()
			totalRows += record.NumRows()

			select {
			case records <- record:
			case <-ctx.Done():
				record.Release()
				return
			}
		}

		if err := reader.Err(); err != nil {
			r.logger.Error().Err(err).Msg("Error reading query results")
		}

		r.logger.Debug().Int64("total_rows", totalRows).Msg("Query results read complete")
	}()

	return &models.QueryResult{
		Schema:  reader.Schema(),
		Records: records,
	}, nil
}

// ExecuteUpdate executes an update statement and returns affected rows.
func (r *queryRepository) ExecuteUpdate(ctx context.Context, statement string, txn repositories.Transaction, args ...interface{}) (*models.UpdateResult, error) {
	r.logger.Debug().
		Str("statement", statement).
		Bool("has_transaction", txn != nil).
		Int("args_count", len(args)).
		Msg("Executing update")

	start := time.Now()

	// Get database connection or transaction
	var result sql.Result

	if txn != nil {
		// Use transaction
		dbTx := txn.GetDBTx()
		if dbTx == nil {
			return nil, errors.New(errors.CodeTransactionFailed, "transaction has no underlying database transaction")
		}

		var err error
		if len(args) > 0 {
			result, err = dbTx.ExecContext(ctx, statement, args...)
		} else {
			result, err = dbTx.ExecContext(ctx, statement)
		}
		if err != nil {
			return nil, errors.Wrapf(err, errors.CodeQueryFailed, "failed to execute update with transaction: %s", statement)
		}
	} else {
		// Use connection pool
		db, err := r.pool.Get(ctx)
		if err != nil {
			return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get connection from pool")
		}

		if len(args) > 0 {
			result, err = db.ExecContext(ctx, statement, args...)
		} else {
			result, err = db.ExecContext(ctx, statement)
		}
		if err != nil {
			return nil, errors.Wrapf(err, errors.CodeQueryFailed, "failed to execute update: %s", statement)
		}
	}

	// Get rows affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeQueryFailed, "failed to get rows affected")
	}

	executionTime := time.Since(start)

	r.logger.Debug().
		Int64("rows_affected", rowsAffected).
		Dur("execution_time", executionTime).
		Msg("Update executed successfully")

	return &models.UpdateResult{
		RowsAffected:  rowsAffected,
		ExecutionTime: executionTime,
	}, nil
}

// Prepare prepares a statement for later execution.
func (r *queryRepository) Prepare(ctx context.Context, query string, txn repositories.Transaction) (*sql.Stmt, error) {
	r.logger.Debug().
		Str("query", query).
		Bool("has_transaction", txn != nil).
		Msg("Preparing statement")

	if txn != nil {
		// Use transaction
		dbTx := txn.GetDBTx()
		if dbTx == nil {
			return nil, errors.New(errors.CodeTransactionFailed, "transaction has no underlying database transaction")
		}

		stmt, err := dbTx.PrepareContext(ctx, query)
		if err != nil {
			return nil, errors.Wrapf(err, errors.CodeQueryFailed, "failed to prepare statement with transaction: %s", query)
		}
		return stmt, nil
	}

	// Use connection pool
	db, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get connection from pool")
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, errors.Wrapf(err, errors.CodeQueryFailed, "failed to prepare statement: %s", query)
	}
	return stmt, nil
}
