// Package duckdb provides DuckDB-specific repository implementations.
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
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
	var err error

	if txn != nil {
		// Use transaction
		dbTx := txn.GetDBTx()
		if dbTx == nil {
			return nil, errors.New(errors.CodeTransactionFailed, "transaction has no underlying database transaction")
		}

		if len(args) > 0 {
			rows, err = dbTx.QueryContext(ctx, query, args...)
		} else {
			rows, err = dbTx.QueryContext(ctx, query)
		}
	} else {
		// Use connection pool
		db, err := r.pool.Get(ctx)
		if err != nil {
			return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get database connection")
		}

		if len(args) > 0 {
			rows, err = db.QueryContext(ctx, query, args...)
		} else {
			rows, err = db.QueryContext(ctx, query)
		}
	}

	if err != nil {
		r.logger.Error().Err(err).Str("query", query).Msg("Query execution failed")
		return nil, r.wrapQueryError(err)
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
	var err error

	if txn != nil {
		// Use transaction
		dbTx := txn.GetDBTx()
		if dbTx == nil {
			return nil, errors.New(errors.CodeTransactionFailed, "transaction has no underlying database transaction")
		}

		if len(args) > 0 {
			result, err = dbTx.ExecContext(ctx, statement, args...)
		} else {
			result, err = dbTx.ExecContext(ctx, statement)
		}
	} else {
		// Use connection pool
		db, err := r.pool.Get(ctx)
		if err != nil {
			return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get database connection")
		}

		if len(args) > 0 {
			result, err = db.ExecContext(ctx, statement, args...)
		} else {
			result, err = db.ExecContext(ctx, statement)
		}
	}

	if err != nil {
		r.logger.Error().Err(err).Str("statement", statement).Msg("Update execution failed")
		return nil, r.wrapQueryError(err)
	}

	// Get rows affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		// Some databases don't support RowsAffected
		r.logger.Warn().Err(err).Msg("Failed to get rows affected")
		rowsAffected = -1
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
			r.logger.Error().Err(err).Str("query", query).Msg("Failed to prepare statement in transaction")
			return nil, r.wrapQueryError(err)
		}

		return stmt, nil
	}

	// Use connection pool
	db, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get database connection")
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		r.logger.Error().Err(err).Str("query", query).Msg("Failed to prepare statement")
		return nil, r.wrapQueryError(err)
	}

	return stmt, nil
}

// wrapQueryError wraps database errors with appropriate error codes.
func (r *queryRepository) wrapQueryError(err error) error {
	if err == nil {
		return nil
	}

	// Check for context errors first
	if err == context.Canceled {
		return errors.Wrap(err, errors.CodeCanceled, "query canceled")
	}
	if err == context.DeadlineExceeded {
		return errors.Wrap(err, errors.CodeDeadlineExceeded, "query timeout")
	}

	// Check for specific SQL errors
	errStr := err.Error()
	switch {
	case contains(errStr, "syntax error", "parse error"):
		return errors.Wrap(err, errors.CodeInvalidRequest, "SQL syntax error")
	case contains(errStr, "does not exist", "no such table", "no such column"):
		return errors.Wrap(err, errors.CodeNotFound, "object not found")
	case contains(errStr, "permission denied", "access denied"):
		return errors.Wrap(err, errors.CodePermissionDenied, "permission denied")
	case contains(errStr, "duplicate key", "unique constraint"):
		return errors.Wrap(err, errors.CodeAlreadyExists, "duplicate key violation")
	case contains(errStr, "foreign key constraint"):
		return errors.Wrap(err, errors.CodeFailedPrecondition, "foreign key constraint violation")
	case contains(errStr, "deadlock"):
		return errors.Wrap(err, errors.CodeAborted, "transaction deadlock")
	case contains(errStr, "connection", "socket"):
		return errors.Wrap(err, errors.CodeUnavailable, "database connection error")
	default:
		return errors.Wrap(err, errors.CodeQueryFailed, fmt.Sprintf("query failed: %s", err))
	}
}

// contains checks if any of the substrings are contained in s (case-insensitive).
func contains(s string, substrs ...string) bool {
	for _, substr := range substrs {
		if containsIgnoreCase(s, substr) {
			return true
		}
	}
	return false
}

// containsIgnoreCase checks if substr is contained in s (case-insensitive).
func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) && containsIgnoreCaseAt(s, substr, 0) >= 0
}

// containsIgnoreCaseAt finds the index of substr in s starting at position start (case-insensitive).
func containsIgnoreCaseAt(s, substr string, start int) int {
	if len(substr) == 0 {
		return start
	}
	if len(s)-start < len(substr) {
		return -1
	}

	for i := start; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if !equalFoldByte(s[i+j], substr[j]) {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// equalFoldByte compares two bytes case-insensitively (ASCII only).
func equalFoldByte(a, b byte) bool {
	if a == b {
		return true
	}
	// Convert to lowercase if uppercase letter
	if a >= 'A' && a <= 'Z' {
		a = a + ('a' - 'A')
	}
	if b >= 'A' && b <= 'Z' {
		b = b + ('a' - 'A')
	}
	return a == b
}
