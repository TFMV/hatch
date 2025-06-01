// Package duckdb provides DuckDB-specific repository implementations.
package duckdb

import (
	"context"
	"database/sql"
	"sync"
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

// preparedStatementRepository implements repositories.PreparedStatementRepository for DuckDB.
type preparedStatementRepository struct {
	pool       pool.ConnectionPool
	allocator  memory.Allocator
	statements sync.Map // map[string]*preparedStatement
	logger     zerolog.Logger
}

// NewPreparedStatementRepository creates a new DuckDB prepared statement repository.
func NewPreparedStatementRepository(pool pool.ConnectionPool, allocator memory.Allocator, logger zerolog.Logger) repositories.PreparedStatementRepository {
	return &preparedStatementRepository{
		pool:      pool,
		allocator: allocator,
		logger:    logger,
	}
}

// Store stores a prepared statement.
func (r *preparedStatementRepository) Store(ctx context.Context, stmt *models.PreparedStatement) error {
	r.logger.Debug().
		Str("handle", stmt.Handle).
		Str("query", stmt.Query).
		Msg("Storing prepared statement")

	// Create internal prepared statement
	ps := &preparedStatement{
		model:     stmt,
		createdAt: time.Now(),
	}

	// Store in map
	r.statements.Store(stmt.Handle, ps)

	return nil
}

// Get retrieves a prepared statement by handle.
func (r *preparedStatementRepository) Get(ctx context.Context, handle string) (*models.PreparedStatement, error) {
	r.logger.Debug().Str("handle", handle).Msg("Getting prepared statement")

	if val, ok := r.statements.Load(handle); ok {
		ps := val.(*preparedStatement)
		return ps.model, nil
	}

	return nil, errors.ErrStatementNotFound.WithDetail("handle", handle)
}

// Remove removes a prepared statement.
func (r *preparedStatementRepository) Remove(ctx context.Context, handle string) error {
	r.logger.Debug().Str("handle", handle).Msg("Removing prepared statement")

	if val, ok := r.statements.LoadAndDelete(handle); ok {
		ps := val.(*preparedStatement)
		// Close the underlying statement if it exists
		if ps.stmt != nil {
			if err := ps.stmt.Close(); err != nil {
				r.logger.Warn().Err(err).Str("handle", handle).Msg("Failed to close prepared statement")
			}
		}
		return nil
	}

	return errors.ErrStatementNotFound.WithDetail("handle", handle)
}

// ExecuteQuery executes a prepared query statement.
func (r *preparedStatementRepository) ExecuteQuery(ctx context.Context, handle string, params [][]interface{}) (*models.QueryResult, error) {
	r.logger.Debug().
		Str("handle", handle).
		Int("param_batches", len(params)).
		Msg("Executing prepared query")

	// Get prepared statement
	val, ok := r.statements.Load(handle)
	if !ok {
		return nil, errors.ErrStatementNotFound.WithDetail("handle", handle)
	}

	ps := val.(*preparedStatement)

	// For now, we'll execute the query directly
	// In a real implementation, we would use the prepared statement
	db, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get database connection")
	}

	// Execute with parameters if provided
	var rows *sql.Rows
	if len(params) > 0 && len(params[0]) > 0 {
		rows, err = db.QueryContext(ctx, ps.model.Query, params[0]...)
	} else {
		rows, err = db.QueryContext(ctx, ps.model.Query)
	}

	if err != nil {
		r.logger.Error().Err(err).Str("query", ps.model.Query).Msg("Query execution failed")
		return nil, errors.Wrap(err, errors.CodeQueryFailed, "failed to execute prepared query")
	}

	// Create batch reader
	reader, err := converter.NewBatchReader(r.allocator, rows, r.logger)
	if err != nil {
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

	// Update execution count
	ps.model.ExecutionCount++
	ps.model.LastUsedAt = time.Now()

	return &models.QueryResult{
		Schema:  reader.Schema(),
		Records: records,
	}, nil
}

// ExecuteUpdate executes a prepared update statement.
func (r *preparedStatementRepository) ExecuteUpdate(ctx context.Context, handle string, params [][]interface{}) (*models.UpdateResult, error) {
	r.logger.Debug().
		Str("handle", handle).
		Int("param_batches", len(params)).
		Msg("Executing prepared update")

	// Get prepared statement
	val, ok := r.statements.Load(handle)
	if !ok {
		return nil, errors.ErrStatementNotFound.WithDetail("handle", handle)
	}

	ps := val.(*preparedStatement)

	// Get database connection
	db, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get database connection")
	}

	start := time.Now()

	// Execute with parameters if provided
	var result sql.Result
	if len(params) > 0 && len(params[0]) > 0 {
		result, err = db.ExecContext(ctx, ps.model.Query, params[0]...)
	} else {
		result, err = db.ExecContext(ctx, ps.model.Query)
	}

	if err != nil {
		r.logger.Error().Err(err).Str("statement", ps.model.Query).Msg("Update execution failed")
		return nil, errors.Wrap(err, errors.CodeQueryFailed, "failed to execute prepared update")
	}

	// Get rows affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		r.logger.Warn().Err(err).Msg("Failed to get rows affected")
		rowsAffected = -1
	}

	executionTime := time.Since(start)

	// Update execution count
	ps.model.ExecutionCount++
	ps.model.LastUsedAt = time.Now()

	r.logger.Debug().
		Int64("rows_affected", rowsAffected).
		Dur("execution_time", executionTime).
		Msg("Update executed successfully")

	return &models.UpdateResult{
		RowsAffected:  rowsAffected,
		ExecutionTime: executionTime,
	}, nil
}

// List returns all prepared statements for a transaction.
func (r *preparedStatementRepository) List(ctx context.Context, transactionID string) ([]*models.PreparedStatement, error) {
	r.logger.Debug().Str("transaction_id", transactionID).Msg("Listing prepared statements")

	var statements []*models.PreparedStatement

	r.statements.Range(func(key, value interface{}) bool {
		ps := value.(*preparedStatement)
		// If transactionID is empty, return all statements
		// Otherwise, only return statements for the specified transaction
		if transactionID == "" || ps.model.TransactionID == transactionID {
			statements = append(statements, ps.model)
		}
		return true
	})

	return statements, nil
}

// preparedStatement holds internal state for a prepared statement.
type preparedStatement struct {
	model     *models.PreparedStatement
	stmt      *sql.Stmt // Optional: actual prepared statement
	createdAt time.Time
}
