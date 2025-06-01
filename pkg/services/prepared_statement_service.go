// Package services contains business logic implementations.
package services

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/TFMV/flight/pkg/errors"
	"github.com/TFMV/flight/pkg/models"
	"github.com/TFMV/flight/pkg/repositories"
)

// preparedStatementService implements PreparedStatementService interface.
type preparedStatementService struct {
	repo       repositories.PreparedStatementRepository
	txnService TransactionService
	logger     Logger
	metrics    MetricsCollector
	mu         sync.RWMutex
}

// NewPreparedStatementService creates a new prepared statement service.
func NewPreparedStatementService(
	repo repositories.PreparedStatementRepository,
	txnService TransactionService,
	logger Logger,
	metrics MetricsCollector,
) PreparedStatementService {
	return &preparedStatementService{
		repo:       repo,
		txnService: txnService,
		logger:     logger,
		metrics:    metrics,
	}
}

// Create creates a new prepared statement.
func (s *preparedStatementService) Create(ctx context.Context, query string, transactionID string) (*models.PreparedStatement, error) {
	timer := s.metrics.StartTimer("prepared_statement_create")
	defer timer.Stop()

	s.logger.Debug("Creating prepared statement", "query", query, "transaction_id", transactionID)

	// Validate query
	if query == "" {
		s.metrics.IncrementCounter("prepared_statement_validation_errors")
		return nil, errors.New(errors.CodeInvalidRequest, "query cannot be empty")
	}

	// Validate transaction if specified
	var txn repositories.Transaction
	if transactionID != "" {
		var err error
		txn, err = s.txnService.Get(ctx, transactionID)
		if err != nil {
			s.logger.Error("Failed to get transaction", "error", err, "transaction_id", transactionID)
			s.metrics.IncrementCounter("transaction_lookup_errors")
			return nil, errors.Wrap(err, errors.CodeTransactionFailed, "failed to get transaction")
		}

		// Check if transaction is still active
		if !txn.IsActive() {
			s.metrics.IncrementCounter("inactive_transaction_errors")
			return nil, errors.New(errors.CodeTransactionFailed, "transaction is no longer active")
		}
	}

	// Generate handle
	handle := uuid.New().String()

	// Create prepared statement
	stmt := &models.PreparedStatement{
		Handle:        handle,
		Query:         query,
		CreatedAt:     time.Now(),
		LastUsedAt:    time.Now(),
		TransactionID: transactionID,
	}

	// Store in repository
	if err := s.repo.Store(ctx, stmt); err != nil {
		s.metrics.IncrementCounter("prepared_statement_create_errors")
		s.logger.Error("Failed to store prepared statement", "error", err, "handle", handle)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to create prepared statement")
	}

	// Update metrics
	s.metrics.IncrementCounter("prepared_statements_created")
	s.metrics.RecordGauge("active_prepared_statements", s.getActiveStatementCount(ctx))

	s.logger.Info("Prepared statement created", "handle", handle, "query", query)

	return stmt, nil
}

// Get retrieves a prepared statement by handle.
func (s *preparedStatementService) Get(ctx context.Context, handle string) (*models.PreparedStatement, error) {
	timer := s.metrics.StartTimer("prepared_statement_get")
	defer timer.Stop()

	s.logger.Debug("Getting prepared statement", "handle", handle)

	// Validate handle
	if handle == "" {
		s.metrics.IncrementCounter("prepared_statement_validation_errors")
		return nil, errors.New(errors.CodeInvalidRequest, "handle cannot be empty")
	}

	// Get from repository
	stmt, err := s.repo.Get(ctx, handle)
	if err != nil {
		if errors.IsNotFound(err) {
			s.metrics.IncrementCounter("prepared_statement_not_found")
			return nil, errors.ErrStatementNotFound.WithDetail("handle", handle)
		}
		s.metrics.IncrementCounter("prepared_statement_get_errors")
		s.logger.Error("Failed to get prepared statement", "error", err, "handle", handle)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get prepared statement")
	}

	// Update last used time (best effort)
	stmt.LastUsedAt = time.Now()

	return stmt, nil
}

// Close closes a prepared statement.
func (s *preparedStatementService) Close(ctx context.Context, handle string) error {
	timer := s.metrics.StartTimer("prepared_statement_close")
	defer timer.Stop()

	s.logger.Debug("Closing prepared statement", "handle", handle)

	// Validate handle
	if handle == "" {
		s.metrics.IncrementCounter("prepared_statement_validation_errors")
		return errors.New(errors.CodeInvalidRequest, "handle cannot be empty")
	}

	// Remove from repository
	if err := s.repo.Remove(ctx, handle); err != nil {
		if errors.IsNotFound(err) {
			s.metrics.IncrementCounter("prepared_statement_not_found")
			return errors.ErrStatementNotFound.WithDetail("handle", handle)
		}
		s.metrics.IncrementCounter("prepared_statement_close_errors")
		s.logger.Error("Failed to close prepared statement", "error", err, "handle", handle)
		return errors.Wrap(err, errors.CodeInternal, "failed to close prepared statement")
	}

	// Update metrics
	s.metrics.IncrementCounter("prepared_statements_closed")
	s.metrics.RecordGauge("active_prepared_statements", s.getActiveStatementCount(ctx))

	s.logger.Info("Prepared statement closed", "handle", handle)

	return nil
}

// ExecuteQuery executes a prepared query statement.
func (s *preparedStatementService) ExecuteQuery(ctx context.Context, handle string, params [][]interface{}) (*models.QueryResult, error) {
	timer := s.metrics.StartTimer("prepared_statement_execute_query")
	defer timer.Stop()

	s.logger.Debug("Executing prepared query", "handle", handle, "param_batches", len(params))

	// Get prepared statement
	stmt, err := s.Get(ctx, handle)
	if err != nil {
		return nil, err
	}

	// Check if it's a query (not an update)
	if stmt.IsResultSetUpdate {
		s.metrics.IncrementCounter("prepared_statement_type_mismatch")
		return nil, errors.New(errors.CodeInvalidRequest, "statement is an update, not a query")
	}

	// Validate transaction if specified
	if stmt.TransactionID != "" {
		txn, err := s.txnService.Get(ctx, stmt.TransactionID)
		if err != nil {
			s.logger.Error("Failed to get transaction", "error", err, "transaction_id", stmt.TransactionID)
			s.metrics.IncrementCounter("transaction_lookup_errors")
			return nil, errors.Wrap(err, errors.CodeTransactionFailed, "failed to get transaction")
		}

		// Check if transaction is still active
		if !txn.IsActive() {
			s.metrics.IncrementCounter("inactive_transaction_errors")
			return nil, errors.New(errors.CodeTransactionFailed, "transaction is no longer active")
		}
	}

	// Execute query
	start := time.Now()
	result, err := s.repo.ExecuteQuery(ctx, handle, params)
	executionTime := time.Since(start)

	if err != nil {
		s.metrics.IncrementCounter("prepared_statement_execute_errors")
		s.logger.Error("Failed to execute prepared query",
			"error", err,
			"handle", handle,
			"execution_time", executionTime)
		return nil, errors.Wrap(err, errors.CodeQueryFailed, "failed to execute prepared query")
	}

	// Update result with execution time
	result.ExecutionTime = executionTime

	// Update statement execution count
	stmt.ExecutionCount++
	stmt.LastUsedAt = time.Now()

	// Record metrics
	s.metrics.IncrementCounter("prepared_queries_executed")
	s.metrics.RecordHistogram("prepared_query_execution_time", executionTime.Seconds())
	if result.TotalRows >= 0 {
		s.metrics.RecordHistogram("prepared_query_result_rows", float64(result.TotalRows))
	}

	s.logger.Info("Prepared query executed successfully",
		"handle", handle,
		"rows", result.TotalRows,
		"execution_time", executionTime)

	return result, nil
}

// ExecuteUpdate executes a prepared update statement.
func (s *preparedStatementService) ExecuteUpdate(ctx context.Context, handle string, params [][]interface{}) (*models.UpdateResult, error) {
	timer := s.metrics.StartTimer("prepared_statement_execute_update")
	defer timer.Stop()

	s.logger.Debug("Executing prepared update", "handle", handle, "param_batches", len(params))

	// Get prepared statement
	stmt, err := s.Get(ctx, handle)
	if err != nil {
		return nil, err
	}

	// Check if it's an update (not a query)
	if !stmt.IsResultSetUpdate {
		s.metrics.IncrementCounter("prepared_statement_type_mismatch")
		return nil, errors.New(errors.CodeInvalidRequest, "statement is a query, not an update")
	}

	// Validate transaction if specified
	if stmt.TransactionID != "" {
		txn, err := s.txnService.Get(ctx, stmt.TransactionID)
		if err != nil {
			s.logger.Error("Failed to get transaction", "error", err, "transaction_id", stmt.TransactionID)
			s.metrics.IncrementCounter("transaction_lookup_errors")
			return nil, errors.Wrap(err, errors.CodeTransactionFailed, "failed to get transaction")
		}

		// Check if transaction is still active
		if !txn.IsActive() {
			s.metrics.IncrementCounter("inactive_transaction_errors")
			return nil, errors.New(errors.CodeTransactionFailed, "transaction is no longer active")
		}
	}

	// Execute update
	start := time.Now()
	result, err := s.repo.ExecuteUpdate(ctx, handle, params)
	executionTime := time.Since(start)

	if err != nil {
		s.metrics.IncrementCounter("prepared_statement_execute_errors")
		s.logger.Error("Failed to execute prepared update",
			"error", err,
			"handle", handle,
			"execution_time", executionTime)
		return nil, errors.Wrap(err, errors.CodeQueryFailed, "failed to execute prepared update")
	}

	// Update result with execution time
	result.ExecutionTime = executionTime

	// Update statement execution count
	stmt.ExecutionCount++
	stmt.LastUsedAt = time.Now()

	// Record metrics
	s.metrics.IncrementCounter("prepared_updates_executed")
	s.metrics.RecordHistogram("prepared_update_execution_time", executionTime.Seconds())
	s.metrics.RecordHistogram("prepared_update_affected_rows", float64(result.RowsAffected))

	s.logger.Info("Prepared update executed successfully",
		"handle", handle,
		"rows_affected", result.RowsAffected,
		"execution_time", executionTime)

	return result, nil
}

// List returns all prepared statements for a transaction.
func (s *preparedStatementService) List(ctx context.Context, transactionID string) ([]*models.PreparedStatement, error) {
	timer := s.metrics.StartTimer("prepared_statement_list")
	defer timer.Stop()

	s.logger.Debug("Listing prepared statements", "transaction_id", transactionID)

	// List from repository
	stmts, err := s.repo.List(ctx, transactionID)
	if err != nil {
		s.metrics.IncrementCounter("prepared_statement_list_errors")
		s.logger.Error("Failed to list prepared statements", "error", err, "transaction_id", transactionID)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to list prepared statements")
	}

	s.logger.Info("Listed prepared statements",
		"count", len(stmts),
		"transaction_id", transactionID)

	return stmts, nil
}

// getActiveStatementCount returns the current count of active prepared statements.
func (s *preparedStatementService) getActiveStatementCount(ctx context.Context) float64 {
	// This is a best-effort count
	// In a real implementation, this could be maintained in memory or queried from the repository
	stmts, err := s.repo.List(ctx, "")
	if err != nil {
		return 0
	}
	return float64(len(stmts))
}
