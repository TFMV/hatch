// Package services contains business logic implementations.
package services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/TFMV/hatch/pkg/errors"
	"github.com/TFMV/hatch/pkg/models"
	"github.com/TFMV/hatch/pkg/repositories"
	"github.com/apache/arrow-go/v18/arrow"
)

// preparedStatementService implements PreparedStatementService interface.
type preparedStatementService struct {
	repo       repositories.PreparedStatementRepository
	txnService TransactionService
	logger     Logger
	metrics    MetricsCollector
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

	// Create parameter schema based on the query
	var paramSchema *arrow.Schema
	if strings.Contains(query, "?") {
		// Count the number of parameters
		paramCount := strings.Count(query, "?")
		fields := make([]arrow.Field, paramCount)

		// Try to infer parameter types from the query
		// This is a simple implementation - in a real system, you'd want to use a proper SQL parser
		queryUpper := strings.ToUpper(query)
		if strings.Contains(queryUpper, "INSERT INTO") {
			// For INSERT queries, try to infer types from the column list
			startIdx := strings.Index(queryUpper, "(")
			endIdx := strings.Index(queryUpper, ")")
			if startIdx > 0 && endIdx > startIdx {
				columns := strings.Split(query[startIdx+1:endIdx], ",")
				for i := 0; i < paramCount && i < len(columns); i++ {
					colName := strings.TrimSpace(columns[i])
					fields[i] = arrow.Field{
						Name:     fmt.Sprintf("param%d", i+1),
						Type:     inferTypeFromColumnName(colName),
						Nullable: true,
					}
				}
			}
		}

		// For any remaining fields or if we couldn't infer types, use default types
		for i := 0; i < paramCount; i++ {
			if fields[i].Type == nil {
				fields[i] = arrow.Field{
					Name:     fmt.Sprintf("param%d", i+1),
					Type:     arrow.PrimitiveTypes.Int64,
					Nullable: true,
				}
			}
		}
		paramSchema = arrow.NewSchema(fields, nil)
	}

	// Create prepared statement
	stmt := &models.PreparedStatement{
		Handle:            handle,
		Query:             query,
		ParameterSchema:   paramSchema,
		ResultSetSchema:   arrow.NewSchema([]arrow.Field{}, nil),                                   // Initialize with empty schema
		IsResultSetUpdate: !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT"), // Basic check for update type
		CreatedAt:         time.Now(),
		LastUsedAt:        time.Now(),
		TransactionID:     transactionID,
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

	s.logger.Info("Prepared statement created",
		"handle", handle,
		"query", query,
		"has_parameters", paramSchema != nil)

	return stmt, nil
}

// inferTypeFromColumnName tries to infer the Arrow type from a column name
func inferTypeFromColumnName(colName string) arrow.DataType {
	colName = strings.ToUpper(colName)
	switch {
	case strings.Contains(colName, "ID") || strings.Contains(colName, "COUNT"):
		return arrow.PrimitiveTypes.Int64
	case strings.Contains(colName, "NAME") || strings.Contains(colName, "STRING") || strings.Contains(colName, "TEXT"):
		return arrow.BinaryTypes.String
	case strings.Contains(colName, "FLOAT") || strings.Contains(colName, "DOUBLE") || strings.Contains(colName, "DECIMAL"):
		return arrow.PrimitiveTypes.Float64
	default:
		return arrow.PrimitiveTypes.Int64 // Default to Int64
	}
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

// SetParameters sets parameters for a prepared statement.
func (s *preparedStatementService) SetParameters(ctx context.Context, handle string, params [][]interface{}) error {
	timer := s.metrics.StartTimer("prepared_statement_set_parameters")
	defer timer.Stop()

	s.logger.Debug("Setting parameters for prepared statement", "handle", handle, "param_batches", len(params))

	// Get prepared statement
	stmt, err := s.Get(ctx, handle)
	if err != nil {
		return err // Get already logs and increments metrics
	}

	if stmt.ParameterSchema != nil && len(params) > 0 {
		expected := len(stmt.ParameterSchema.Fields())
		if len(params[0]) != expected {
			return errors.New(errors.CodeInvalidRequest, fmt.Sprintf("parameter count mismatch: expected %d got %d", expected, len(params[0])))
		}
	}

	// Store parameters in the statement model (or call repository to update if needed)
	// This depends on how the repository and model are designed to handle parameter binding.
	// For this example, let's assume the PreparedStatement model can hold the bound parameters.
	// If params are to be stored persistently or in a way the repo manages, this would call s.repo.SetParameters(ctx, handle, params)
	stmt.BoundParameters = params // This field needs to be added to models.PreparedStatement
	stmt.LastUsedAt = time.Now()

	// If the repository needs to be updated with the new BoundParameters or LastUsedAt:
	if err := s.repo.Store(ctx, stmt); err != nil {
		s.metrics.IncrementCounter("prepared_statement_set_parameters_errors")
		s.logger.Error("Failed to update prepared statement with new parameters", "error", err, "handle", handle)
		return errors.Wrap(err, errors.CodeInternal, "failed to set parameters")
	}

	s.logger.Info("Parameters set for prepared statement", "handle", handle)
	s.metrics.IncrementCounter("prepared_statement_parameters_set")
	return nil
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
