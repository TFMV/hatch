// Package services contains business logic implementations.
package services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/TFMV/porter/pkg/errors"
	"github.com/TFMV/porter/pkg/models"
	"github.com/TFMV/porter/pkg/repositories"
)

// queryService implements QueryService interface.
type queryService struct {
	repo       repositories.QueryRepository
	txnService TransactionService
	logger     Logger
	metrics    MetricsCollector
	classifier *StatementClassifier
}

// NewQueryService creates a new query service.
func NewQueryService(
	repo repositories.QueryRepository,
	txnService TransactionService,
	logger Logger,
	metrics MetricsCollector,
) QueryService {
	return &queryService{
		repo:       repo,
		txnService: txnService,
		logger:     logger,
		metrics:    metrics,
		classifier: NewStatementClassifier(),
	}
}

// ExecuteQuery executes a query with proper error handling and metrics.
func (s *queryService) ExecuteQuery(ctx context.Context, req *models.QueryRequest) (*models.QueryResult, error) {
	// Start metrics
	timer := s.metrics.StartTimer("query_execution")
	defer timer.Stop()

	// Log query execution
	s.logger.Debug("Executing query", "query", req.Query, "transaction_id", req.TransactionID)

	// Validate request
	if err := s.validateQueryRequest(req); err != nil {
		s.metrics.IncrementCounter("query_validation_errors")
		return nil, err
	}

	// Get transaction if specified
	var txn repositories.Transaction
	if req.TransactionID != "" {
		var err error
		txn, err = s.txnService.Get(ctx, req.TransactionID)
		if err != nil {
			s.logger.Error("Failed to get transaction", "error", err, "transaction_id", req.TransactionID)
			s.metrics.IncrementCounter("transaction_lookup_errors")
			return nil, errors.Wrap(err, errors.CodeTransactionFailed, "failed to get transaction")
		}

		// Check if transaction is still active
		if !txn.IsActive() {
			s.metrics.IncrementCounter("inactive_transaction_errors")
			return nil, errors.New(errors.CodeTransactionFailed, "transaction is no longer active")
		}
	}

	// Execute query with timeout
	queryCtx := ctx
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		queryCtx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}

	// Execute query
	start := time.Now()
	result, err := s.repo.ExecuteQuery(queryCtx, req.Query, txn, req.Parameters...)
	executionTime := time.Since(start)

	if err != nil {
		s.metrics.IncrementCounter("query_execution_errors")
		s.logger.Error("Query execution failed",
			"error", err,
			"query", req.Query,
			"execution_time", executionTime)
		return nil, s.wrapQueryError(err)
	}

	// Update result with execution time
	result.ExecutionTime = executionTime

	// Record metrics
	s.metrics.IncrementCounter("successful_queries")
	s.metrics.RecordHistogram("query_execution_time", executionTime.Seconds())
	if result.TotalRows >= 0 {
		s.metrics.RecordHistogram("query_result_rows", float64(result.TotalRows))
	}

	s.logger.Info("Query executed successfully",
		"query", req.Query,
		"rows", result.TotalRows,
		"execution_time", executionTime)

	return result, nil
}

// ExecuteUpdate executes an update statement.
func (s *queryService) ExecuteUpdate(ctx context.Context, req *models.UpdateRequest) (*models.UpdateResult, error) {
	// Start metrics
	timer := s.metrics.StartTimer("update_execution")
	defer timer.Stop()

	// Log update execution
	s.logger.Debug("Executing update", "statement", req.Statement, "transaction_id", req.TransactionID)

	// Validate request
	if err := s.validateUpdateRequest(req); err != nil {
		s.metrics.IncrementCounter("update_validation_errors")
		return nil, err
	}

	// Get transaction if specified
	var txn repositories.Transaction
	if req.TransactionID != "" {
		var err error
		txn, err = s.txnService.Get(ctx, req.TransactionID)
		if err != nil {
			s.logger.Error("Failed to get transaction", "error", err, "transaction_id", req.TransactionID)
			s.metrics.IncrementCounter("transaction_lookup_errors")
			return nil, errors.Wrap(err, errors.CodeTransactionFailed, "failed to get transaction")
		}

		// Check if transaction is still active
		if !txn.IsActive() {
			s.metrics.IncrementCounter("inactive_transaction_errors")
			return nil, errors.New(errors.CodeTransactionFailed, "transaction is no longer active")
		}
	}

	// Execute update with timeout
	updateCtx := ctx
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		updateCtx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}

	// Execute update
	start := time.Now()
	result, err := s.repo.ExecuteUpdate(updateCtx, req.Statement, txn, req.Parameters...)
	executionTime := time.Since(start)

	if err != nil {
		s.metrics.IncrementCounter("update_execution_errors")
		s.logger.Error("Update execution failed",
			"error", err,
			"statement", req.Statement,
			"execution_time", executionTime)
		return nil, s.wrapQueryError(err)
	}

	// Update result with execution time
	result.ExecutionTime = executionTime

	// Record metrics
	s.metrics.IncrementCounter("successful_updates")
	s.metrics.RecordHistogram("update_execution_time", executionTime.Seconds())
	s.metrics.RecordHistogram("update_affected_rows", float64(result.RowsAffected))

	s.logger.Info("Update executed successfully",
		"statement", req.Statement,
		"rows_affected", result.RowsAffected,
		"execution_time", executionTime)

	return result, nil
}

// ValidateQuery validates a SQL query without executing it.
func (s *queryService) ValidateQuery(ctx context.Context, query string) error {
	// Basic validation
	if query == "" {
		return errors.New(errors.CodeInvalidRequest, "query cannot be empty")
	}

	// Normalize query
	query = strings.TrimSpace(query)

	// Check for dangerous operations (can be made configurable)
	upperQuery := strings.ToUpper(query)
	dangerousKeywords := []string{
		"DROP DATABASE",
		"DROP SCHEMA",
		"TRUNCATE",
	}

	for _, keyword := range dangerousKeywords {
		if strings.Contains(upperQuery, keyword) {
			s.logger.Warn("Dangerous query detected", "query", query, "keyword", keyword)
			s.metrics.IncrementCounter("dangerous_query_attempts")
			// Note: We don't block here, just log. This can be made configurable.
		}
	}

	if err := s.classifier.ValidateStatement(query); err != nil {
		return errors.New(errors.CodeInvalidRequest, err.Error())
	}

	return nil
}

// validateQueryRequest validates a query request.
func (s *queryService) validateQueryRequest(req *models.QueryRequest) error {
	if req == nil {
		return errors.New(errors.CodeInvalidRequest, "query request cannot be nil")
	}

	if req.Query == "" {
		return errors.ErrInvalidQuery.WithDetail("query", "cannot be empty")
	}

	// Validate max rows
	if req.MaxRows < 0 {
		return errors.New(errors.CodeInvalidRequest, "max_rows cannot be negative")
	}

	// Validate timeout
	if req.Timeout < 0 {
		return errors.New(errors.CodeInvalidRequest, "timeout cannot be negative")
	}

	return nil
}

// validateUpdateRequest validates an update request.
func (s *queryService) validateUpdateRequest(req *models.UpdateRequest) error {
	if req == nil {
		return errors.New(errors.CodeInvalidRequest, "update request cannot be nil")
	}

	if req.Statement == "" {
		return errors.New(errors.CodeInvalidRequest, "statement cannot be empty")
	}

	if err := s.classifier.ValidateStatement(req.Statement); err != nil {
		return errors.New(errors.CodeInvalidRequest, err.Error())
	}

	// Validate timeout
	if req.Timeout < 0 {
		return errors.New(errors.CodeInvalidRequest, "timeout cannot be negative")
	}

	return nil
}

// GetStatementType returns the type of a SQL statement.
func (s *queryService) GetStatementType(query string) StatementType {
	return s.classifier.ClassifyStatement(query)
}

// IsUpdateStatement returns true if the statement should return an update count.
func (s *queryService) IsUpdateStatement(query string) bool {
	return s.classifier.IsUpdateStatement(query)
}

// IsQueryStatement returns true if the statement should return a result set.
func (s *queryService) IsQueryStatement(query string) bool {
	return s.classifier.IsQueryStatement(query)
}

// wrapQueryError wraps database errors with appropriate error codes.
func (s *queryService) wrapQueryError(err error) error {
	if err == nil {
		return nil
	}

	errStr := err.Error()

	// Check for common database errors
	switch {
	case strings.Contains(errStr, "syntax error"):
		return errors.Wrap(err, errors.CodeInvalidRequest, "SQL syntax error")
	case strings.Contains(errStr, "does not exist"):
		return errors.Wrap(err, errors.CodeNotFound, "object not found")
	case strings.Contains(errStr, "permission denied"):
		return errors.Wrap(err, errors.CodePermissionDenied, "permission denied")
	case strings.Contains(errStr, "timeout"):
		return errors.Wrap(err, errors.CodeDeadlineExceeded, "query timeout")
	case strings.Contains(errStr, "connection"):
		return errors.Wrap(err, errors.CodeUnavailable, "database connection error")
	default:
		return errors.Wrap(err, errors.CodeQueryFailed, fmt.Sprintf("query failed: %s", err))
	}
}
