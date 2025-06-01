// Package handlers contains Flight SQL protocol handlers.
package handlers

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/TFMV/flight/pkg/services"
)

// preparedStatementHandler implements PreparedStatementHandler interface.
type preparedStatementHandler struct {
	preparedStatementService services.PreparedStatementService
	queryService             services.QueryService
	allocator                memory.Allocator
	logger                   Logger
	metrics                  MetricsCollector
}

// NewPreparedStatementHandler creates a new prepared statement handler.
func NewPreparedStatementHandler(
	preparedStatementService services.PreparedStatementService,
	queryService services.QueryService,
	allocator memory.Allocator,
	logger Logger,
	metrics MetricsCollector,
) PreparedStatementHandler {
	return &preparedStatementHandler{
		preparedStatementService: preparedStatementService,
		queryService:             queryService,
		allocator:                allocator,
		logger:                   logger,
		metrics:                  metrics,
	}
}

// Create creates a new prepared statement.
func (h *preparedStatementHandler) Create(ctx context.Context, query string, transactionID string) (string, *arrow.Schema, error) {
	timer := h.metrics.StartTimer("handler_prepared_statement_create")
	defer timer.Stop()

	h.logger.Debug("Creating prepared statement",
		"query", truncateQuery(query),
		"transaction_id", transactionID)

	// Create prepared statement
	stmt, err := h.preparedStatementService.Create(ctx, query, transactionID)
	if err != nil {
		h.metrics.IncrementCounter("handler_prepared_statement_create_errors")
		h.logger.Error("Failed to create prepared statement", "error", err)
		return "", nil, fmt.Errorf("failed to create prepared statement: %w", err)
	}

	h.logger.Info("Prepared statement created",
		"handle", stmt.Handle,
		"has_parameters", stmt.ParameterSchema != nil)
	h.metrics.IncrementCounter("handler_prepared_statements_created")

	return stmt.Handle, stmt.ResultSetSchema, nil
}

// Close closes a prepared statement.
func (h *preparedStatementHandler) Close(ctx context.Context, handle string) error {
	timer := h.metrics.StartTimer("handler_prepared_statement_close")
	defer timer.Stop()

	h.logger.Debug("Closing prepared statement", "handle", handle)

	if handle == "" {
		h.metrics.IncrementCounter("handler_prepared_statement_invalid_handle")
		return fmt.Errorf("invalid prepared statement handle")
	}

	// Close prepared statement
	if err := h.preparedStatementService.Close(ctx, handle); err != nil {
		h.metrics.IncrementCounter("handler_prepared_statement_close_errors")
		h.logger.Error("Failed to close prepared statement", "error", err, "handle", handle)
		return fmt.Errorf("failed to close prepared statement: %w", err)
	}

	h.logger.Info("Prepared statement closed", "handle", handle)
	h.metrics.IncrementCounter("handler_prepared_statements_closed")

	return nil
}

// ExecuteQuery executes a prepared query statement.
func (h *preparedStatementHandler) ExecuteQuery(ctx context.Context, handle string, params arrow.Record) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_prepared_statement_execute_query")
	defer timer.Stop()

	h.logger.Debug("Executing prepared query", "handle", handle)

	if handle == "" {
		h.metrics.IncrementCounter("handler_prepared_statement_invalid_handle")
		return nil, nil, fmt.Errorf("invalid prepared statement handle")
	}

	// Convert Arrow record to parameter values
	paramValues, err := h.extractParameters(params)
	if err != nil {
		h.metrics.IncrementCounter("handler_prepared_statement_parameter_errors")
		return nil, nil, fmt.Errorf("failed to extract parameters: %w", err)
	}

	// Execute query
	result, err := h.preparedStatementService.ExecuteQuery(ctx, handle, paramValues)
	if err != nil {
		h.metrics.IncrementCounter("handler_prepared_statement_query_errors")
		h.logger.Error("Failed to execute prepared query", "error", err, "handle", handle)
		return nil, nil, fmt.Errorf("failed to execute prepared query: %w", err)
	}

	// Get schema from result
	schema := result.Schema
	if schema == nil {
		h.metrics.IncrementCounter("handler_prepared_statement_empty_schema")
		return nil, nil, fmt.Errorf("prepared query returned no schema")
	}

	// Create stream for results
	chunks := make(chan flight.StreamChunk, 16)

	// Start streaming results in background
	go func() {
		defer close(chunks)

		recordCount := 0
		for record := range result.Records {
			select {
			case <-ctx.Done():
				h.logger.Warn("Prepared query streaming cancelled", "records_sent", recordCount)
				record.Release()
				return
			case chunks <- flight.StreamChunk{Data: record}:
				recordCount++
			}
		}

		h.logger.Info("Prepared query streaming completed",
			"handle", handle,
			"records_sent", recordCount,
			"total_rows", result.TotalRows,
			"execution_time", result.ExecutionTime)

		h.metrics.RecordHistogram("handler_prepared_query_records", float64(recordCount))
		h.metrics.RecordHistogram("handler_prepared_query_duration", result.ExecutionTime.Seconds())
	}()

	return schema, chunks, nil
}

// ExecuteUpdate executes a prepared update statement.
func (h *preparedStatementHandler) ExecuteUpdate(ctx context.Context, handle string, params arrow.Record) (int64, error) {
	timer := h.metrics.StartTimer("handler_prepared_statement_execute_update")
	defer timer.Stop()

	h.logger.Debug("Executing prepared update", "handle", handle)

	if handle == "" {
		h.metrics.IncrementCounter("handler_prepared_statement_invalid_handle")
		return 0, fmt.Errorf("invalid prepared statement handle")
	}

	// Convert Arrow record to parameter values
	paramValues, err := h.extractParameters(params)
	if err != nil {
		h.metrics.IncrementCounter("handler_prepared_statement_parameter_errors")
		return 0, fmt.Errorf("failed to extract parameters: %w", err)
	}

	// Execute update
	result, err := h.preparedStatementService.ExecuteUpdate(ctx, handle, paramValues)
	if err != nil {
		h.metrics.IncrementCounter("handler_prepared_statement_update_errors")
		h.logger.Error("Failed to execute prepared update", "error", err, "handle", handle)
		return 0, fmt.Errorf("failed to execute prepared update: %w", err)
	}

	h.logger.Info("Prepared update executed successfully",
		"handle", handle,
		"rows_affected", result.RowsAffected,
		"execution_time", result.ExecutionTime)

	h.metrics.RecordHistogram("handler_prepared_update_rows", float64(result.RowsAffected))
	h.metrics.RecordHistogram("handler_prepared_update_duration", result.ExecutionTime.Seconds())

	return result.RowsAffected, nil
}

// GetSchema returns the schema for a prepared statement.
func (h *preparedStatementHandler) GetSchema(ctx context.Context, handle string) (*arrow.Schema, error) {
	timer := h.metrics.StartTimer("handler_prepared_statement_get_schema")
	defer timer.Stop()

	h.logger.Debug("Getting prepared statement schema", "handle", handle)

	if handle == "" {
		h.metrics.IncrementCounter("handler_prepared_statement_invalid_handle")
		return nil, fmt.Errorf("invalid prepared statement handle")
	}

	// Get prepared statement
	stmt, err := h.preparedStatementService.Get(ctx, handle)
	if err != nil {
		h.metrics.IncrementCounter("handler_prepared_statement_get_errors")
		h.logger.Error("Failed to get prepared statement", "error", err, "handle", handle)
		return nil, fmt.Errorf("failed to get prepared statement: %w", err)
	}

	if stmt.ResultSetSchema == nil {
		h.metrics.IncrementCounter("handler_prepared_statement_no_schema")
		return nil, fmt.Errorf("prepared statement has no schema")
	}

	h.logger.Info("Retrieved prepared statement schema", "handle", handle)

	return stmt.ResultSetSchema, nil
}

// GetParameterSchema returns the parameter schema for a prepared statement.
func (h *preparedStatementHandler) GetParameterSchema(ctx context.Context, handle string) (*arrow.Schema, error) {
	timer := h.metrics.StartTimer("handler_prepared_statement_get_parameter_schema")
	defer timer.Stop()

	h.logger.Debug("Getting prepared statement parameter schema", "handle", handle)

	if handle == "" {
		h.metrics.IncrementCounter("handler_prepared_statement_invalid_handle")
		return nil, fmt.Errorf("invalid prepared statement handle")
	}

	// Get prepared statement
	stmt, err := h.preparedStatementService.Get(ctx, handle)
	if err != nil {
		h.metrics.IncrementCounter("handler_prepared_statement_get_errors")
		h.logger.Error("Failed to get prepared statement", "error", err, "handle", handle)
		return nil, fmt.Errorf("failed to get prepared statement: %w", err)
	}

	// Return parameter schema (may be nil if no parameters)
	if stmt.ParameterSchema == nil {
		h.logger.Info("Prepared statement has no parameters", "handle", handle)
		// Return empty schema for statements without parameters
		return arrow.NewSchema([]arrow.Field{}, nil), nil
	}

	h.logger.Info("Retrieved prepared statement parameter schema",
		"handle", handle,
		"num_params", len(stmt.ParameterSchema.Fields()))

	return stmt.ParameterSchema, nil
}

// extractParameters extracts parameter values from an Arrow record.
func (h *preparedStatementHandler) extractParameters(params arrow.Record) ([][]interface{}, error) {
	if params == nil {
		return nil, nil
	}

	defer params.Release()

	numRows := params.NumRows()
	numCols := params.NumCols()

	// Create result slice
	result := make([][]interface{}, numRows)
	for i := range result {
		result[i] = make([]interface{}, numCols)
	}

	// Extract values from each column
	for colIdx := int64(0); colIdx < numCols; colIdx++ {
		col := params.Column(int(colIdx))

		for rowIdx := int64(0); rowIdx < numRows; rowIdx++ {
			if col.IsNull(int(rowIdx)) {
				result[rowIdx][colIdx] = nil
				continue
			}

			// Extract value based on column type
			value, err := h.extractValue(col, int(rowIdx))
			if err != nil {
				return nil, fmt.Errorf("failed to extract value at row %d, col %d: %w", rowIdx, colIdx, err)
			}
			result[rowIdx][colIdx] = value
		}
	}

	return result, nil
}

// extractValue extracts a single value from an Arrow array.
func (h *preparedStatementHandler) extractValue(arr arrow.Array, idx int) (interface{}, error) {
	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(idx), nil
	case *array.Int8:
		return a.Value(idx), nil
	case *array.Int16:
		return a.Value(idx), nil
	case *array.Int32:
		return a.Value(idx), nil
	case *array.Int64:
		return a.Value(idx), nil
	case *array.Uint8:
		return a.Value(idx), nil
	case *array.Uint16:
		return a.Value(idx), nil
	case *array.Uint32:
		return a.Value(idx), nil
	case *array.Uint64:
		return a.Value(idx), nil
	case *array.Float32:
		return a.Value(idx), nil
	case *array.Float64:
		return a.Value(idx), nil
	case *array.String:
		return a.Value(idx), nil
	case *array.Binary:
		return a.Value(idx), nil
	case *array.Timestamp:
		return a.Value(idx).ToTime(a.DataType().(*arrow.TimestampType).Unit), nil
	case *array.Date32:
		return a.Value(idx).ToTime(), nil
	case *array.Date64:
		return a.Value(idx).ToTime(), nil
	case *array.Time32:
		return a.Value(idx), nil
	case *array.Time64:
		return a.Value(idx), nil
	default:
		return nil, fmt.Errorf("unsupported array type: %T", arr)
	}
}
