// Package handlers contains Flight SQL protocol handlers.
package handlers

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"

	"github.com/TFMV/flight/pkg/errors"
	"github.com/TFMV/flight/pkg/models"
	"github.com/TFMV/flight/pkg/services"
)

// queryHandler implements QueryHandler interface.
type queryHandler struct {
	queryService services.QueryService
	allocator    memory.Allocator
	logger       Logger
	metrics      MetricsCollector
}

// NewQueryHandler creates a new query handler.
func NewQueryHandler(
	queryService services.QueryService,
	allocator memory.Allocator,
	logger Logger,
	metrics MetricsCollector,
) QueryHandler {
	return &queryHandler{
		queryService: queryService,
		allocator:    allocator,
		logger:       logger,
		metrics:      metrics,
	}
}

// ExecuteStatement executes a SQL statement and returns results.
func (h *queryHandler) ExecuteStatement(ctx context.Context, query string, transactionID string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_execute_statement")
	defer timer.Stop()

	h.logger.Debug("Executing statement",
		"query", truncateQuery(query),
		"transaction_id", transactionID)

	// Create query request
	req := &models.QueryRequest{
		Query:         query,
		TransactionID: transactionID,
	}

	// Execute query
	result, err := h.queryService.ExecuteQuery(ctx, req)
	if err != nil {
		h.metrics.IncrementCounter("handler_query_errors")
		h.logger.Error("Failed to execute query", "error", err)
		return nil, nil, h.mapServiceError(err)
	}

	// Get Arrow schema from result
	schema := result.Schema
	if schema == nil {
		h.metrics.IncrementCounter("handler_empty_schema")
		return nil, nil, errors.New(errors.CodeInternal, "query returned no schema")
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
				h.logger.Warn("Query streaming cancelled", "records_sent", recordCount)
				record.Release()
				return
			case chunks <- flight.StreamChunk{Data: record}:
				recordCount++
			}
		}

		h.logger.Info("Query streaming completed",
			"records_sent", recordCount,
			"total_rows", result.TotalRows,
			"execution_time", result.ExecutionTime)

		h.metrics.RecordHistogram("handler_query_records", float64(recordCount))
		h.metrics.RecordHistogram("handler_query_duration", result.ExecutionTime.Seconds())
	}()

	return schema, chunks, nil
}

// ExecuteUpdate executes a SQL update and returns affected rows.
func (h *queryHandler) ExecuteUpdate(ctx context.Context, query string, transactionID string) (int64, error) {
	timer := h.metrics.StartTimer("handler_execute_update")
	defer timer.Stop()

	h.logger.Debug("Executing update",
		"query", truncateQuery(query),
		"transaction_id", transactionID)

	// Create update request
	req := &models.UpdateRequest{
		Statement:     query,
		TransactionID: transactionID,
	}

	// Execute update
	result, err := h.queryService.ExecuteUpdate(ctx, req)
	if err != nil {
		h.metrics.IncrementCounter("handler_update_errors")
		h.logger.Error("Failed to execute update", "error", err)
		return 0, h.mapServiceError(err)
	}

	h.logger.Info("Update executed successfully",
		"rows_affected", result.RowsAffected,
		"execution_time", result.ExecutionTime)

	h.metrics.RecordHistogram("handler_update_rows", float64(result.RowsAffected))
	h.metrics.RecordHistogram("handler_update_duration", result.ExecutionTime.Seconds())

	return result.RowsAffected, nil
}

// GetFlightInfo returns flight information for a statement.
func (h *queryHandler) GetFlightInfo(ctx context.Context, query string) (*flight.FlightInfo, error) {
	timer := h.metrics.StartTimer("handler_get_flight_info")
	defer timer.Stop()

	h.logger.Debug("Getting flight info", "query", truncateQuery(query))

	// Validate query to get schema
	if err := h.queryService.ValidateQuery(ctx, query); err != nil {
		h.metrics.IncrementCounter("handler_validation_errors")
		return nil, h.mapServiceError(err)
	}

	// For now, create a minimal FlightInfo
	// In a real implementation, we would analyze the query to provide accurate schema
	ticket := &flight.Ticket{
		Ticket: []byte(uuid.New().String()),
	}

	endpoint := &flight.FlightEndpoint{
		Ticket: ticket,
		// In a distributed system, we would include location information
		Location: []*flight.Location{},
	}

	// Create a descriptor for the query
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(query),
	}

	info := &flight.FlightInfo{
		FlightDescriptor: descriptor,
		Endpoint:         []*flight.FlightEndpoint{endpoint},
		TotalRecords:     -1, // Unknown until execution
		TotalBytes:       -1, // Unknown until execution
		// Schema would be set if we could determine it without execution
	}

	h.metrics.IncrementCounter("handler_flight_info_created")

	return info, nil
}

// mapServiceError maps service errors to appropriate Flight errors.
func (h *queryHandler) mapServiceError(err error) error {
	if err == nil {
		return nil
	}

	// Check if it's already a Flight error
	if flightErr, ok := err.(*errors.FlightError); ok {
		switch flightErr.Code {
		case errors.CodeInvalidRequest:
			return fmt.Errorf("invalid argument: %w", flightErr)
		case errors.CodeNotFound:
			return fmt.Errorf("not found: %w", flightErr)
		case errors.CodeAlreadyExists:
			return fmt.Errorf("already exists: %w", flightErr)
		case errors.CodeUnauthorized:
			return fmt.Errorf("unauthenticated: %w", flightErr)
		case errors.CodePermissionDenied:
			return fmt.Errorf("permission denied: %w", flightErr)
		case errors.CodeDeadlineExceeded:
			return fmt.Errorf("deadline exceeded: %w", flightErr)
		case errors.CodeCanceled:
			return fmt.Errorf("canceled: %w", flightErr)
		case errors.CodeResourceExhausted:
			return fmt.Errorf("resource exhausted: %w", flightErr)
		case errors.CodeInternal:
			return fmt.Errorf("internal error: %w", flightErr)
		case errors.CodeUnavailable:
			return fmt.Errorf("unavailable: %w", flightErr)
		case errors.CodeUnimplemented:
			return fmt.Errorf("unimplemented: %w", flightErr)
		default:
			return fmt.Errorf("unknown error: %w", flightErr)
		}
	}

	// Default to internal error
	return fmt.Errorf("internal error: %w", err)
}

// truncateQuery truncates long queries for logging.
func truncateQuery(query string) string {
	const maxLen = 100
	if len(query) <= maxLen {
		return query
	}
	return query[:maxLen] + "..."
}

// createRecordBatchStream creates a stream of record batches from a query result.
func (h *queryHandler) createRecordBatchStream(ctx context.Context, result *models.QueryResult) <-chan flight.StreamChunk {
	chunks := make(chan flight.StreamChunk, 16)

	go func() {
		defer close(chunks)

		builder := array.NewRecordBuilder(h.allocator, result.Schema)
		defer builder.Release()

		batchSize := 1024
		currentSize := 0

		for record := range result.Records {
			// Check context
			if ctx.Err() != nil {
				record.Release()
				chunks <- flight.StreamChunk{Err: ctx.Err()}
				return
			}

			// Accumulate records until batch size
			for i := int64(0); i < record.NumRows(); i++ {
				for j := 0; j < int(record.NumCols()); j++ {
					col := record.Column(j)
					if err := appendValueToBuilder(builder.Field(j), col, int(i)); err != nil {
						record.Release()
						chunks <- flight.StreamChunk{Err: err}
						return
					}
				}
				currentSize++

				// Send batch if full
				if currentSize >= batchSize {
					batch := builder.NewRecord()
					chunks <- flight.StreamChunk{Data: batch}
					currentSize = 0
				}
			}

			record.Release()
		}

		// Send remaining records
		if currentSize > 0 {
			batch := builder.NewRecord()
			chunks <- flight.StreamChunk{Data: batch}
		}
	}()

	return chunks
}

// appendValueToBuilder appends a value from an array to a builder.
func appendValueToBuilder(builder array.Builder, arr arrow.Array, idx int) error {
	if arr.IsNull(idx) {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		b.Append(arr.(*array.Boolean).Value(idx))
	case *array.Int8Builder:
		b.Append(arr.(*array.Int8).Value(idx))
	case *array.Int16Builder:
		b.Append(arr.(*array.Int16).Value(idx))
	case *array.Int32Builder:
		b.Append(arr.(*array.Int32).Value(idx))
	case *array.Int64Builder:
		b.Append(arr.(*array.Int64).Value(idx))
	case *array.Uint8Builder:
		b.Append(arr.(*array.Uint8).Value(idx))
	case *array.Uint16Builder:
		b.Append(arr.(*array.Uint16).Value(idx))
	case *array.Uint32Builder:
		b.Append(arr.(*array.Uint32).Value(idx))
	case *array.Uint64Builder:
		b.Append(arr.(*array.Uint64).Value(idx))
	case *array.Float32Builder:
		b.Append(arr.(*array.Float32).Value(idx))
	case *array.Float64Builder:
		b.Append(arr.(*array.Float64).Value(idx))
	case *array.StringBuilder:
		b.Append(arr.(*array.String).Value(idx))
	case *array.BinaryBuilder:
		b.Append(arr.(*array.Binary).Value(idx))
	case *array.TimestampBuilder:
		b.Append(arr.(*array.Timestamp).Value(idx))
	case *array.Date32Builder:
		b.Append(arr.(*array.Date32).Value(idx))
	case *array.Date64Builder:
		b.Append(arr.(*array.Date64).Value(idx))
	case *array.Time32Builder:
		b.Append(arr.(*array.Time32).Value(idx))
	case *array.Time64Builder:
		b.Append(arr.(*array.Time64).Value(idx))
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}
