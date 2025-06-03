// Package handlers contains Flight SQL protocol handlers.
package handlers

import (
	"context"
	stdErrors "errors" // Standard library errors aliased
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"

	flightErrors "github.com/TFMV/hatch/pkg/errors"
	"github.com/TFMV/hatch/pkg/infrastructure/pool"
	"github.com/TFMV/hatch/pkg/models"
	"github.com/TFMV/hatch/pkg/services"
	flightpb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
)

// queryHandler implements QueryHandler interface.
type queryHandler struct {
	queryService services.QueryService
	allocator    memory.Allocator
	logger       Logger
	metrics      MetricsCollector
	recordPool   *pool.FastRecordPool
	schemaCache  *pool.SchemaCache
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
		recordPool:   pool.NewFastRecordPool(allocator),
		schemaCache:  pool.NewSchemaCache(100), // Cache up to 100 schemas
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
		return nil, nil, flightErrors.New(flightErrors.CodeInternal, "query returned no schema")
	}

	// Try to get schema from cache
	if cached, ok := h.schemaCache.Get(schema); ok {
		h.metrics.IncrementCounter("handler_schema_cache_hit")
		schema = cached
	} else {
		h.metrics.IncrementCounter("handler_schema_cache_miss")
		h.schemaCache.Put(schema)
	}

	// Create stream for results
	chunks := make(chan flight.StreamChunk, 16)

	// Start streaming results in background
	go func() {
		defer close(chunks)

		recordCount := 0
		for record := range result.Records {
			if record == nil {
				continue
			}

			// Get a pooled record and copy data
			pooled := h.recordPool.Get(record.Schema())
			// TODO: Implement efficient data copy from record to pooled
			// For now, just use the original record

			select {
			case <-ctx.Done():
				h.logger.Warn("Query streaming cancelled", "records_sent", recordCount)
				record.Release()
				pooled.Release()
				return
			case chunks <- flight.StreamChunk{Data: record}:
				recordCount++
				pooled.Release() // Release the unused pooled record for now
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
func (h *queryHandler) GetFlightInfo(ctx context.Context, query string) (*flightpb.FlightInfo, error) {
	ttimer := h.metrics.StartTimer("handler_get_flight_info")
	defer ttimer.Stop()

	h.logger.Debug("Getting flight info", "query", truncateQuery(query))

	if err := h.queryService.ValidateQuery(ctx, query); err != nil {
		h.metrics.IncrementCounter("handler_validation_errors")
		return nil, h.mapServiceError(err)
	}

	// Execute a dummy query to get the schema
	req := &models.QueryRequest{
		Query:   query,
		MaxRows: 0, // We only need the schema
	}
	result, err := h.queryService.ExecuteQuery(ctx, req)
	if err != nil {
		h.logger.Error("Failed to get schema", "error", err)
		h.metrics.IncrementCounter("handler_internal_errors")
		return nil, h.mapServiceError(err)
	}

	// Get Arrow schema from result
	schema := result.Schema
	if schema == nil {
		h.metrics.IncrementCounter("handler_internal_errors")
		return nil, h.mapServiceError(flightErrors.New(flightErrors.CodeInternal, "schema is nil"))
	}

	// Try to get schema from cache
	if cached, ok := h.schemaCache.Get(schema); ok {
		h.metrics.IncrementCounter("handler_schema_cache_hit")
		schema = cached
	} else {
		h.metrics.IncrementCounter("handler_schema_cache_miss")
		h.schemaCache.Put(schema)
	}

	// Create a FlightInfo with the schema
	return &flightpb.FlightInfo{
		Schema: flight.SerializeSchema(schema, h.allocator),
		Endpoint: []*flightpb.FlightEndpoint{{
			Ticket: &flightpb.Ticket{},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// mapServiceError maps service errors to appropriate Flight errors.
func (h *queryHandler) mapServiceError(err error) error {
	if err == nil {
		return nil
	}

	// Check if it's a FlightError
	var flightErr *flightErrors.FlightError
	if stdErrors.As(err, &flightErr) { // Use aliased standard errors for As
		// Return a new error with just the message content from the FlightError.
		// This avoids the gRPC layer trying to serialize FlightError.Details into a proto.Any
		// if the client isn't equipped to handle custom proto detail types.
		// The specific error code is implicitly mapped by gRPC status codes if this handler
		// is called from a gRPC context that translates errors to statuses.
		switch flightErr.Code {
		case flightErrors.CodeInvalidRequest:
			return fmt.Errorf("invalid argument: %s", flightErr.Message)
		case flightErrors.CodeNotFound:
			return fmt.Errorf("not found: %s", flightErr.Message)
		case flightErrors.CodeAlreadyExists:
			return fmt.Errorf("already exists: %s", flightErr.Message)
		case flightErrors.CodeUnauthorized:
			return fmt.Errorf("unauthenticated: %s", flightErr.Message)
		case flightErrors.CodePermissionDenied:
			return fmt.Errorf("permission denied: %s", flightErr.Message)
		case flightErrors.CodeDeadlineExceeded:
			return fmt.Errorf("deadline exceeded: %s", flightErr.Message)
		case flightErrors.CodeCanceled:
			return fmt.Errorf("canceled: %s", flightErr.Message)
		case flightErrors.CodeResourceExhausted:
			return fmt.Errorf("resource exhausted: %s", flightErr.Message)
		case flightErrors.CodeInternal:
			return fmt.Errorf("internal error: %s", flightErr.Message)
		case flightErrors.CodeUnavailable:
			return fmt.Errorf("unavailable: %s", flightErr.Message)
		case flightErrors.CodeUnimplemented:
			return fmt.Errorf("unimplemented: %s", flightErr.Message)
		default:
			// For unknown FlightError codes, still return a generic message.
			return fmt.Errorf("unknown error: %s", flightErr.Message)
		}
	}

	// For non-FlightError types, return a generic internal error message, including the original error text.
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

// ExecuteQueryAndStream executes a query and returns its schema and a channel of StreamChunks.
func (h *queryHandler) ExecuteQueryAndStream(ctx context.Context, query string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_execute_query_and_stream")
	defer timer.Stop()

	h.logger.Debug("Executing query for streaming", "query", truncateQuery(query))

	// Create query request
	req := &models.QueryRequest{
		Query: query,
	}

	// Execute query
	queryResult, err := h.queryService.ExecuteQuery(ctx, req)
	if err != nil {
		h.metrics.IncrementCounter("handler_query_errors")
		h.logger.Error("Failed to execute query", "error", err)
		return nil, nil, h.mapServiceError(err)
	}

	// Get Arrow schema from result
	schema := queryResult.Schema
	if schema == nil {
		h.metrics.IncrementCounter("handler_internal_errors")
		return nil, nil, h.mapServiceError(flightErrors.New(flightErrors.CodeInternal, "schema is nil"))
	}

	// Create output channel for stream chunks
	outCh := make(chan flight.StreamChunk, 16)

	// Start goroutine to stream records
	go func() {
		defer close(outCh)

		recordCount := 0
		for record := range queryResult.Records {
			if record == nil {
				continue
			}

			// Get a pooled record
			pooled := h.recordPool.Get(record.Schema())

			// Create a copy of the record using zero-copy slicing
			// This is more efficient than NewSlice as it uses the pool
			h.recordPool.Put(pooled) // Return the unused pooled record
			recordCopy := array.NewRecord(
				record.Schema(),
				record.Columns(),
				record.NumRows(),
			)

			if recordCopy.NumRows() > 0 {
				select {
				case outCh <- flight.StreamChunk{Data: recordCopy}:
					recordCount++
					h.logger.Debug("Sent record chunk to stream",
						"rows", recordCopy.NumRows(),
						"count", recordCount)
				case <-ctx.Done():
					h.logger.Info("Context cancelled during chunk send",
						"error", ctx.Err(),
						"records_sent", recordCount)
					recordCopy.Release()
					return
				}
			} else {
				recordCopy.Release()
			}
		}

		h.logger.Info("Query streaming completed",
			"records_sent", recordCount)
		h.metrics.RecordHistogram("handler_stream_records", float64(recordCount))
	}()

	return schema, outCh, nil
}
