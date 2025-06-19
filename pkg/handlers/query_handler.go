// Package handlers contains Flight SQL protocol handlers.
package handlers

import (
	"context"
	stdErrors "errors" // Standard library errors aliased
	"fmt"
	"runtime"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	flightErrors "github.com/TFMV/porter/pkg/errors"
	"github.com/TFMV/porter/pkg/infrastructure/pool"
	"github.com/TFMV/porter/pkg/models"
	"github.com/TFMV/porter/pkg/services"
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

	// Create buffered channel for results
	chunks := make(chan flight.StreamChunk, 16)

	// Start streaming results in background
	go func() {
		defer close(chunks)

		recordCount := 0
		for record := range result.Records {
			if record == nil {
				continue
			}

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
func (h *queryHandler) GetFlightInfo(ctx context.Context, query string) (*flightpb.FlightInfo, error) {
	timer := h.metrics.StartTimer("handler_get_flight_info")
	defer timer.Stop()

	h.logger.Debug("Getting flight info", "query", truncateQuery(query))
	h.logger.Info("GetFlightInfo called with query", "query", query)

	if err := h.queryService.ValidateQuery(ctx, query); err != nil {
		h.metrics.IncrementCounter("handler_validation_errors")
		h.logger.Error("Query validation failed", "error", err, "query", query)
		return nil, h.mapServiceError(err)
	}

	// Execute a dummy query to get the schema
	req := &models.QueryRequest{
		Query:   query,
		MaxRows: 0, // We only need the schema
	}
	result, err := h.queryService.ExecuteQuery(ctx, req)
	if err != nil {
		h.logger.Error("Failed to get schema", "error", err, "query", query)
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

	// Build descriptor and ticket following Flight SQL specification
	cmd := &flightpb.CommandStatementQuery{Query: query}

	// Marshal the command into Any protobuf for the descriptor
	var cmdAny anypb.Any
	if err := cmdAny.MarshalFrom(cmd); err != nil {
		return nil, h.mapServiceError(err)
	}

	// Serialize the Any protobuf as command bytes
	cmdBytes, err := proto.Marshal(&cmdAny)
	if err != nil {
		return nil, h.mapServiceError(err)
	}

	// For the ticket, we need to create a TicketStatementQuery with the query as the statement handle
	// The statement handle should contain the original query, not the protobuf command
	ticketStmt := &flightpb.TicketStatementQuery{StatementHandle: []byte(query)}

	// Marshal the ticket statement into Any protobuf
	var ticketAny anypb.Any
	if err := ticketAny.MarshalFrom(ticketStmt); err != nil {
		return nil, h.mapServiceError(err)
	}

	// Serialize the ticket Any protobuf
	ticketBytes, err := proto.Marshal(&ticketAny)
	if err != nil {
		return nil, h.mapServiceError(err)
	}

	// Create ticket with the properly formatted ticket bytes
	ticket := &flightpb.Ticket{Ticket: ticketBytes}

	return &flightpb.FlightInfo{
		Schema: flight.SerializeSchema(schema, h.allocator),
		FlightDescriptor: &flightpb.FlightDescriptor{
			Type: flightpb.FlightDescriptor_CMD,
			Cmd:  cmdBytes,
		},
		Endpoint: []*flightpb.FlightEndpoint{{
			Ticket: ticket,
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

	// Try to get schema from cache
	if cached, ok := h.schemaCache.Get(schema); ok {
		h.metrics.IncrementCounter("handler_schema_cache_hit")
		schema = cached
	} else {
		h.metrics.IncrementCounter("handler_schema_cache_miss")
		h.schemaCache.Put(schema)
	}

	// Create output channel for stream chunks with adaptive buffer size
	outCh := make(chan flight.StreamChunk, 32) // Increased buffer for better throughput

	// Start goroutine to stream records
	go func() {
		defer close(outCh)

		var (
			recordCount int
			batchSize   int = 1024 // Start with reasonable batch size
			lastGC      time.Time
		)

		// Get initial pooled record
		pooled := h.recordPool.Get(schema)
		if pooled == nil {
			h.logger.Error("Failed to get pooled record")
			return
		}
		defer pooled.Release()

		for record := range queryResult.Records {
			if record == nil {
				continue
			}

			// Check memory pressure periodically
			if time.Since(lastGC) > 5*time.Second {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)

				// Adjust batch size based on memory pressure
				if m.HeapAlloc > m.HeapSys*3/4 { // Over 75% heap usage
					batchSize = max(1024, batchSize/2)
					runtime.GC() // Force GC to reclaim memory
				} else if m.HeapAlloc < m.HeapSys/2 { // Under 50% heap usage
					batchSize = min(32768, batchSize*2)
				}
				lastGC = time.Now()
			}

			// Create a copy of the record using the pool
			recordCopy := array.NewRecord(
				schema,
				record.Columns(),
				record.NumRows(),
			)

			if recordCopy.NumRows() > 0 {
				select {
				case outCh <- flight.StreamChunk{Data: recordCopy}:
					recordCount++
					h.metrics.RecordHistogram("handler_record_rows", float64(recordCopy.NumRows()))
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

			// Return the pooled record
			h.recordPool.Put(pooled)
			pooled = h.recordPool.Get(schema)
			if pooled == nil {
				h.logger.Error("Failed to get pooled record")
				return
			}
		}

		h.logger.Info("Query streaming completed",
			"records_sent", recordCount)
		h.metrics.RecordHistogram("handler_stream_records", float64(recordCount))

		// Get pool stats
		stats := h.recordPool.Stats()
		h.metrics.RecordHistogram("handler_pool_hits", float64(stats.Hits))
		h.metrics.RecordHistogram("handler_pool_misses", float64(stats.Misses))
		h.metrics.RecordHistogram("handler_pool_allocs", float64(stats.Allocs))
	}()

	return schema, outCh, nil
}

// Helper functions for min/max
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ExecuteFromTicket executes a query from a Flight ticket.
func (h *queryHandler) ExecuteFromTicket(ctx context.Context, ticket []byte) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_execute_from_ticket")
	defer timer.Stop()

	// Extract query from ticket
	query := string(ticket)
	if query == "" {
		h.metrics.IncrementCounter("handler_empty_ticket")
		return nil, nil, flightErrors.New(flightErrors.CodeInvalidRequest, "empty ticket")
	}

	h.logger.Debug("Executing query from ticket", "query", truncateQuery(query))

	// Execute the query
	return h.ExecuteStatement(ctx, query, "")
}

// IsUpdateStatement returns true if the statement should return an update count.
func (h *queryHandler) IsUpdateStatement(query string) bool {
	return h.queryService.IsUpdateStatement(query)
}

// IsQueryStatement returns true if the statement should return a result set.
func (h *queryHandler) IsQueryStatement(query string) bool {
	return h.queryService.IsQueryStatement(query)
}
