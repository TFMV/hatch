// Package handlers contains Flight SQL protocol handlers.
package handlers

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/TFMV/flight/pkg/errors"
	"github.com/TFMV/flight/pkg/models"
	"github.com/TFMV/flight/pkg/services"
	flightpb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
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
func (h *queryHandler) GetFlightInfo(ctx context.Context, query string) (*flightpb.FlightInfo, error) {
	ttimer := h.metrics.StartTimer("handler_get_flight_info")
	defer ttimer.Stop()

	h.logger.Debug("Getting flight info", "query", truncateQuery(query))

	if err := h.queryService.ValidateQuery(ctx, query); err != nil {
		h.metrics.IncrementCounter("handler_validation_errors")
		return nil, h.mapServiceError(err)
	}

	// Create a TicketStatementQuery protobuf message
	statementQueryTicketProto := &flightpb.TicketStatementQuery{
		StatementHandle: []byte(query),
	}
	// Marshal the TicketStatementQuery for the ticket's content
	marshalledTicketStatementQueryBytes, err := proto.Marshal(statementQueryTicketProto)
	if err != nil {
		h.logger.Error("Failed to marshal TicketStatementQuery", "error", err)
		h.metrics.IncrementCounter("handler_internal_errors")
		return nil, errors.New(errors.CodeInternal, fmt.Sprintf("failed to marshal ticket statement query: %v", err))
	}

	ticket := &flightpb.Ticket{
		Ticket: marshalledTicketStatementQueryBytes,
	}

	endpoint := &flightpb.FlightEndpoint{
		Ticket:   ticket,
		Location: []*flightpb.Location{},
	}

	descriptor := &flightpb.FlightDescriptor{
		Type: flightpb.FlightDescriptor_CMD,
		// Cmd field will be set below with CommandStatementQuery
	}

	cmdPayload := &flightpb.CommandStatementQuery{
		Query: query,
	}
	cmdBytes, err := proto.Marshal(cmdPayload)
	if err != nil {
		h.logger.Error("Failed to marshal CommandStatementQuery", "error", err)
		h.metrics.IncrementCounter("handler_internal_errors")
		return nil, errors.New(errors.CodeInternal, fmt.Sprintf("failed to marshal command payload: %v", err))
	}
	descriptor.Cmd = cmdBytes

	info := &flightpb.FlightInfo{
		Schema:           nil,
		FlightDescriptor: descriptor,
		Endpoint:         []*flightpb.FlightEndpoint{endpoint},
		TotalRecords:     -1,
		TotalBytes:       -1,
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

// ExecuteQueryAndStream executes a query and returns its schema and a channel of StreamChunks.
func (h *queryHandler) ExecuteQueryAndStream(ctx context.Context, query string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_execute_query_and_stream")
	defer timer.Stop()

	h.logger.Debug("Executing query for streaming", "query", truncateQuery(query))

	req := &models.QueryRequest{
		Query: query,
		// TransactionID can be added if necessary from context or other means
	}
	queryResult, err := h.queryService.ExecuteQuery(ctx, req)
	if err != nil {
		h.logger.Error("Failed to execute query for streaming", "error", err, "query", truncateQuery(query))
		return nil, nil, h.mapServiceError(err)
	}

	if queryResult.Schema == nil {
		h.metrics.IncrementCounter("handler_query_no_schema")
		return nil, nil, errors.New(errors.CodeInternal, "query executed but returned no schema")
	}

	outCh := make(chan flight.StreamChunk) // Unbuffered, or buffered if a small number of records is typical

	go func() {
		defer close(outCh)

		// queryResult.Records is <-chan arrow.Record
		for record := range queryResult.Records { // Corrected loop for channel
			currentRecord := record       // Capture range variable for the goroutine
			defer currentRecord.Release() // Release each record after processing

			if currentRecord != nil && currentRecord.NumRows() > 0 {
				// Send the chunk
				select {
				case outCh <- flight.StreamChunk{Data: currentRecord}: // Send the arrow.Record directly
					h.logger.Debug("Sent record chunk to stream", "rows", currentRecord.NumRows())
				case <-ctx.Done():
					h.logger.Info("Context cancelled during chunk send", "error", ctx.Err())
					return
				}
			} else {
				h.logger.Debug("Query record is nil or empty, skipping stream send")
			}
		}
		// After the loop, all records from the channel have been processed or the channel was closed.
		h.logger.Debug("Finished processing records channel for streaming")
	}()

	h.metrics.IncrementCounter("handler_query_stream_started")
	return queryResult.Schema, outCh, nil
}
