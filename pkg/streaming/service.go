package streaming

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MetricsCollector defines a subset of metrics collection capabilities needed by the service.
// This should align with the one in cmd/server/server/server.go or a shared package.
type MetricsCollector interface {
	IncrementCounter(name string, labels ...string)
	StartTimer(name string) Timer
	RecordHistogram(name string, value float64, labels ...string)
}

// Timer defines a simple timer interface for metrics, compatible with server.Timer.
type Timer interface {
	Stop() float64
}

// service implements the StreamingService interface.
type service struct {
	repository StreamingRepository
	logger     zerolog.Logger
	metrics    MetricsCollector
}

func getClientID(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ids := md.Get("client_id"); len(ids) > 0 {
			return ids[0]
		}
	}
	return ""
}

// NewService creates a new StreamingService.
func NewService(repository StreamingRepository, logger zerolog.Logger, metrics MetricsCollector) StreamingService {
	return &service{
		repository: repository,
		logger:     logger,
		metrics:    metrics,
	}
}

// HandleDoPut streams data into a specified target.
func (s *service) HandleDoPut(ctx context.Context, desc *flight.FlightDescriptor, schema *arrow.Schema, reader flight.MessageReader, writer flight.MetadataWriter) error {
	if desc == nil || schema == nil || reader == nil || writer == nil {
		return status.Error(codes.InvalidArgument, "invalid stream inputs")
	}

	s.logger.Debug().Fields(map[string]interface{}{"descriptor_path": desc.Path}).Msg("HandleDoPut called")

	clientID := getClientID(ctx)
	start := time.Now()
	timer := s.metrics.StartTimer("streaming_service_handle_do_put_duration_seconds")
	defer timer.Stop()
	defer func(start time.Time) {
		duration := time.Since(start).Seconds()
		s.metrics.RecordHistogram("flight_sql_rpc_duration_seconds", duration, "rpc", "DoPut")
	}(start)

	if len(desc.Path) == 0 {
		s.metrics.IncrementCounter("streaming_service_handle_do_put_errors_total", "reason", "missing_target_path")
		s.metrics.IncrementCounter("flight_sql_rpc_total", "rpc", "DoPut", "status", "error")
		s.logger.Error().Str("rpc_name", "DoPut").Str("client_id", clientID).Float64("duration_ms", time.Since(start).Seconds()*1000).Str("error_type", "missing_target_path").Msg("failed to ingest stream")
		return status.Error(codes.InvalidArgument, "missing target path in FlightDescriptor")
	}
	if schema == nil || len(schema.Fields()) == 0 {
		return status.Error(codes.InvalidArgument, "missing schema")
	}
	targetTable := desc.Path[0]

	transactionID := ""

	rowsAffected, err := s.repository.IngestStream(ctx, transactionID, targetTable, schema, reader)
	if err != nil {
		s.metrics.IncrementCounter("streaming_service_handle_do_put_errors_total", "reason", "ingestion_failed")
		s.metrics.IncrementCounter("flight_sql_rpc_total", "rpc", "DoPut", "status", "error")
		s.logger.Error().Err(err).Str("target_table", targetTable).Str("rpc_name", "DoPut").Str("client_id", clientID).Float64("duration_ms", time.Since(start).Seconds()*1000).Str("error_type", "ingestion_failed").Msg("failed to ingest stream")
		return status.Errorf(codes.Internal, "failed to ingest stream: %v", err)
	}

	s.metrics.IncrementCounter("streaming_service_handle_do_put_success_total")
	s.metrics.IncrementCounter("flight_sql_rpc_total", "rpc", "DoPut", "status", "success")
	s.logger.Info().Str("target_table", targetTable).Int64("rows_affected", rowsAffected).Str("rpc_name", "DoPut").Str("client_id", clientID).Float64("duration_ms", time.Since(start).Seconds()*1000).Int64("record_count", rowsAffected).Msg("successfully ingested stream")

	// Encode rowsAffected as the PutResult metadata
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, uint64(rowsAffected))
	if err := writer.WriteMetadata(result); err != nil {
		s.metrics.IncrementCounter("streaming_service_handle_do_put_errors_total", "reason", "write_put_result")
		s.metrics.IncrementCounter("flight_sql_rpc_total", "rpc", "DoPut", "status", "error")
		s.logger.Error().Err(err).Str("rpc_name", "DoPut").Str("client_id", clientID).Float64("duration_ms", time.Since(start).Seconds()*1000).Str("error_type", "write_put_result").Msg("failed to write PutResult")
		return status.Errorf(codes.Internal, "failed to write PutResult: %v", err)
	}

	return nil
}
