package streaming

import (
	"context"
	"encoding/binary"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MetricsCollector defines a subset of metrics collection capabilities needed by the service.
// This should align with the one in cmd/server/server/server.go or a shared package.
type MetricsCollector interface {
	IncrementCounter(name string, labels ...string)
	StartTimer(name string) Timer
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
	s.logger.Debug().Fields(map[string]interface{}{"descriptor_path": desc.Path}).Msg("HandleDoPut called")

	timer := s.metrics.StartTimer("streaming_service_handle_do_put_duration_seconds")
	defer timer.Stop() // TODO:The duration is returned and could be recorded by an adapter if needed.

	if len(desc.Path) == 0 {
		s.metrics.IncrementCounter("streaming_service_handle_do_put_errors_total", "reason", "missing_target_path")
		return status.Error(codes.InvalidArgument, "missing target path in FlightDescriptor")
	}
	targetTable := desc.Path[0]

	transactionID := ""

	rowsAffected, err := s.repository.IngestStream(ctx, transactionID, targetTable, schema, reader)
	if err != nil {
		s.logger.Error().Err(err).Str("target_table", targetTable).Msg("failed to ingest stream")
		s.metrics.IncrementCounter("streaming_service_handle_do_put_errors_total", "reason", "ingestion_failed")
		return status.Errorf(codes.Internal, "failed to ingest stream: %v", err)
	}

	s.logger.Info().Str("target_table", targetTable).Int64("rows_affected", rowsAffected).Msg("successfully ingested stream")
	s.metrics.IncrementCounter("streaming_service_handle_do_put_success_total")

	// Encode rowsAffected as the PutResult metadata
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, uint64(rowsAffected))
	if err := writer.WriteMetadata(result); err != nil {
		s.logger.Error().Err(err).Msg("failed to write PutResult")
		s.metrics.IncrementCounter("streaming_service_handle_do_put_errors_total", "reason", "write_put_result")
		return status.Errorf(codes.Internal, "failed to write PutResult: %v", err)
	}

	return nil
}
