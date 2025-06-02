package streaming

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

// StreamingService defines the interface for handling streaming data operations.
type StreamingService interface {
	// HandleDoPut streams data into a specified target.
	// The desc parameter provides context, including potentially the target identifier.
	// reader is the stream of Arrow RecordBatches.
	// writer is used to send back metadata/results (e.g., rows written or error messages).
	HandleDoPut(ctx context.Context, desc *flight.FlightDescriptor, schema *arrow.Schema, reader flight.MessageReader, writer flight.MetadataWriter) error
}

// StreamingRepository defines the interface for ingesting streaming data into the database.
type StreamingRepository interface {
	// IngestStream ingests Arrow RecordBatches into the database.
	// transactionID is the optional transaction identifier if the operation is part of a transaction.
	// target is a generic identifier for the data destination (e.g., table name).
	// schema is the schema of the incoming data.
	// reader provides the stream of RecordBatches.
	// Returns the number of records ingested and any error.
	IngestStream(ctx context.Context, transactionID string, target string, schema *arrow.Schema, reader flight.MessageReader) (int64, error)
}
