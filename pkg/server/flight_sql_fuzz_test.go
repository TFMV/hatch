package server

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FuzzFlightSQLServer exercises FlightSQL server methods with random inputs to
// ensure robustness against malformed requests.
func FuzzFlightSQLServer(f *testing.F) {
	f.Add([]byte("SELECT 1"))
	f.Fuzz(func(t *testing.T, data []byte) {
		server, queryHandler, metadataHandler, _, psHandler := setupTestServer(t)
		allocator := memory.NewGoAllocator()

		// safe defaults for handlers
		queryHandler.getFlightInfoFunc = func(ctx context.Context, query string) (*flight.FlightInfo, error) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Int32}}, nil)
			desc := &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte(query)}
			return &flight.FlightInfo{
				Schema:           flight.SerializeSchema(schema, allocator),
				FlightDescriptor: desc,
				Endpoint: []*flight.FlightEndpoint{{
					Ticket: &flight.Ticket{Ticket: desc.Cmd},
				}},
				TotalRecords: -1,
				TotalBytes:   -1,
			}, nil
		}
		queryHandler.executeStatementFunc = func(ctx context.Context, query string, txnID string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Int32}}, nil)
			ch := make(chan flight.StreamChunk)
			close(ch)
			return schema, ch, nil
		}
		metadataHandler.getSqlInfoFunc = func(ctx context.Context, info []uint32) (*arrow.Schema, <-chan flight.StreamChunk, error) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int32}}, nil)
			ch := make(chan flight.StreamChunk)
			close(ch)
			return schema, ch, nil
		}
		metadataHandler.getXdbcTypeInfoFunc = func(ctx context.Context, dt *int32) (*arrow.Schema, <-chan flight.StreamChunk, error) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int32}}, nil)
			ch := make(chan flight.StreamChunk)
			close(ch)
			return schema, ch, nil
		}

		psHandler.createFunc = func(ctx context.Context, q string, tx string) (string, *arrow.Schema, error) {
			return "h", arrow.NewSchema(nil, nil), nil
		}
		psHandler.closeFunc = func(ctx context.Context, h string) error { return nil }
		psHandler.getSchemaFunc = func(ctx context.Context, h string) (*arrow.Schema, error) {
			return arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil), nil
		}
		psHandler.setParametersFunc = func(ctx context.Context, h string, p arrow.Record) error { return nil }
		psHandler.executeUpdateFunc = func(ctx context.Context, h string, p arrow.Record) (int64, error) { return 0, nil }

		ctx := context.Background()
		query := string(data)

		if _, err := server.GetFlightInfoStatement(ctx, &statementQuery{query: query}, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: data}); err != nil {
			t.Errorf("GetFlightInfoStatement error: %v", err)
		}
		if _, _, err := server.DoGetStatement(ctx, &statementQueryTicket{handle: data}); err != nil {
			t.Errorf("DoGetStatement error: %v", err)
		}

		schema1 := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
		b1 := array.NewRecordBuilder(allocator, schema1)
		b1.Field(0).(*array.Int64Builder).Append(1)
		rec1 := b1.NewRecord()
		b1.Release()

		// Use the same schema as rec1 to avoid schema mismatch
		b2 := array.NewRecordBuilder(allocator, schema1)
		b2.Field(0).(*array.Int64Builder).Append(2)
		rec2 := b2.NewRecord()
		b2.Release()

		reader := &sliceMessageReader{records: []arrow.Record{rec1, rec2}}
		writer := nopMetadataWriter{}
		if _, err := server.DoPutPreparedStatementQuery(ctx, &preparedStatementQueryCmd{handle: data}, reader, writer); err != nil {
			t.Errorf("DoPutPreparedStatementQuery error: %v", err)
		}

		reader = &sliceMessageReader{records: []arrow.Record{rec1}}
		if _, err := server.DoPutPreparedStatementUpdate(ctx, &preparedStatementUpdateCmd{handle: data}, reader); err != nil {
			t.Errorf("DoPutPreparedStatementUpdate error: %v", err)
		}

		rec1.Release()
		rec2.Release()
	})
}
