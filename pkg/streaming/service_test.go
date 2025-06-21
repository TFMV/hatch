package streaming

import (
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ---- mocks ----
type mockRepo struct {
	ingestFunc func(ctx context.Context, txn, tgt string, sch *arrow.Schema, reader flight.MessageReader) (int64, error)
}

func (m *mockRepo) IngestStream(ctx context.Context, txn, tgt string, sch *arrow.Schema, reader flight.MessageReader) (int64, error) {
	return m.ingestFunc(ctx, txn, tgt, sch, reader)
}

type nopTimer struct{}

func (nopTimer) Stop() float64 { return 0 }

type mockMetrics struct{}

func (mockMetrics) IncrementCounter(name string, labels ...string)               {}
func (mockMetrics) StartTimer(name string) Timer                                 { return nopTimer{} }
func (mockMetrics) RecordHistogram(name string, value float64, labels ...string) {}

type nopReader struct{}

func (nopReader) Read() (arrow.Record, error)                      { return nil, io.EOF }
func (nopReader) Schema() *arrow.Schema                            { return nil }
func (nopReader) Next() bool                                       { return false }
func (nopReader) Record() arrow.Record                             { return nil }
func (nopReader) Err() error                                       { return nil }
func (nopReader) Chunk() flight.StreamChunk                        { return flight.StreamChunk{} }
func (nopReader) LatestFlightDescriptor() *flight.FlightDescriptor { return nil }
func (nopReader) LatestAppMetadata() []byte                        { return nil }
func (nopReader) Release()                                         {}
func (nopReader) Retain()                                          {}

type nopWriter struct{ meta [][]byte }

func (w *nopWriter) WriteMetadata(b []byte) error { w.meta = append(w.meta, b); return nil }

type errWriter struct{}

func (errWriter) WriteMetadata([]byte) error { return io.ErrClosedPipe }

// ---- tests ----
func TestGetClientID(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("client_id", "abc"))
	require.Equal(t, "abc", getClientID(ctx))
	require.Equal(t, "", getClientID(context.Background()))
}

func TestArrowToDuckDBType(t *testing.T) {
	repo := &duckDBStreamingRepository{logger: zerolog.Nop()}
	dt := &arrow.Decimal256Type{Precision: 40, Scale: 2}
	require.Equal(t, "DECIMAL(38, 2)", repo.arrowToDuckDBType(dt))

	unknown := repo.arrowToDuckDBType(&arrow.DictionaryType{})
	require.Equal(t, "BLOB", unknown)
}

func TestGenerateCreateTableStmt(t *testing.T) {
	repo := &duckDBStreamingRepository{logger: zerolog.Nop()}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	stmt := repo.generateCreateTableStmt(repo.quoteIdentifier("t"), schema)
	require.Equal(t, "CREATE TABLE IF NOT EXISTS \"t\" (\"id\" BIGINT NOT NULL, \"name\" VARCHAR)", stmt)
}

func TestHandleDoPut(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "c", Type: arrow.PrimitiveTypes.Int64}}, nil)

	t.Run("missing path", func(t *testing.T) {
		svc := service{metrics: mockMetrics{}, logger: zerolog.Nop()}
		err := svc.HandleDoPut(context.Background(), &flight.FlightDescriptor{}, schema, nopReader{}, &nopWriter{})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("missing schema", func(t *testing.T) {
		svc := service{metrics: mockMetrics{}, logger: zerolog.Nop()}
		desc := &flight.FlightDescriptor{Path: []string{"t"}}
		err := svc.HandleDoPut(context.Background(), desc, nil, nopReader{}, &nopWriter{})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("repo error", func(t *testing.T) {
		repo := &mockRepo{ingestFunc: func(ctx context.Context, txn, tgt string, sch *arrow.Schema, reader flight.MessageReader) (int64, error) {
			return 0, io.ErrUnexpectedEOF
		}}
		svc := service{repository: repo, metrics: mockMetrics{}, logger: zerolog.Nop()}
		desc := &flight.FlightDescriptor{Path: []string{"t"}}
		err := svc.HandleDoPut(context.Background(), desc, schema, nopReader{}, &nopWriter{})
		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("writer error", func(t *testing.T) {
		repo := &mockRepo{ingestFunc: func(ctx context.Context, txn, tgt string, sch *arrow.Schema, reader flight.MessageReader) (int64, error) {
			return 1, nil
		}}
		writer := &errWriter{}
		svc := service{repository: repo, metrics: mockMetrics{}, logger: zerolog.Nop()}
		desc := &flight.FlightDescriptor{Path: []string{"t"}}
		err := svc.HandleDoPut(context.Background(), desc, schema, nopReader{}, writer)
		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("success", func(t *testing.T) {
		repo := &mockRepo{ingestFunc: func(ctx context.Context, txn, tgt string, sch *arrow.Schema, reader flight.MessageReader) (int64, error) {
			return 2, nil
		}}
		w := &nopWriter{}
		svc := service{repository: repo, metrics: mockMetrics{}, logger: zerolog.Nop()}
		desc := &flight.FlightDescriptor{Path: []string{"t"}}
		err := svc.HandleDoPut(context.Background(), desc, schema, nopReader{}, w)
		require.NoError(t, err)
		require.Len(t, w.meta, 1)
		require.Equal(t, uint64(2), binary.LittleEndian.Uint64(w.meta[0]))
	})
}
