package server

import (
	"context"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TFMV/porter/pkg/cache"
	"github.com/TFMV/porter/pkg/handlers"
	"github.com/TFMV/porter/pkg/infrastructure/metrics"
	"github.com/TFMV/porter/pkg/models"
)

// Mock implementations for dependencies
type mockQueryHandler struct {
	handlers.QueryHandler
	getFlightInfoFunc    func(ctx context.Context, query string) (*flight.FlightInfo, error)
	executeStatementFunc func(ctx context.Context, query string, txnID string) (*arrow.Schema, <-chan flight.StreamChunk, error)
	executeUpdateFunc    func(ctx context.Context, query string, txnID string) (int64, error)
}

func (m *mockQueryHandler) GetFlightInfo(ctx context.Context, query string) (*flight.FlightInfo, error) {
	return m.getFlightInfoFunc(ctx, query)
}

func (m *mockQueryHandler) ExecuteStatement(ctx context.Context, query string, txnID string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return m.executeStatementFunc(ctx, query, txnID)
}

func (m *mockQueryHandler) ExecuteUpdate(ctx context.Context, query string, txnID string) (int64, error) {
	return m.executeUpdateFunc(ctx, query, txnID)
}

type mockMetadataHandler struct {
	handlers.MetadataHandler
	getCatalogsFunc     func(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error)
	getSchemasFunc      func(ctx context.Context, catalog *string, schemaPattern *string) (*arrow.Schema, <-chan flight.StreamChunk, error)
	getTablesFunc       func(ctx context.Context, catalog *string, schemaPattern *string, tablePattern *string, tableTypes []string, includeSchema bool) (*arrow.Schema, <-chan flight.StreamChunk, error)
	getColumnsFunc      func(ctx context.Context, catalog *string, schemaPattern *string, tablePattern *string, columnPattern *string) (*arrow.Schema, <-chan flight.StreamChunk, error)
	getTableTypesFunc   func(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error)
	getPrimaryKeysFunc  func(ctx context.Context, catalog *string, schema *string, table string) (*arrow.Schema, <-chan flight.StreamChunk, error)
	getSqlInfoFunc      func(ctx context.Context, info []uint32) (*arrow.Schema, <-chan flight.StreamChunk, error)
	getXdbcTypeInfoFunc func(ctx context.Context, dataType *int32) (*arrow.Schema, <-chan flight.StreamChunk, error)
}

func (m *mockMetadataHandler) GetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return m.getCatalogsFunc(ctx)
}

func (m *mockMetadataHandler) GetSchemas(ctx context.Context, catalog *string, schemaPattern *string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return m.getSchemasFunc(ctx, catalog, schemaPattern)
}

func (m *mockMetadataHandler) GetTables(ctx context.Context, catalog *string, schemaPattern *string, tablePattern *string, tableTypes []string, includeSchema bool) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return m.getTablesFunc(ctx, catalog, schemaPattern, tablePattern, tableTypes, includeSchema)
}

func (m *mockMetadataHandler) GetColumns(ctx context.Context, catalog *string, schemaPattern *string, tablePattern *string, columnPattern *string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return m.getColumnsFunc(ctx, catalog, schemaPattern, tablePattern, columnPattern)
}

func (m *mockMetadataHandler) GetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return m.getTableTypesFunc(ctx)
}

func (m *mockMetadataHandler) GetPrimaryKeys(ctx context.Context, catalog *string, schema *string, table string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return m.getPrimaryKeysFunc(ctx, catalog, schema, table)
}

func (m *mockMetadataHandler) GetSqlInfo(ctx context.Context, info []uint32) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return m.getSqlInfoFunc(ctx, info)
}

func (m *mockMetadataHandler) GetXdbcTypeInfo(ctx context.Context, dataType *int32) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return m.getXdbcTypeInfoFunc(ctx, dataType)
}

type mockTransactionHandler struct {
	handlers.TransactionHandler
	beginFunc    func(ctx context.Context, readOnly bool) (string, error)
	commitFunc   func(ctx context.Context, txnID string) error
	rollbackFunc func(ctx context.Context, txnID string) error
}

func (m *mockTransactionHandler) Begin(ctx context.Context, readOnly bool) (string, error) {
	return m.beginFunc(ctx, readOnly)
}

func (m *mockTransactionHandler) Commit(ctx context.Context, txnID string) error {
	return m.commitFunc(ctx, txnID)
}

func (m *mockTransactionHandler) Rollback(ctx context.Context, txnID string) error {
	return m.rollbackFunc(ctx, txnID)
}

type mockPreparedStatementHandler struct {
	handlers.PreparedStatementHandler
	createFunc        func(ctx context.Context, query string, txnID string) (string, *arrow.Schema, error)
	closeFunc         func(ctx context.Context, handle string) error
	getSchemaFunc     func(ctx context.Context, handle string) (*arrow.Schema, error)
	setParametersFunc func(ctx context.Context, handle string, params arrow.Record) error
	executeUpdateFunc func(ctx context.Context, handle string, params arrow.Record) (int64, error)
}

func (m *mockPreparedStatementHandler) Create(ctx context.Context, query string, txnID string) (string, *arrow.Schema, error) {
	return m.createFunc(ctx, query, txnID)
}

func (m *mockPreparedStatementHandler) Close(ctx context.Context, handle string) error {
	return m.closeFunc(ctx, handle)
}

func (m *mockPreparedStatementHandler) GetSchema(ctx context.Context, handle string) (*arrow.Schema, error) {
	return m.getSchemaFunc(ctx, handle)
}

func (m *mockPreparedStatementHandler) SetParameters(ctx context.Context, handle string, params arrow.Record) error {
	return m.setParametersFunc(ctx, handle, params)
}

func (m *mockPreparedStatementHandler) ExecuteUpdate(ctx context.Context, handle string, params arrow.Record) (int64, error) {
	return m.executeUpdateFunc(ctx, handle, params)
}

type mockCache struct {
	cache.Cache
	getFunc func(ctx context.Context, key string) (arrow.Record, error)
	putFunc func(ctx context.Context, key string, record arrow.Record) error
}

func (m *mockCache) Get(ctx context.Context, key string) (arrow.Record, error) {
	return m.getFunc(ctx, key)
}

func (m *mockCache) Put(ctx context.Context, key string, record arrow.Record) error {
	return m.putFunc(ctx, key, record)
}

type mockCacheKeyGenerator struct {
	cache.CacheKeyGenerator
	generateKeyFunc func(query string, params map[string]interface{}) string
}

func (m *mockCacheKeyGenerator) GenerateKey(query string, params map[string]interface{}) string {
	return m.generateKeyFunc(query, params)
}

type mockMetricsCollector struct {
	metrics.Collector
	incrementCounterFunc func(name string, labels ...string)
	recordHistogramFunc  func(name string, value float64, labels ...string)
	recordGaugeFunc      func(name string, value float64, labels ...string)
	startTimerFunc       func(name string) metrics.Timer
}

func (m *mockMetricsCollector) IncrementCounter(name string, labels ...string) {
	m.incrementCounterFunc(name, labels...)
}

func (m *mockMetricsCollector) RecordHistogram(name string, value float64, labels ...string) {
	m.recordHistogramFunc(name, value, labels...)
}

func (m *mockMetricsCollector) RecordGauge(name string, value float64, labels ...string) {
	m.recordGaugeFunc(name, value, labels...)
}

func (m *mockMetricsCollector) StartTimer(name string) metrics.Timer {
	return m.startTimerFunc(name)
}

func setupTestServer(t *testing.T) (*FlightSQLServer, *mockQueryHandler, *mockMetadataHandler, *mockTransactionHandler, *mockPreparedStatementHandler) {
	allocator := memory.NewGoAllocator()
	logger := zerolog.New(zerolog.NewTestWriter(t))

	// Create mock handlers
	queryHandler := &mockQueryHandler{}
	metadataHandler := &mockMetadataHandler{}
	transactionHandler := &mockTransactionHandler{}
	preparedStatementHandler := &mockPreparedStatementHandler{}

	// Create mock cache
	mockCache := &mockCache{
		getFunc: func(ctx context.Context, key string) (arrow.Record, error) {
			return nil, nil
		},
		putFunc: func(ctx context.Context, key string, record arrow.Record) error {
			return nil
		},
	}

	// Create mock cache key generator
	mockKeyGen := &mockCacheKeyGenerator{
		generateKeyFunc: func(query string, params map[string]interface{}) string {
			return query
		},
	}

	// Create mock metrics collector
	mockMetrics := &mockMetricsCollector{
		incrementCounterFunc: func(name string, labels ...string) {},
		recordHistogramFunc:  func(name string, value float64, labels ...string) {},
		recordGaugeFunc:      func(name string, value float64, labels ...string) {},
		startTimerFunc: func(name string) metrics.Timer {
			return &mockTimer{}
		},
	}

	// Create server
	server := NewFlightSQLServer(
		queryHandler,
		metadataHandler,
		transactionHandler,
		preparedStatementHandler,
		nil, // pool not needed for tests
		nil, // converter not needed for tests
		allocator,
		mockCache,
		mockKeyGen,
		mockMetrics,
		logger,
	)

	return server, queryHandler, metadataHandler, transactionHandler, preparedStatementHandler
}

type mockTimer struct {
	metrics.Timer
}

func (m *mockTimer) Stop() float64 {
	return 0
}

// statementQuery implements flightsql.StatementQuery
type statementQuery struct {
	query         string
	transactionID []byte
}

func (s *statementQuery) GetQuery() string {
	return s.query
}

func (s *statementQuery) GetTransactionId() []byte {
	return s.transactionID
}

// statementUpdate implements flightsql.StatementUpdate
type statementUpdate struct {
	query         string
	transactionID []byte
}

func (s *statementUpdate) GetQuery() string {
	return s.query
}

func (s *statementUpdate) GetTransactionId() []byte {
	return s.transactionID
}

// actionBeginTransactionRequest implements flightsql.ActionBeginTransactionRequest
type actionBeginTransactionRequest struct {
	options *models.TransactionOptions
}

func (a *actionBeginTransactionRequest) GetTransactionOptions() *models.TransactionOptions {
	return a.options
}

// actionEndTransactionRequest implements flightsql.ActionEndTransactionRequest
type actionEndTransactionRequest struct {
	action        flightsql.EndTransactionRequestType
	transactionID []byte
}

func (a *actionEndTransactionRequest) GetAction() flightsql.EndTransactionRequestType {
	return a.action
}

func (a *actionEndTransactionRequest) GetTransactionId() []byte {
	return a.transactionID
}

// statementQueryTicket implements flightsql.StatementQueryTicket
type statementQueryTicket struct {
	handle []byte
}

func (s *statementQueryTicket) GetStatementHandle() []byte {
	return s.handle
}

// actionCreatePreparedStatementRequest implements flightsql.ActionCreatePreparedStatementRequest
type actionCreatePreparedStatementRequest struct {
	query         string
	transactionID []byte
}

func (a *actionCreatePreparedStatementRequest) GetQuery() string {
	return a.query
}

func (a *actionCreatePreparedStatementRequest) GetTransactionId() []byte {
	return a.transactionID
}

// preparedStatementQueryCmd implements flightsql.PreparedStatementQuery
type preparedStatementQueryCmd struct {
	handle []byte
}

func (p *preparedStatementQueryCmd) GetPreparedStatementHandle() []byte { return p.handle }

// preparedStatementUpdateCmd implements flightsql.PreparedStatementUpdate
type preparedStatementUpdateCmd struct {
	handle []byte
}

func (p *preparedStatementUpdateCmd) GetPreparedStatementHandle() []byte { return p.handle }

type sliceMessageReader struct {
	records []arrow.Record
	idx     int
	schema  *arrow.Schema
}

func (r *sliceMessageReader) Read() (arrow.Record, error) {
	if r.idx >= len(r.records) {
		return nil, io.EOF
	}
	rec := r.records[r.idx]
	r.idx++
	return rec, nil
}

func (r *sliceMessageReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *sliceMessageReader) Next() bool {
	return r.idx < len(r.records)
}

func (r *sliceMessageReader) Record() arrow.Record {
	if r.idx > 0 && r.idx <= len(r.records) {
		return r.records[r.idx-1]
	}
	return nil
}

func (r *sliceMessageReader) Err() error {
	return nil
}

func (r *sliceMessageReader) Chunk() flight.StreamChunk {
	if r.idx > 0 && r.idx <= len(r.records) {
		return flight.StreamChunk{Data: r.records[r.idx-1]}
	}
	return flight.StreamChunk{}
}

func (r *sliceMessageReader) LatestFlightDescriptor() *flight.FlightDescriptor {
	return nil
}

func (r *sliceMessageReader) LatestAppMetadata() []byte {
	return nil
}

func (r *sliceMessageReader) Release() {
	for _, rec := range r.records {
		rec.Release()
	}
}

func (r *sliceMessageReader) Retain() {
	for _, rec := range r.records {
		rec.Retain()
	}
}

type nopMetadataWriter struct{}

func (nopMetadataWriter) WriteMetadata([]byte) error { return nil }

func TestGetFlightInfoStatement(t *testing.T) {
	server, queryHandler, _, _, _ := setupTestServer(t)

	t.Run("successful query", func(t *testing.T) {
		// Setup mock response
		queryHandler.getFlightInfoFunc = func(ctx context.Context, query string) (*flight.FlightInfo, error) {
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			}, nil)
			return &flight.FlightInfo{
				Schema: flight.SerializeSchema(schema, memory.NewGoAllocator()),
			}, nil
		}

		// Test
		stmtQuery := &statementQuery{query: "SELECT * FROM test"}
		info, err := server.GetFlightInfoStatement(context.Background(), stmtQuery, nil)
		require.NoError(t, err)
		assert.NotNil(t, info)
		assert.NotNil(t, info.Schema)
	})

	t.Run("cache hit", func(t *testing.T) {
		// Setup mock cache
		server.memoryCache = &mockCache{
			getFunc: func(ctx context.Context, key string) (arrow.Record, error) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				}, nil)
				builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
				defer builder.Release()
				return builder.NewRecord(), nil
			},
		}

		// Test
		stmtQuery := &statementQuery{query: "SELECT * FROM test"}
		info, err := server.GetFlightInfoStatement(context.Background(), stmtQuery, nil)
		require.NoError(t, err)
		assert.NotNil(t, info)
		assert.NotNil(t, info.Schema)
	})
}

func TestDoGetStatement(t *testing.T) {
	server, queryHandler, _, _, _ := setupTestServer(t)

	t.Run("successful query", func(t *testing.T) {
		// Setup mock response
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		ch := make(chan flight.StreamChunk, 1)
		ch <- flight.StreamChunk{Data: array.NewRecord(schema, nil, 0)}
		close(ch)

		queryHandler.executeStatementFunc = func(ctx context.Context, query string, txnID string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
			return schema, ch, nil
		}

		// Test
		stmtQuery := &statementQuery{query: "SELECT * FROM test"}
		schema, chunks, err := server.DoGetStatement(context.Background(), &statementQueryTicket{
			handle: []byte(stmtQuery.query),
		})
		require.NoError(t, err)
		assert.NotNil(t, schema)
		assert.NotNil(t, chunks)

		// Consume chunks
		for chunk := range chunks {
			assert.NotNil(t, chunk.Data)
			chunk.Data.Release()
		}
	})

	t.Run("cache hit", func(t *testing.T) {
		// Setup mock cache
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		record := array.NewRecord(schema, nil, 0)
		defer record.Release()

		server.memoryCache = &mockCache{
			getFunc: func(ctx context.Context, key string) (arrow.Record, error) {
				return record, nil
			},
		}

		// Test
		stmtQuery := &statementQuery{query: "SELECT * FROM test"}
		schema, chunks, err := server.DoGetStatement(context.Background(), &statementQueryTicket{
			handle: []byte(stmtQuery.query),
		})
		require.NoError(t, err)
		assert.NotNil(t, schema)
		assert.NotNil(t, chunks)

		// Consume chunks
		for chunk := range chunks {
			assert.NotNil(t, chunk.Data)
			chunk.Data.Release()
		}
	})
}

func TestDoPutCommandStatementUpdate(t *testing.T) {
	server, queryHandler, _, _, _ := setupTestServer(t)

	t.Run("successful update", func(t *testing.T) {
		// Setup mock response
		queryHandler.executeUpdateFunc = func(ctx context.Context, query string, txnID string) (int64, error) {
			return 1, nil
		}

		// Test
		stmtUpdate := &statementUpdate{query: "UPDATE test SET value = 42"}
		affected, err := server.DoPutCommandStatementUpdate(context.Background(), stmtUpdate)
		require.NoError(t, err)
		assert.Equal(t, int64(1), affected)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		queryHandler.executeUpdateFunc = func(ctx context.Context, query string, txnID string) (int64, error) {
			return 0, assert.AnError
		}

		// Test
		stmtUpdate := &statementUpdate{query: "UPDATE test SET value = 42"}
		affected, err := server.DoPutCommandStatementUpdate(context.Background(), stmtUpdate)
		assert.Error(t, err)
		assert.Equal(t, int64(0), affected)
	})
}

func TestGetFlightInfoCatalogs(t *testing.T) {
	server, _, metadataHandler, _, _ := setupTestServer(t)

	t.Run("successful catalog retrieval", func(t *testing.T) {
		// Setup mock response
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "catalog_name", Type: arrow.BinaryTypes.String},
		}, nil)
		ch := make(chan flight.StreamChunk, 1)
		close(ch)

		metadataHandler.getCatalogsFunc = func(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
			return schema, ch, nil
		}

		// Test
		desc := &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte("get_catalogs"),
		}
		info, err := server.GetFlightInfoCatalogs(context.Background(), desc)
		require.NoError(t, err)
		assert.NotNil(t, info)
		assert.NotNil(t, info.Schema)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		metadataHandler.getCatalogsFunc = func(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
			return nil, nil, assert.AnError
		}

		// Test
		desc := &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte("get_catalogs"),
		}
		info, err := server.GetFlightInfoCatalogs(context.Background(), desc)
		assert.Error(t, err)
		assert.Nil(t, info)
	})
}

func TestBeginTransaction(t *testing.T) {
	server, _, _, transactionHandler, _ := setupTestServer(t)

	t.Run("successful transaction begin", func(t *testing.T) {
		// Setup mock response
		transactionHandler.beginFunc = func(ctx context.Context, readOnly bool) (string, error) {
			return "txn123", nil
		}

		// Test
		req := &actionBeginTransactionRequest{
			options: &models.TransactionOptions{
				IsolationLevel: models.IsolationLevelReadCommitted,
				ReadOnly:       false,
			},
		}
		txnID, err := server.BeginTransaction(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, []byte("txn123"), txnID)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		transactionHandler.beginFunc = func(ctx context.Context, readOnly bool) (string, error) {
			return "", assert.AnError
		}

		// Test
		req := &actionBeginTransactionRequest{
			options: &models.TransactionOptions{
				IsolationLevel: models.IsolationLevelReadCommitted,
				ReadOnly:       false,
			},
		}
		txnID, err := server.BeginTransaction(context.Background(), req)
		assert.Error(t, err)
		assert.Nil(t, txnID)
	})
}

func TestEndTransaction(t *testing.T) {
	server, _, _, transactionHandler, _ := setupTestServer(t)

	t.Run("successful commit", func(t *testing.T) {
		// Setup mock response
		transactionHandler.commitFunc = func(ctx context.Context, txnID string) error {
			return nil
		}

		// Test
		req := &actionEndTransactionRequest{
			action:        flightsql.EndTransactionCommit,
			transactionID: []byte("txn123"),
		}
		err := server.EndTransaction(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("successful rollback", func(t *testing.T) {
		// Setup mock response
		transactionHandler.rollbackFunc = func(ctx context.Context, txnID string) error {
			return nil
		}

		// Test
		req := &actionEndTransactionRequest{
			action:        flightsql.EndTransactionRollback,
			transactionID: []byte("txn123"),
		}
		err := server.EndTransaction(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		transactionHandler.commitFunc = func(ctx context.Context, txnID string) error {
			return assert.AnError
		}

		// Test
		req := &actionEndTransactionRequest{
			action:        flightsql.EndTransactionCommit,
			transactionID: []byte("txn123"),
		}
		err := server.EndTransaction(context.Background(), req)
		assert.Error(t, err)
	})
}

func TestCreatePreparedStatement(t *testing.T) {
	server, _, _, _, preparedStatementHandler := setupTestServer(t)

	t.Run("successful creation - SELECT query", func(t *testing.T) {
		// Setup mock response
		expectedHandle := "stmt-123"
		expectedSchema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		}, nil)

		preparedStatementHandler.createFunc = func(ctx context.Context, query string, txnID string) (string, *arrow.Schema, error) {
			assert.Equal(t, "SELECT * FROM users WHERE id = ?", query)
			assert.Equal(t, "txn-1", txnID)
			return expectedHandle, expectedSchema, nil
		}

		// Test
		req := &actionCreatePreparedStatementRequest{
			query:         "SELECT * FROM users WHERE id = ?",
			transactionID: []byte("txn-1"),
		}
		result, err := server.CreatePreparedStatement(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, []byte(expectedHandle), result.Handle)
		assert.NotNil(t, result.DatasetSchema)
	})

	t.Run("successful creation - INSERT query", func(t *testing.T) {
		// Setup mock response
		expectedHandle := "stmt-456"
		expectedSchema := arrow.NewSchema([]arrow.Field{
			{Name: "affected_rows", Type: arrow.PrimitiveTypes.Int64},
		}, nil)

		preparedStatementHandler.createFunc = func(ctx context.Context, query string, txnID string) (string, *arrow.Schema, error) {
			assert.Equal(t, "INSERT INTO users (name, age) VALUES (?, ?)", query)
			assert.Empty(t, txnID)
			return expectedHandle, expectedSchema, nil
		}

		// Test
		req := &actionCreatePreparedStatementRequest{
			query: "INSERT INTO users (name, age) VALUES (?, ?)",
		}
		result, err := server.CreatePreparedStatement(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, []byte(expectedHandle), result.Handle)
		assert.NotNil(t, result.DatasetSchema)
	})

	t.Run("error - invalid query", func(t *testing.T) {
		// Setup mock response
		preparedStatementHandler.createFunc = func(ctx context.Context, query string, txnID string) (string, *arrow.Schema, error) {
			return "", nil, assert.AnError
		}

		// Test
		req := &actionCreatePreparedStatementRequest{
			query: "INVALID SQL",
		}
		result, err := server.CreatePreparedStatement(context.Background(), req)
		assert.Error(t, err)
		assert.Empty(t, result.Handle)
		assert.Nil(t, result.DatasetSchema)
	})

	t.Run("error - empty query", func(t *testing.T) {
		// Test
		req := &actionCreatePreparedStatementRequest{
			query: "",
		}
		result, err := server.CreatePreparedStatement(context.Background(), req)
		assert.Error(t, err)
		assert.Empty(t, result.Handle)
		assert.Nil(t, result.DatasetSchema)
	})
}

func TestDoPutPreparedStatementQuery(t *testing.T) {
	server, _, _, _, psHandler := setupTestServer(t)

	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(allocator, schema)
	builder.Field(0).(*array.Int64Builder).Append(int64(42))
	rec := builder.NewRecord()
	builder.Release()

	reader := &sliceMessageReader{records: []arrow.Record{rec}}
	writer := nopMetadataWriter{}

	var captured arrow.Record
	psHandler.setParametersFunc = func(ctx context.Context, handle string, params arrow.Record) error {
		captured = params
		return nil
	}

	handle := []byte("stmt-query")
	cmd := &preparedStatementQueryCmd{handle: handle}
	resp, err := server.DoPutPreparedStatementQuery(context.Background(), cmd, reader, writer)
	require.NoError(t, err)
	assert.Equal(t, handle, resp)
	require.NotNil(t, captured)
	assert.Equal(t, int64(1), captured.NumRows())

	captured.Release()
	rec.Release()
}

func TestDoPutPreparedStatementQuery_Errors(t *testing.T) {
	server, _, _, _, psHandler := setupTestServer(t)
	allocator := memory.NewGoAllocator()

	schema1 := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b1 := array.NewRecordBuilder(allocator, schema1)
	b1.Field(0).(*array.Int64Builder).Append(int64(1))
	rec1 := b1.NewRecord()
	b1.Release()

	schema2 := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Float64}}, nil)
	b2 := array.NewRecordBuilder(allocator, schema2)
	b2.Field(0).(*array.Float64Builder).Append(1.0)
	rec2 := b2.NewRecord()
	b2.Release()

	t.Run("schema mismatch", func(t *testing.T) {
		reader := &sliceMessageReader{records: []arrow.Record{rec1, rec2}}
		writer := nopMetadataWriter{}
		psHandler.setParametersFunc = func(ctx context.Context, handle string, params arrow.Record) error { return nil }
		_, err := server.DoPutPreparedStatementQuery(context.Background(), &preparedStatementQueryCmd{handle: []byte("ps")}, reader, writer)
		assert.Error(t, err)
	})

	t.Run("set parameters error", func(t *testing.T) {
		reader := &sliceMessageReader{records: []arrow.Record{rec1}}
		writer := nopMetadataWriter{}
		psHandler.setParametersFunc = func(ctx context.Context, handle string, params arrow.Record) error { return assert.AnError }
		_, err := server.DoPutPreparedStatementQuery(context.Background(), &preparedStatementQueryCmd{handle: []byte("ps")}, reader, writer)
		assert.Error(t, err)
	})

	rec1.Release()
	rec2.Release()
}

func TestDoPutPreparedStatementUpdate(t *testing.T) {
	server, _, _, _, psHandler := setupTestServer(t)
	allocator := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(allocator, schema)
	b.Field(0).(*array.Int64Builder).Append(int64(7))
	rec := b.NewRecord()
	b.Release()

	psHandler.executeUpdateFunc = func(ctx context.Context, handle string, params arrow.Record) (int64, error) {
		require.NotNil(t, params)
		return 5, nil
	}

	reader := &sliceMessageReader{records: []arrow.Record{rec}}
	affected, err := server.DoPutPreparedStatementUpdate(context.Background(), &preparedStatementUpdateCmd{handle: []byte("ps")}, reader)
	require.NoError(t, err)
	assert.Equal(t, int64(5), affected)

	rec.Release()
}

func TestDoPutPreparedStatementUpdate_Errors(t *testing.T) {
	server, _, _, _, psHandler := setupTestServer(t)
	allocator := memory.NewGoAllocator()

	schema1 := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b1 := array.NewRecordBuilder(allocator, schema1)
	b1.Field(0).(*array.Int64Builder).Append(int64(1))
	rec1 := b1.NewRecord()
	b1.Release()

	schema2 := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Float64}}, nil)
	b2 := array.NewRecordBuilder(allocator, schema2)
	b2.Field(0).(*array.Float64Builder).Append(1.0)
	rec2 := b2.NewRecord()
	b2.Release()

	t.Run("schema mismatch", func(t *testing.T) {
		reader := &sliceMessageReader{records: []arrow.Record{rec1, rec2}}
		psHandler.executeUpdateFunc = func(ctx context.Context, handle string, params arrow.Record) (int64, error) { return 0, nil }
		_, err := server.DoPutPreparedStatementUpdate(context.Background(), &preparedStatementUpdateCmd{handle: []byte("ps")}, reader)
		assert.Error(t, err)
	})

	t.Run("execute error", func(t *testing.T) {
		reader := &sliceMessageReader{records: []arrow.Record{rec1}}
		psHandler.executeUpdateFunc = func(ctx context.Context, handle string, params arrow.Record) (int64, error) { return 0, assert.AnError }
		_, err := server.DoPutPreparedStatementUpdate(context.Background(), &preparedStatementUpdateCmd{handle: []byte("ps")}, reader)
		assert.Error(t, err)
	})

	rec1.Release()
	rec2.Release()
}
