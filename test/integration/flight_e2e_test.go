package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/TFMV/flight/cmd/server/config"
	"github.com/TFMV/flight/cmd/server/server"
)

// TestMetricsCollector implements the MetricsCollector interface for testing
type TestMetricsCollector struct {
	counters   map[string]int
	histograms map[string][]float64
	gauges     map[string]float64
	timers     map[string][]float64
}

// NewTestMetricsCollector creates a new test metrics collector
func NewTestMetricsCollector() *TestMetricsCollector {
	return &TestMetricsCollector{
		counters:   make(map[string]int),
		histograms: make(map[string][]float64),
		gauges:     make(map[string]float64),
		timers:     make(map[string][]float64),
	}
}

// IncrementCounter implements MetricsCollector
func (m *TestMetricsCollector) IncrementCounter(name string, labels ...string) {
	m.counters[name]++
}

// RecordHistogram implements MetricsCollector
func (m *TestMetricsCollector) RecordHistogram(name string, value float64, labels ...string) {
	m.histograms[name] = append(m.histograms[name], value)
}

// RecordGauge implements MetricsCollector
func (m *TestMetricsCollector) RecordGauge(name string, value float64, labels ...string) {
	m.gauges[name] = value
}

// StartTimer implements MetricsCollector
func (m *TestMetricsCollector) StartTimer(name string) server.Timer {
	return &testTimer{
		name:    name,
		start:   time.Now(),
		metrics: m,
	}
}

// testTimer implements the Timer interface for testing
type testTimer struct {
	name    string
	start   time.Time
	metrics *TestMetricsCollector
}

// Stop implements Timer
func (t *testTimer) Stop() float64 {
	duration := time.Since(t.start).Seconds()
	t.metrics.timers[t.name] = append(t.metrics.timers[t.name], duration)
	return duration
}

// TestFlightE2E tests end-to-end functionality of the Hatch server with Flight SQL
func TestFlightE2E(t *testing.T) {
	// Create test configuration
	cfg := config.DefaultConfig()
	cfg.Database = ":memory:" // Use in-memory database for tests
	cfg.LogLevel = "debug"
	cfg.Cache.Enabled = true
	cfg.Cache.EnableStats = true
	cfg.Cache.MaxSize = 100 * 1024 * 1024 // 100MB
	cfg.Cache.TTL = 5 * time.Minute
	cfg.Cache.CleanupInterval = 1 * time.Minute

	// Create logger
	logger := zerolog.New(zerolog.NewTestWriter(t)).With().Timestamp().Logger()

	// Create metrics collector
	metrics := NewTestMetricsCollector()

	// Create server
	srv, err := server.New(cfg, logger, metrics)
	require.NoError(t, err)

	// Create gRPC server
	grpcServer := grpc.NewServer()
	srv.Register(grpcServer)

	// Create listener
	listener, err := net.Listen("tcp", "localhost:8815")
	require.NoError(t, err)

	// Start server
	go func() {
		require.NoError(t, grpcServer.Serve(listener))
	}()

	// Create client connection
	conn, err := grpc.Dial("localhost:8815", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	// Create Flight SQL client
	client, err := flightsql.NewClient("localhost:8815", nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer client.Close()

	// Test cases
	t.Run("CreateTable", testCreateTable(client))
	t.Run("InsertData", testInsertData(client))
	t.Run("QueryData", testQueryData(client))
	t.Run("UpdateData", testUpdateData(client))
	t.Run("DeleteData", testDeleteData(client))

	// Cleanup
	grpcServer.GracefulStop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Close(ctx)
}

func testCreateTable(client *flightsql.Client) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		// Create test table
		createQuery := "CREATE TABLE test_flight (id BIGINT, name VARCHAR, value DOUBLE)"
		_, err := client.Execute(ctx, createQuery)
		require.NoError(t, err)
	}
}

func testInsertData(client *flightsql.Client) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		// Create Arrow record for insertion
		schema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "name", Type: arrow.BinaryTypes.String},
				{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			},
			nil,
		)

		// Create arrays for the record
		ids := array.NewInt64Builder(memory.DefaultAllocator)
		names := array.NewStringBuilder(memory.DefaultAllocator)
		values := array.NewFloat64Builder(memory.DefaultAllocator)

		// Add data
		ids.AppendValues([]int64{1, 2, 3}, nil)
		names.AppendValues([]string{"test1", "test2", "test3"}, nil)
		values.AppendValues([]float64{1.1, 2.2, 3.3}, nil)

		// Create record
		record := array.NewRecord(schema, []arrow.Array{
			ids.NewArray(),
			names.NewArray(),
			values.NewArray(),
		}, 3)
		defer record.Release()

		// Insert data using prepared statement
		stmt, err := client.Prepare(ctx, "INSERT INTO test_flight (id, name, value) VALUES (?, ?, ?)")
		require.NoError(t, err)
		defer stmt.Close(ctx)

		// Set the parameters for the prepared statement
		stmt.SetParameters(record)
		_, err = stmt.Execute(ctx)
		require.NoError(t, err)
	}
}

func testQueryData(client *flightsql.Client) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		// Execute query
		info, err := client.Execute(ctx, "SELECT * FROM test_flight")
		require.NoError(t, err)

		// Get stream
		reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
		require.NoError(t, err)
		defer reader.Release()

		// Read all records
		record, err := reader.Read()
		require.NoError(t, err)

		// Verify results
		assert.Equal(t, int64(3), record.NumRows())
		assert.Equal(t, int64(3), record.NumCols())

		// Verify column names
		assert.Equal(t, "id", record.ColumnName(0))
		assert.Equal(t, "name", record.ColumnName(1))
		assert.Equal(t, "value", record.ColumnName(2))

		// Verify data
		idCol := record.Column(0).(*array.Int64)
		nameCol := record.Column(1).(*array.String)
		valueCol := record.Column(2).(*array.Float64)

		assert.Equal(t, int64(1), idCol.Value(0))
		assert.Equal(t, "test1", nameCol.Value(0))
		assert.Equal(t, 1.1, valueCol.Value(0))
	}
}

func testUpdateData(client *flightsql.Client) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		// Update data
		_, err := client.Execute(ctx, "UPDATE test_flight SET value = 4.4 WHERE id = 1")
		require.NoError(t, err)

		// Verify update
		info, err := client.Execute(ctx, "SELECT value FROM test_flight WHERE id = 1")
		require.NoError(t, err)

		reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
		require.NoError(t, err)
		defer reader.Release()

		record, err := reader.Read()
		require.NoError(t, err)
		assert.Equal(t, float64(4.4), record.Column(0).(*array.Float64).Value(0))
	}
}

func testDeleteData(client *flightsql.Client) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		// Delete data
		_, err := client.Execute(ctx, "DELETE FROM test_flight WHERE id = 2")
		require.NoError(t, err)

		// Verify deletion
		info, err := client.Execute(ctx, "SELECT COUNT(*) FROM test_flight")
		require.NoError(t, err)

		reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
		require.NoError(t, err)
		defer reader.Release()

		record, err := reader.Read()
		require.NoError(t, err)
		assert.Equal(t, int64(2), record.Column(0).(*array.Int64).Value(0))
	}
}
