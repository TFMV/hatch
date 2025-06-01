package test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/TFMV/flight/cmd/server/config"
	"github.com/TFMV/flight/cmd/server/server"
)

// TestServer represents a test server instance
type TestServer struct {
	*server.FlightSQLServer
	grpcServer *grpc.Server
	client     flight.Client
	conn       *grpc.ClientConn
}

// NewTestServer creates a new test server instance
func NewTestServer(t *testing.T) *TestServer {
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
	conn, err := grpc.NewClient("0.0.0.0:8815", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	// Create Flight client
	client := flight.NewClientFromConn(conn, nil)

	return &TestServer{
		FlightSQLServer: srv,
		grpcServer:      grpcServer,
		client:          client,
		conn:            conn,
	}
}

// Close closes the test server and cleans up resources
func (ts *TestServer) Close() {
	if ts.conn != nil {
		ts.conn.Close()
	}
	if ts.grpcServer != nil {
		ts.grpcServer.GracefulStop()
	}
	if ts.FlightSQLServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ts.FlightSQLServer.Close(ctx)
	}
}

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

// Helper functions for testing

// assertFlightInfo asserts that a FlightInfo has the expected properties
func assertFlightInfo(t *testing.T, info *flight.FlightInfo, expectedSchema *arrow.Schema) {
	assert.NotNil(t, info)
	if info == nil {
		t.Log("assertFlightInfo received a nil FlightInfo object")
		return
	}
	assert.NotEmpty(t, info.Endpoint)
	assert.NotNil(t, info.FlightDescriptor)

	if expectedSchema != nil {
		assert.NotNil(t, info.Schema)
		actualSchema, err := flight.DeserializeSchema(info.Schema, memory.NewGoAllocator())
		require.NoError(t, err)
		assert.True(t, expectedSchema.Equal(actualSchema))
	}
}

// assertStreamChunks asserts that a stream of chunks contains the expected data
func assertStreamChunks(t *testing.T, chunks <-chan flight.StreamChunk, expectedSchema *arrow.Schema, expectedRecords int) {
	count := 0
	for chunk := range chunks {
		assert.NotNil(t, chunk.Data)
		if expectedSchema != nil {
			assert.True(t, expectedSchema.Equal(chunk.Data.Schema()))
		}
		count++
	}
	assert.Equal(t, expectedRecords, count)
}
