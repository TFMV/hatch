package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/TFMV/hatch/cmd/server/config"
	"github.com/TFMV/hatch/cmd/server/server"
	"github.com/TFMV/hatch/test/utils"
)

// TestMetricsCollector implements the MetricsCollector interface for testing
type TestMetricsCollector struct {
	mu         sync.RWMutex
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name]++
}

// RecordHistogram implements MetricsCollector
func (m *TestMetricsCollector) RecordHistogram(name string, value float64, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.histograms[name] = append(m.histograms[name], value)
}

// RecordGauge implements MetricsCollector
func (m *TestMetricsCollector) RecordGauge(name string, value float64, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[name] = value
}

// StartTimer implements MetricsCollector
func (m *TestMetricsCollector) StartTimer(name string) server.Timer {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	t.metrics.mu.Lock()
	defer t.metrics.mu.Unlock()
	t.metrics.timers[t.name] = append(t.metrics.timers[t.name], duration)
	return duration
}

// FlightE2ETestSuite defines the structure for end-to-end tests.
type FlightE2ETestSuite struct {
	suite.Suite
	server    *server.FlightSQLServer
	listener  net.Listener
	client    *flightsql.Client
	addr      string
	allocator memory.Allocator
	logger    zerolog.Logger
	ctx       context.Context
	cancel    context.CancelFunc
}

func TestFlightE2ETestSuite(t *testing.T) {
	suite.Run(t, new(FlightE2ETestSuite))
}

func (s *FlightE2ETestSuite) SetupSuite() {
	cfg := config.DefaultConfig()
	cfg.Database = ":memory:"

	s.logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		Level(zerolog.DebugLevel).
		With().Timestamp().Logger()

	var err error
	s.server, err = server.New(cfg, s.logger, &utils.NoOpMetricsCollector{})
	require.NoError(s.T(), err)

	s.listener, err = net.Listen("tcp", "localhost:0")
	require.NoError(s.T(), err)

	grpcServer := grpc.NewServer(s.server.GetMiddleware()...)
	s.server.Register(grpcServer)

	go func() {
		if errSrv := grpcServer.Serve(s.listener); errSrv != nil && errSrv != grpc.ErrServerStopped {
			s.logger.Fatal().Err(errSrv).Msg("Flight SQL server failed")
		}
	}()
	s.T().Logf("Flight SQL server started on %s", s.listener.Addr().String())
	s.addr = s.listener.Addr().String()

	var errFSQLClient error
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	for i := 0; i < 5; i++ {
		s.client, errFSQLClient = flightsql.NewClient(s.addr, nil, nil, dialOpts...)
		if errFSQLClient == nil {
			checkCtx, cancelCheck := context.WithTimeout(context.Background(), 3*time.Second)
			_, checkErr := s.client.GetSqlInfo(checkCtx, []flightsql.SqlInfo{})
			cancelCheck()
			if checkErr == nil {
				break
			}
			errFSQLClient = fmt.Errorf("flightsql.NewClient succeeded but GetSqlInfo failed: %w", checkErr)
			if s.client != nil {
				s.client.Close()
				s.client = nil
			}
		}
		s.logger.Warn().Err(errFSQLClient).Int("attempt", i+1).Str("address", s.addr).Msg("Failed to create Flight SQL client, retrying...")
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	require.NoError(s.T(), errFSQLClient, "Failed to create Flight SQL client after multiple retries")
	require.NotNil(s.T(), s.client, "Flight SQL client is nil after successful creation attempt")

	s.allocator = memory.NewGoAllocator()
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 90*time.Second)

	s.setupTestTable(s.ctx, s.T())
}

// Placeholder for setupTestTable method
// Please replace with the actual implementation if available.
func (s *FlightE2ETestSuite) setupTestTable(ctx context.Context, t *testing.T) {
	// Example: Create a simple table for other tests to use.
	// This is a placeholder and might not match the original intent.
	tableCreateQuery := "CREATE TABLE IF NOT EXISTS common_test_table (id INT, description VARCHAR)"
	_, err := s.client.Execute(s.ctx, tableCreateQuery)
	require.NoError(t, err, "setupTestTable: failed to create common_test_table")
	s.T().Log("setupTestTable: common_test_table created or already exists.")
}

// TestFlightE2E tests end-to-end functionality of the Hatch server with Flight SQL
func TestFlightE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Flight E2E test in CI environment")
	}
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
	grpcServer := grpc.NewServer(srv.GetMiddleware()...)
	srv.Register(grpcServer)

	// Create listener
	listener, err := net.Listen("tcp", "localhost:0") // Use random port to avoid conflict with SetupSuite
	require.NoError(t, err)

	// Start server
	go func() {
		if serveErr := grpcServer.Serve(listener); serveErr != nil && serveErr != grpc.ErrServerStopped {
			logger.Fatal().Err(serveErr).Msg("TestFlightE2E gRPC server failed")
		}
	}()

	serverAddr := listener.Addr().String()
	logger.Info().Str("address", serverAddr).Msg("TestFlightE2E server started")

	// Create Flight SQL client
	var flightSQLClient *flightsql.Client
	var errFlightSQLClient error
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	for i := 0; i < 5; i++ { // Retry loop for flightsql.NewClient
		flightSQLClient, errFlightSQLClient = flightsql.NewClient(serverAddr, nil, nil, dialOpts...)
		if errFlightSQLClient == nil {
			// Verify client is responsive with a simple GetSqlInfo call
			pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, errPing := flightSQLClient.GetSqlInfo(pingCtx, []flightsql.SqlInfo{})
			pingCancel()
			if errPing == nil {
				break // Client created and responsive
			}
			errFlightSQLClient = fmt.Errorf("flightsql.NewClient for TestFlightE2E succeeded but GetSqlInfo failed: %w", errPing)
			if flightSQLClient != nil {
				flightSQLClient.Close() // Close problematic client
			}
		}
		logger.Warn().Err(errFlightSQLClient).Int("attempt", i+1).Str("address", serverAddr).Msg("Failed to create flightSQLClient for TestFlightE2E, retrying...")
		time.Sleep(time.Duration(i+1) * time.Second) // Incremental backoff
	}
	require.NoError(t, errFlightSQLClient, "Failed to create flightSQLClient for TestFlightE2E after multiple retries")
	require.NotNil(t, flightSQLClient, "flightSQLClient for TestFlightE2E is nil after successful creation attempt")
	defer flightSQLClient.Close()

	// Test cases
	t.Run("CreateTable", testCreateTable(flightSQLClient))
	t.Run("InsertData", testInsertData(flightSQLClient))
	t.Run("QueryData", testQueryData(flightSQLClient))
	t.Run("UpdateData", testUpdateData(flightSQLClient))
	t.Run("DeleteData", testDeleteData(flightSQLClient))

	// Cleanup
	grpcServer.GracefulStop()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	srv.Close(closeCtx)
	logger.Info().Msg("TestFlightE2E server stopped")
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
		prep, err := client.Prepare(ctx, "INSERT INTO test_flight (id, name, value) VALUES (?, ?, ?)")
		require.NoError(t, err)
		defer prep.Close(ctx)

		// Create parameter record for a single row
		builder := array.NewRecordBuilder(memory.DefaultAllocator, prep.ParameterSchema())
		defer builder.Release()

		// Set values for a single row
		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues([]string{"test1"}, nil)
		builder.Field(2).(*array.Int64Builder).AppendValues([]int64{11}, nil)

		paramRecord := builder.NewRecord()
		defer paramRecord.Release()

		// Set parameters and execute
		prep.SetParameters(paramRecord)
		rowsAffected, err := prep.ExecuteUpdate(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		// Verify insertion
		info, err := client.Execute(ctx, "SELECT COUNT(*) FROM test_flight")
		require.NoError(t, err)
		require.NotNil(t, info)
		require.NotEmpty(t, info.Endpoint)
		require.NotNil(t, info.Endpoint[0].Ticket)

		reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
		require.NoError(t, err)
		defer reader.Release()

		record, err := reader.Read()
		require.NoError(t, err)
		require.Equal(t, int64(1), record.Column(0).(*array.Int64).Value(0))
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
		assert.Equal(t, int64(1), record.NumRows())
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
		assert.Equal(t, float64(11.0), valueCol.Value(0))
	}
}

func testUpdateData(client *flightsql.Client) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		// Update data
		rowsAffected, err := client.ExecuteUpdate(ctx, "UPDATE test_flight SET value = 4.4 WHERE id = 1")
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

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
		rowsAffected, err := client.ExecuteUpdate(ctx, "DELETE FROM test_flight WHERE id = 1")
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		// Verify deletion
		info, err := client.Execute(ctx, "SELECT COUNT(*) FROM test_flight")
		require.NoError(t, err)
		require.NotEmpty(t, info.Endpoint)
		require.NotNil(t, info.Endpoint[0].Ticket)

		reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
		require.NoError(t, err)
		defer reader.Release()

		record, err := reader.Read()
		require.NoError(t, err)
		assert.Equal(t, int64(0), record.Column(0).(*array.Int64).Value(0))
	}
}

func TestQueryOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Query Operations test in CI environment")
	}
	// Create test configuration
	cfg := config.DefaultConfig()
	cfg.Database = ":memory:" // Use in-memory database for tests
	cfg.LogLevel = "debug"

	// Create logger
	logger := zerolog.New(zerolog.NewTestWriter(t)).With().Timestamp().Logger()

	// Create metrics collector
	metrics := NewTestMetricsCollector()

	// Create server
	srv, err := server.New(cfg, logger, metrics)
	require.NoError(t, err)

	// Create gRPC server
	grpcServer := grpc.NewServer(srv.GetMiddleware()...)
	srv.Register(grpcServer)

	// Create listener
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	// Start server
	go func() {
		if serveErr := grpcServer.Serve(listener); serveErr != nil && serveErr != grpc.ErrServerStopped {
			logger.Fatal().Err(serveErr).Msg("TestQueryOperations gRPC server failed")
		}
	}()

	serverAddr := listener.Addr().String()
	logger.Info().Str("address", serverAddr).Msg("TestQueryOperations server started")

	// Create Flight SQL client
	var client *flightsql.Client
	var errClient error
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	for i := 0; i < 5; i++ {
		client, errClient = flightsql.NewClient(serverAddr, nil, nil, dialOpts...)
		if errClient == nil {
			pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, errPing := client.GetSqlInfo(pingCtx, []flightsql.SqlInfo{})
			pingCancel()
			if errPing == nil {
				break
			}
			errClient = fmt.Errorf("flightsql.NewClient succeeded but GetSqlInfo failed: %w", errPing)
			if client != nil {
				client.Close()
			}
		}
		logger.Warn().Err(errClient).Int("attempt", i+1).Str("address", serverAddr).Msg("Failed to create client, retrying...")
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	require.NoError(t, errClient, "Failed to create client after multiple retries")
	require.NotNil(t, client, "Client is nil after successful creation attempt")
	defer client.Close()

	// Create test table
	ctx := context.Background()
	createQuery := "CREATE TABLE test_flight (id BIGINT, name VARCHAR, value DOUBLE)"
	_, err = client.Execute(ctx, createQuery)
	require.NoError(t, err)

	// Insert test data first
	insertQuery := "INSERT INTO test_flight (id, name, value) VALUES (1, 'test', 1.1)"
	rowsAffected, err := client.ExecuteUpdate(ctx, insertQuery)
	require.NoError(t, err)
	require.Equal(t, int64(1), rowsAffected)

	t.Run("DoPutCommandStatementUpdate", func(t *testing.T) {
		rowsAffected, err := client.ExecuteUpdate(ctx, "DELETE FROM test_flight WHERE id = 1")
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		// Verify deletion
		info, err := client.Execute(ctx, "SELECT COUNT(*) FROM test_flight WHERE id = 1")
		require.NoError(t, err)
		require.NotNil(t, info)
		require.NotEmpty(t, info.Endpoint)
		require.NotNil(t, info.Endpoint[0].Ticket)

		reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
		require.NoError(t, err)
		defer reader.Release()

		record, err := reader.Read()
		require.NoError(t, err)
		require.Equal(t, int64(0), record.Column(0).(*array.Int64).Value(0))
	})

	// Cleanup
	grpcServer.GracefulStop()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	srv.Close(closeCtx)
	logger.Info().Msg("TestQueryOperations server stopped")
}
