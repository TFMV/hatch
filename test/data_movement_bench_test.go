package test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/TFMV/hatch/cmd/server/config"
	"github.com/TFMV/hatch/cmd/server/server"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
)

// isCI returns true if running in a CI environment
func isCI() bool {
	return os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != ""
}

// skipInCI skips the test if running in CI environment
func skipInCI(b *testing.B) {
	if isCI() {
		b.Skip("Skipping benchmark in CI environment")
	}
}

// noopMetrics implements the metrics interface with no-op operations
type noopMetrics struct{}

func (m *noopMetrics) RecordLatency(_ string, _ time.Duration)          {}
func (m *noopMetrics) RecordCount(_ string, _ int64)                    {}
func (m *noopMetrics) RecordBytes(_ string, _ int64)                    {}
func (m *noopMetrics) IncrementCounter(_ string, _ ...string)           {}
func (m *noopMetrics) RecordGauge(_ string, _ float64, _ ...string)     {}
func (m *noopMetrics) RecordHistogram(_ string, _ float64, _ ...string) {}
func (m *noopMetrics) StartTimer(_ string) server.Timer                 { return &noopTimer{} }

type noopTimer struct{}

func (t *noopTimer) Stop() float64 { return 0 }

// setupBenchmarkServer creates a new server instance for benchmarking
func setupBenchmarkServer(b *testing.B) *server.FlightSQLServer {
	cfg := &config.Config{
		Database:          ":memory:",
		ConnectionTimeout: 30 * time.Second,
		QueryTimeout:      5 * time.Minute,
		ConnectionPool: config.ConnectionPoolConfig{
			MaxOpenConnections: 25,
			MaxIdleConnections: 5,
			ConnMaxLifetime:    30 * time.Minute,
			ConnMaxIdleTime:    10 * time.Minute,
			HealthCheckPeriod:  1 * time.Minute,
		},
		Cache: config.CacheConfig{
			Enabled: true,
			MaxSize: 100 * 1024 * 1024,
		},
		SafeCopy: false,
	}

	logger := zerolog.Nop()
	srv, err := server.New(cfg, logger, &noopMetrics{})
	if err != nil {
		b.Fatalf("Failed to create server: %v", err)
	}

	return srv
}

// BenchmarkParquetRead benchmarks reading different parquet files using DuckDB
func BenchmarkParquetRead(b *testing.B) {
	skipInCI(b)
	testFiles := []struct {
		name     string
		path     string
		expected int64 // expected number of rows
	}{
		{"Simple", "simple.parquet", 1},
		{"Complex", "complex.parquet", 2},
		{"SparkOnTime", "spark-ontime.parquet", 10},
		{"SparkStore", "spark-store.parquet", 12},
		{"UserData", "userdata1.parquet", 1000},
		{"LineItem", "lineitem-top10000.gzip.parquet", 10000},
		{"SortedZstd", "sorted.zstd_18_131072_small.parquet", 131073},
	}

	for _, tf := range testFiles {
		b.Run(tf.name, func(b *testing.B) {
			filePath := filepath.Join("/Users/thomasmcgeehan/flight/flight/data/parquet-testing", tf.path)

			// Create a new DuckDB connection
			db, err := sql.Open("duckdb", ":memory:")
			require.NoError(b, err)
			defer db.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create a temporary table
				_, err = db.Exec(fmt.Sprintf("CREATE TABLE temp AS SELECT * FROM read_parquet('%s')", filePath))
				require.NoError(b, err)

				// Count rows to verify
				var count int64
				err = db.QueryRow("SELECT COUNT(*) FROM temp").Scan(&count)
				require.NoError(b, err)
				require.Equal(b, tf.expected, count)

				// Clean up
				_, err = db.Exec("DROP TABLE temp")
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkParquetWrite benchmarks writing different parquet files using DuckDB
func BenchmarkParquetWrite(b *testing.B) {
	skipInCI(b)
	testFiles := []struct {
		name string
		path string
	}{
		{"Simple", "simple.parquet"},
		{"Complex", "complex.parquet"},
		{"SparkOnTime", "spark-ontime.parquet"},
		{"SparkStore", "spark-store.parquet"},
		{"UserData", "userdata1.parquet"},
	}

	for _, tf := range testFiles {
		b.Run(tf.name, func(b *testing.B) {
			filePath := filepath.Join("/Users/thomasmcgeehan/flight/flight/data/parquet-testing", tf.path)

			// Create a new DuckDB connection
			db, err := sql.Open("duckdb", ":memory:")
			require.NoError(b, err)
			defer db.Close()

			// Load the data once
			_, err = db.Exec(fmt.Sprintf("CREATE TABLE source AS SELECT * FROM read_parquet('%s')", filePath))
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Write to a new Parquet file
				outPath := filepath.Join(b.TempDir(), fmt.Sprintf("output_%d.parquet", i))
				_, err = db.Exec(fmt.Sprintf("COPY source TO '%s' (FORMAT PARQUET)", outPath))
				require.NoError(b, err)
			}

			// Clean up
			_, err = db.Exec("DROP TABLE source")
			require.NoError(b, err)
		})
	}
}

// BenchmarkFlightStream benchmarks streaming data through Flight using DuckDB
func BenchmarkFlightStream(b *testing.B) {
	skipInCI(b)
	srv := setupBenchmarkServer(b)
	defer srv.Close(context.Background())

	testFiles := []struct {
		name string
		path string
	}{
		{"SparkOnTime", "spark-ontime.parquet"},
		{"SparkStore", "spark-store.parquet"},
		{"UserData", "userdata1.parquet"},
	}

	for _, tf := range testFiles {
		b.Run(tf.name, func(b *testing.B) {
			filePath := filepath.Join("/Users/thomasmcgeehan/flight/flight/data/parquet-testing", tf.path)

			// Create a new DuckDB connection
			db, err := sql.Open("duckdb", ":memory:")
			require.NoError(b, err)
			defer db.Close()

			// Load the data once
			_, err = db.Exec(fmt.Sprintf("CREATE TABLE source AS SELECT * FROM read_parquet('%s')", filePath))
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Write to a temporary Parquet file
				tempPath := filepath.Join(b.TempDir(), fmt.Sprintf("temp_%d.parquet", i))
				_, err = db.Exec(fmt.Sprintf("COPY source TO '%s' (FORMAT PARQUET)", tempPath))
				require.NoError(b, err)

				// Execute query through Flight
				query := fmt.Sprintf("SELECT * FROM read_parquet('%s')", filePath)
				_, recordChan, err := srv.QueryHandler().ExecuteQueryAndStream(context.Background(), query)
				require.NoError(b, err)

				// Process records
				for record := range recordChan {
					if record.Data != nil {
						record.Data.Release()
					}
				}

				os.Remove(tempPath)
			}

			// Clean up
			_, err = db.Exec("DROP TABLE source")
			require.NoError(b, err)
		})
	}
}

// BenchmarkCompressionFormats benchmarks different compression formats using DuckDB
func BenchmarkCompressionFormats(b *testing.B) {
	skipInCI(b)
	testFiles := []struct {
		name string
		path string
	}{
		{"Gzip", "lineitem-top10000.gzip.parquet"},
		{"Zstd", "zstd.parquet"},
		{"Uncompressed", "simple.parquet"},
	}

	for _, tf := range testFiles {
		b.Run(tf.name, func(b *testing.B) {
			filePath := filepath.Join("/Users/thomasmcgeehan/flight/flight/data/parquet-testing", tf.path)

			// Create a new DuckDB connection
			db, err := sql.Open("duckdb", ":memory:")
			require.NoError(b, err)
			defer db.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create a temporary table
				_, err = db.Exec(fmt.Sprintf("CREATE TABLE temp AS SELECT * FROM read_parquet('%s')", filePath))
				require.NoError(b, err)

				// Clean up
				_, err = db.Exec("DROP TABLE temp")
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkDataTypes benchmarks different data types using DuckDB
func BenchmarkDataTypes(b *testing.B) {
	skipInCI(b)
	testFiles := []struct {
		name string
		path string
	}{
		{"Timestamps", "timestamp.parquet"},
		{"TimeTZ", "timetz.parquet"},
		{"Unsigned", "unsigned.parquet"},
		{"Struct", "struct.parquet"},
		{"Map", "map.parquet"},
		{"List", "list_sort_segfault.parquet"},
		{"Decimal", "decimals.parquet"},
	}

	for _, tf := range testFiles {
		b.Run(tf.name, func(b *testing.B) {
			filePath := filepath.Join("/Users/thomasmcgeehan/flight/flight/data/parquet-testing", tf.path)

			// Create a new DuckDB connection
			db, err := sql.Open("duckdb", ":memory:")
			require.NoError(b, err)
			defer db.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create a temporary table
				_, err = db.Exec(fmt.Sprintf("CREATE TABLE temp AS SELECT * FROM read_parquet('%s')", filePath))
				require.NoError(b, err)

				// Clean up
				_, err = db.Exec("DROP TABLE temp")
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkConcurrentAccess benchmarks concurrent access to parquet files using DuckDB
func BenchmarkConcurrentAccess(b *testing.B) {
	skipInCI(b)
	testFiles := []struct {
		name string
		path string
	}{
		{"Simple", "simple.parquet"},
		{"Complex", "complex.parquet"},
		{"SparkOnTime", "spark-ontime.parquet"},
	}

	concurrencyLevels := []int{2, 4, 8, 16}

	for _, tf := range testFiles {
		for _, level := range concurrencyLevels {
			b.Run(fmt.Sprintf("%s_Concurrency=%d", tf.name, level), func(b *testing.B) {
				filePath := filepath.Join("/Users/thomasmcgeehan/flight/flight/data/parquet-testing", tf.path)

				// Create a connection pool
				pool := make(chan *sql.DB, level)
				for i := 0; i < level; i++ {
					db, err := sql.Open("duckdb", ":memory:")
					require.NoError(b, err)
					pool <- db
				}
				defer func() {
					for i := 0; i < level; i++ {
						db := <-pool
						db.Close()
					}
				}()

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					// Get a connection from the pool
					db := <-pool
					defer func() { pool <- db }()

					for pb.Next() {
						// Generate a unique table name for this goroutine
						tableName := fmt.Sprintf("temp_%d", time.Now().UnixNano())

						// Start a transaction
						tx, err := db.Begin()
						require.NoError(b, err)

						// Create a temporary table with unique name
						_, err = tx.Exec(fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet('%s')", tableName, filePath))
						if err != nil {
							if err := tx.Rollback(); err != nil {
								b.Fatalf("Failed to rollback transaction: %v", err)
							}
							continue
						}

						// Clean up
						_, err = tx.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
						if err != nil {
							if err := tx.Rollback(); err != nil {
								b.Fatalf("Failed to rollback transaction: %v", err)
							}
							continue
						}

						err = tx.Commit()
						require.NoError(b, err)
					}
				})
			})
		}
	}
}

// BenchmarkLargeDataMovement benchmarks Hatch's ability to move large volumes of data
func BenchmarkLargeDataMovement(b *testing.B) {
	skipInCI(b)
	srv := setupBenchmarkServer(b)
	defer srv.Close(context.Background())

	ctx := context.Background()

	// Create the large dataset through Flight
	createTableQuery := "CREATE TABLE large_data AS " +
		"SELECT " +
		"    i AS id, " +
		"    'name-' || i::VARCHAR AS name, " +
		"    random() * 1000 AS value, " +
		"    (now() + (i * interval '1 second'))::TIMESTAMP AS timestamp, " +
		"    random()::VARCHAR AS text_data, " +
		"    (random() > 0.5) AS bool_data, " +
		"    random() * 1.0 AS double_data " +
		"FROM range(1, 1000001) AS t(i)"
	cmd := &statementUpdate{query: createTableQuery}
	_, err := srv.DoPutCommandStatementUpdate(ctx, cmd)
	require.NoError(b, err)

	// Verify table exists and has data
	verifyQuery := "SELECT COUNT(*) FROM large_data"
	_, recordChan, err := srv.QueryHandler().ExecuteQueryAndStream(ctx, verifyQuery)
	require.NoError(b, err)

	var count int
	for record := range recordChan {
		if record.Data != nil {
			count = int(record.Data.Column(0).(*array.Int64).Value(0))
			record.Data.Release()
		}
	}
	require.Equal(b, 1000000, count, "Table should have 1M rows")

	// Test different batch sizes
	batchSizes := []int{10000, 50000, 100000, 500000, 1000000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize=%d", size), func(b *testing.B) {
			// First verify the query works
			verifyQuery := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT * FROM large_data LIMIT %d) t", size)
			_, recordChan, err := srv.QueryHandler().ExecuteQueryAndStream(ctx, verifyQuery)
			require.NoError(b, err)

			var testCount int
			for record := range recordChan {
				if record.Data != nil {
					testCount = int(record.Data.Column(0).(*array.Int64).Value(0))
					record.Data.Release()
				}
			}
			require.Equal(b, size, testCount, "Query should return correct number of rows")

			query := fmt.Sprintf("SELECT * FROM (SELECT * FROM large_data LIMIT %d) t", size)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Execute query through Flight
				_, recordChan, err := srv.QueryHandler().ExecuteQueryAndStream(ctx, query)
				require.NoError(b, err)

				// Process records and track total bytes
				var totalBytes int64
				for record := range recordChan {
					if record.Data != nil {
						// Calculate approximate size of the record
						for j := 0; j < int(record.Data.NumCols()); j++ {
							col := record.Data.Column(j)
							// Estimate size based on column type
							switch col.DataType().ID() {
							case arrow.INT64:
								totalBytes += int64(col.Len() * 8)
							case arrow.STRING:
								// For strings, estimate average length of 20 bytes
								totalBytes += int64(col.Len() * 20)
							case arrow.FLOAT64:
								totalBytes += int64(col.Len() * 8)
							case arrow.BOOL:
								totalBytes += int64(col.Len())
							case arrow.TIMESTAMP:
								totalBytes += int64(col.Len() * 8)
							default:
								// For unknown types, estimate 16 bytes per value
								totalBytes += int64(col.Len() * 16)
							}
						}
						record.Data.Release()
					}
				}

				// Report custom metric for bytes processed
				b.ReportMetric(float64(totalBytes), "bytes/op")
			}
		})
	}

	// Clean up
	dropQuery := "DROP TABLE large_data"
	cmd = &statementUpdate{query: dropQuery}
	_, err = srv.DoPutCommandStatementUpdate(ctx, cmd)
	require.NoError(b, err)
}

// startServer starts a Flight server on a random port and returns the server and address
func startServer(b *testing.B) (*server.FlightSQLServer, string) {
	// Find a free port
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(b, err, "Failed to find free port")
	addr := listener.Addr().String()
	listener.Close()

	// Create server config
	cfg := &config.Config{
		Database:          ":memory:",
		ConnectionTimeout: 30 * time.Second,
		QueryTimeout:      5 * time.Minute,
		ConnectionPool: config.ConnectionPoolConfig{
			MaxOpenConnections: 25,
			MaxIdleConnections: 5,
			ConnMaxLifetime:    30 * time.Minute,
			ConnMaxIdleTime:    10 * time.Minute,
			HealthCheckPeriod:  1 * time.Minute,
		},
		Cache: config.CacheConfig{
			Enabled: true,
			MaxSize: 100 * 1024 * 1024,
		},
		SafeCopy: false,
	}

	// Create server
	logger := zerolog.Nop()
	srv, err := server.New(cfg, logger, &noopMetrics{})
	require.NoError(b, err, "Failed to create server")

	// Create gRPC server and start it
	grpcServer := grpc.NewServer()
	srv.Register(grpcServer)

	// Create new listener for actual server
	listener, err = net.Listen("tcp", addr)
	require.NoError(b, err, "Failed to create listener")

	// Start server in background
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			b.Logf("Server stopped: %v", err)
		}
	}()

	return srv, addr
}

// connectClient creates a Flight client connected to the given address
func connectClient(b *testing.B, addr string) flight.Client {
	// Set up client connection
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	require.NoError(b, err, "Failed to dial server")

	// Create client
	client := flight.NewClientFromConn(conn, nil)
	return client
}

// BenchmarkFlightTransfer benchmarks data transfer through Flight
func BenchmarkFlightTransfer(b *testing.B) {
	// Start server
	srv, addr := startServer(b)
	defer srv.Close(context.Background())

	// Connect client
	client := connectClient(b, addr)

	// Create test data sizes
	sizes := []struct {
		name string
		rows int
	}{
		{"Small", 1000},
		{"Medium", 100000},
		{"Large", 1000000},
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Rows=%d", size.rows), func(b *testing.B) {
			ctx := context.Background()

			// Create test table with specified number of rows
			createQuery := fmt.Sprintf(`
				CREATE TABLE test_data AS 
				SELECT 
					i AS id,
					'name-' || i::VARCHAR AS name,
					random() * 1000 AS value,
					(now() + (i * interval '1 second'))::TIMESTAMP AS timestamp,
					random()::VARCHAR AS text_data,
					(random() > 0.5) AS bool_data,
					random() * 1.0 AS double_data
				FROM range(1, %d) AS t(i)
			`, size.rows)

			cmd := &statementUpdate{query: createQuery}
			_, err := srv.DoPutCommandStatementUpdate(ctx, cmd)
			require.NoError(b, err, "Failed to create test data")

			// Verify row count
			verifyQuery := "SELECT COUNT(*) FROM test_data"
			_, recordChan, err := srv.QueryHandler().ExecuteQueryAndStream(ctx, verifyQuery)
			require.NoError(b, err)

			var count int64
			for record := range recordChan {
				if record.Data != nil {
					count = record.Data.Column(0).(*array.Int64).Value(0)
					record.Data.Release()
				}
			}
			require.Equal(b, int64(size.rows), count, "Wrong number of rows created")

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Get FlightInfo
				desc := &flight.FlightDescriptor{
					Type: flight.DescriptorCMD,
					Cmd:  []byte("SELECT * FROM test_data"),
				}
				info, err := client.GetFlightInfo(ctx, desc)
				require.NoError(b, err)

				// Track metrics
				var (
					totalBytes int64
					totalRows  int64
					peakAlloc  uint64
				)

				// Get data stream
				for _, endpoint := range info.Endpoint {
					stream, err := client.DoGet(ctx, endpoint.Ticket)
					require.NoError(b, err)

					reader, err := flight.NewRecordReader(stream)
					require.NoError(b, err)
					defer reader.Release()

					for {
						record, err := reader.Read()
						if err == io.EOF {
							break
						}
						require.NoError(b, err)

						totalRows += record.NumRows()

						// Calculate memory size
						for j := 0; j < int(record.NumCols()); j++ {
							col := record.Column(j)
							totalBytes += int64(col.Data().Len())
						}

						// Track peak allocation
						var m runtime.MemStats
						runtime.ReadMemStats(&m)
						if m.Alloc > peakAlloc {
							peakAlloc = m.Alloc
						}

						record.Release()
					}
				}

				// Verify row count
				require.Equal(b, int64(size.rows), totalRows, "Wrong number of rows received")

				// Report metrics
				b.ReportMetric(float64(totalBytes)/float64(b.N), "bytes/op")
				b.ReportMetric(float64(totalRows)/float64(b.N), "rows/op")
				b.ReportMetric(float64(peakAlloc)/(1024*1024), "peak_mb")
				b.ReportMetric(float64(totalBytes)/(1024*1024*float64(b.Elapsed().Seconds())), "mb/sec")
			}

			// Clean up
			dropQuery := "DROP TABLE test_data"
			cmd = &statementUpdate{query: dropQuery}
			_, err = srv.DoPutCommandStatementUpdate(ctx, cmd)
			require.NoError(b, err, "Failed to drop test table")
		})
	}
}
