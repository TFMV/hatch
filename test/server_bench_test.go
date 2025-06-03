package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/TFMV/flight/cmd/server/config"
	"github.com/TFMV/flight/cmd/server/server"
	"github.com/TFMV/flight/pkg/infrastructure/pool"
)

// noopMetrics is a no-op implementation of server.MetricsCollector for benchmarks
type noopMetrics struct{}

func (n *noopMetrics) IncrementCounter(name string, labels ...string)               {}
func (n *noopMetrics) RecordHistogram(name string, value float64, labels ...string) {}
func (n *noopMetrics) RecordGauge(name string, value float64, labels ...string)     {}
func (n *noopMetrics) StartTimer(name string) server.Timer                          { return &noopTimer{} }

type noopTimer struct{}

func (n *noopTimer) Stop() float64 { return 0 }

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

// generateLargeDataset creates a large Arrow record batch for benchmarking
func generateLargeDataset(b *testing.B, size int) arrow.Record {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	for i := 0; i < size; i++ {
		builder.Field(0).(*array.Int64Builder).Append(int64(i))
		builder.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("name-%d", i))
		builder.Field(2).(*array.Float64Builder).Append(float64(i) * 1.5)
		builder.Field(3).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UnixNano()))
	}

	return builder.NewRecord()
}

// setupTestTables creates the necessary test tables for benchmarking
func setupTestTables(b *testing.B, srv *server.FlightSQLServer) {
	ctx := context.Background()

	// Create large_table
	createTableQuery := `
		CREATE TABLE large_table (
			id BIGINT,
			name VARCHAR,
			value DOUBLE,
			timestamp TIMESTAMP
		)
	`
	b.Logf("Executing create table query: %s", createTableQuery)
	cmd := &statementUpdate{query: createTableQuery}
	_, err := srv.DoPutCommandStatementUpdate(ctx, cmd)
	if err != nil {
		b.Fatalf("Failed to create large_table: %v", err)
	}

	// Insert test data using DuckDB's range function
	insertQuery := `INSERT INTO large_table SELECT i AS id, 'name-' || i::VARCHAR AS name, random() * 1000 AS value, now() AS timestamp FROM range(1, 100001) AS t(i)`
	b.Logf("Executing insert query: %s", insertQuery)
	cmd = &statementUpdate{query: insertQuery}
	_, err = srv.DoPutCommandStatementUpdate(ctx, cmd)
	if err != nil {
		b.Logf("Full error details: %+v", err)
		b.Fatalf("Failed to insert test data: %v", err)
	}
}

// BenchmarkGetFlightInfoStatement benchmarks the GetFlightInfoStatement method
func BenchmarkGetFlightInfoStatement(b *testing.B) {
	srv := setupBenchmarkServer(b)
	defer srv.Close(context.Background())

	// Setup test tables
	setupTestTables(b, srv)

	ctx := context.Background()
	desc := &flight.FlightDescriptor{}

	queries := []string{
		"SELECT 1",
		"SELECT * FROM large_table LIMIT 1000",
		"SELECT * FROM large_table WHERE id > 1000 AND value < 500.0 LIMIT 1000",
		"SELECT id, ANY_VALUE(name) as name, ANY_VALUE(value) as value, ANY_VALUE(timestamp) as timestamp FROM large_table GROUP BY id HAVING COUNT(*) > 1 LIMIT 1000",
	}

	for _, query := range queries {
		b.Run(fmt.Sprintf("Query=%s", query), func(b *testing.B) {
			cmd := &statementQuery{query: query}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				info, err := srv.GetFlightInfoStatement(ctx, cmd, desc)
				if err != nil {
					b.Fatalf("GetFlightInfoStatement failed: %v", err)
				}
				require.NotNil(b, info)
			}
		})
	}
}

// BenchmarkDoGetStatement benchmarks the DoGetStatement method
func BenchmarkDoGetStatement(b *testing.B) {
	srv := setupBenchmarkServer(b)
	defer srv.Close(context.Background())

	ctx := context.Background()
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size=%d", size), func(b *testing.B) {
			query := fmt.Sprintf("SELECT * FROM generate_series(1, %d)", size)
			ticket := &statementQueryTicket{handle: []byte(query)}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				schema, chunks, err := srv.DoGetStatement(ctx, ticket)
				if err != nil {
					b.Fatalf("DoGetStatement failed: %v", err)
				}
				require.NotNil(b, schema)

				// Consume all chunks to measure full processing time
				for chunk := range chunks {
					if chunk.Data != nil {
						chunk.Data.Release()
					}
				}
			}
		})
	}
}

// BenchmarkRecordBuilderPool benchmarks the record builder pool performance
func BenchmarkRecordBuilderPool(b *testing.B) {
	allocator := memory.NewGoAllocator()
	builderPool := pool.NewRecordBuilderPool(allocator)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b.Run("Get/Put", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			builder := builderPool.Get(schema)
			builderPool.Put(builder)
		}
	})

	b.Run("BuildRecord", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			builder := builderPool.Get(schema)
			builder.Field(0).(*array.Int64Builder).Append(int64(i))
			builder.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("name-%d", i))
			builder.Field(2).(*array.Float64Builder).Append(float64(i) * 1.5)
			record := builder.NewRecord()
			record.Release()
			builderPool.Put(builder)
		}
	})
}

// BenchmarkByteBufferPool benchmarks the byte buffer pool performance
func BenchmarkByteBufferPool(b *testing.B) {
	bufferPool := pool.NewByteBufferPool()
	sizes := []int{64, 1024, 4096, 16384}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size=%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf := bufferPool.Get(size)
				bufferPool.Put(buf)
			}
		})
	}
}

// BenchmarkParallelQueryExecution benchmarks parallel query execution
func BenchmarkParallelQueryExecution(b *testing.B) {
	srv := setupBenchmarkServer(b)
	defer srv.Close(context.Background())

	ctx := context.Background()
	query := "SELECT * FROM generate_series(1, 1000)"
	ticket := &statementQueryTicket{handle: []byte(query)}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			schema, chunks, err := srv.DoGetStatement(ctx, ticket)
			if err != nil {
				b.Fatalf("DoGetStatement failed: %v", err)
			}
			require.NotNil(b, schema)

			for chunk := range chunks {
				if chunk.Data != nil {
					chunk.Data.Release()
				}
			}
		}
	})
}

// BenchmarkMemoryUsage benchmarks memory usage patterns
func BenchmarkMemoryUsage(b *testing.B) {
	srv := setupBenchmarkServer(b)
	defer srv.Close(context.Background())

	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size=%d", size), func(b *testing.B) {
			record := generateLargeDataset(b, size)
			defer record.Release()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				builder := array.NewRecordBuilder(memory.NewGoAllocator(), record.Schema())
				for j := 0; j < int(record.NumCols()); j++ {
					col := record.Column(j)
					for k := 0; k < int(col.Len()); k++ {
						if !col.IsNull(k) {
							switch arr := col.(type) {
							case *array.Int64:
								builder.Field(j).(*array.Int64Builder).Append(arr.Value(k))
							case *array.String:
								builder.Field(j).(*array.StringBuilder).Append(arr.Value(k))
							case *array.Float64:
								builder.Field(j).(*array.Float64Builder).Append(arr.Value(k))
							case *array.Timestamp:
								builder.Field(j).(*array.TimestampBuilder).Append(arr.Value(k))
							}
						}
					}
				}
				newRecord := builder.NewRecord()
				newRecord.Release()
				builder.Release()
			}
		})
	}
}
