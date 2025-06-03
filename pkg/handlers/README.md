# Hatch Handlers Package

The handlers package implements the Flight SQL protocol handlers, providing a clean interface between the Flight SQL protocol and the underlying database services. It follows a modular design pattern with clear separation of concerns and comprehensive error handling.

## Design Overview

### Core Interfaces

1. **QueryHandler**

   ```go
   type QueryHandler interface {
       ExecuteStatement(ctx context.Context, query string, transactionID string) (*arrow.Schema, <-chan flight.StreamChunk, error)
       ExecuteUpdate(ctx context.Context, query string, transactionID string) (int64, error)
       GetFlightInfo(ctx context.Context, query string) (*flight.FlightInfo, error)
       ExecuteQueryAndStream(ctx context.Context, query string) (*arrow.Schema, <-chan flight.StreamChunk, error)
   }
   ```

2. **MetadataHandler**

   ```go
   type MetadataHandler interface {
       GetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error)
       GetSchemas(ctx context.Context, catalog *string, schemaPattern *string) (*arrow.Schema, <-chan flight.StreamChunk, error)
       GetTables(ctx context.Context, catalog *string, schemaPattern *string, tablePattern *string, tableTypes []string, includeSchema bool) (*arrow.Schema, <-chan flight.StreamChunk, error)
       // ... additional metadata operations
   }
   ```

3. **TransactionHandler**

   ```go
   type TransactionHandler interface {
       Begin(ctx context.Context, readOnly bool) (string, error)
       Commit(ctx context.Context, transactionID string) error
       Rollback(ctx context.Context, transactionID string) error
   }
   ```

4. **PreparedStatementHandler**

   ```go
   type PreparedStatementHandler interface {
       Create(ctx context.Context, query string, transactionID string) (string, *arrow.Schema, error)
       Close(ctx context.Context, handle string) error
       ExecuteQuery(ctx context.Context, handle string, params arrow.Record) (*arrow.Schema, <-chan flight.StreamChunk, error)
       // ... additional prepared statement operations
   }
   ```

## Implementation Details

### Query Handler

The query handler manages SQL query execution and result streaming:

1. **Query Execution Flow**
   - Validates query parameters
   - Executes query through service layer
   - Streams results as Arrow record batches
   - Handles cancellation and cleanup

2. **Error Handling**
   - Maps service errors to Flight SQL errors
   - Provides detailed error messages
   - Maintains error context

3. **Performance Monitoring**
   - Tracks execution time
   - Records record counts
   - Monitors error rates

### Metadata Handler

Handles database metadata discovery:

1. **Catalog Operations**
   - Lists available catalogs
   - Retrieves schema information
   - Manages table metadata

2. **Schema Discovery**
   - Pattern-based filtering
   - Type information retrieval
   - Relationship discovery

### Transaction Handler

Manages database transactions:

1. **Transaction Lifecycle**
   - Transaction creation
   - Commit operations
   - Rollback handling

2. **State Management**
   - Transaction ID generation
   - State tracking
   - Cleanup procedures

### Prepared Statement Handler

Handles prepared statement operations:

1. **Statement Management**
   - Statement creation
   - Parameter binding
   - Resource cleanup

2. **Execution Flow**
   - Parameter validation
   - Statement execution
   - Result streaming

## Cross-Cutting Concerns

### Logging

```go
type Logger interface {
    Debug(msg string, keysAndValues ...interface{})
    Info(msg string, keysAndValues ...interface{})
    Warn(msg string, keysAndValues ...interface{})
    Error(msg string, keysAndValues ...interface{})
}
```

- Structured logging
- Context-aware logging
- Performance impact minimization

### Metrics

```go
type MetricsCollector interface {
    IncrementCounter(name string, tags ...string)
    RecordHistogram(name string, value float64, tags ...string)
    RecordGauge(name string, value float64, tags ...string)
    StartTimer(name string) Timer
}
```

- Operation timing
- Error tracking
- Resource usage monitoring

## Error Handling

The package implements a comprehensive error handling strategy:

1. **Error Types**
   - Flight SQL specific errors
   - Service layer errors
   - Protocol errors

2. **Error Mapping**
   - Consistent error codes
   - Detailed error messages
   - Context preservation

3. **Error Recovery**
   - Graceful degradation
   - Resource cleanup
   - State recovery

## Performance Considerations

1. **Resource Management**
   - Arrow memory allocation
   - Connection pooling
   - Resource cleanup

2. **Streaming Optimization**
   - Chunked result delivery
   - Backpressure handling
   - Memory efficiency

3. **Concurrency**
   - Thread safety
   - Context propagation
   - Cancellation support

## Usage Example

```go
// Create handlers with dependencies
queryHandler := handlers.NewQueryHandler(
    queryService,
    memory.DefaultAllocator,
    logger,
    metricsCollector,
)

// Execute a query
schema, chunks, err := queryHandler.ExecuteStatement(ctx, "SELECT * FROM users", "")
if err != nil {
    log.Printf("Query execution failed: %v", err)
    return
}

// Process results
for chunk := range chunks {
    // Process Arrow record batch
    processRecord(chunk.Data)
}
```

## Best Practices

1. **Handler Configuration**
   - Proper service initialization
   - Resource allocation
   - Dependency injection

2. **Error Handling**
   - Consistent error mapping
   - Proper error propagation
   - Resource cleanup

3. **Performance**
   - Monitor execution times
   - Track resource usage
   - Implement backpressure

4. **Security**
   - Input validation
   - Access control
   - Resource limits
