# Hatch Pool Infrastructure

The pool infrastructure package provides efficient resource pooling for the Flight SQL server, including database connections and object pools for memory optimization.

## Design Overview

### Core Components

1. **Connection Pool**

   ```go
   type ConnectionPool interface {
       Get(ctx context.Context) (*sql.DB, error)
       Stats() PoolStats
       HealthCheck(ctx context.Context) error
       Close() error
   }
   ```

2. **Object Pools**

   ```go
   type ByteBufferPool struct {
       pools map[int]*sync.Pool
       mu    sync.RWMutex
   }

   type RecordBuilderPool struct {
       pool      sync.Pool
       allocator memory.Allocator
   }
   ```

## Implementation Details

### Connection Pool

The connection pool manages database connections with the following features:

1. **Configuration**

   ```go
   type Config struct {
       DSN                string
       MaxOpenConnections int
       MaxIdleConnections int
       ConnMaxLifetime    time.Duration
       ConnMaxIdleTime    time.Duration
       HealthCheckPeriod  time.Duration
       ConnectionTimeout  time.Duration
   }
   ```

2. **Statistics**

   ```go
   type PoolStats struct {
       OpenConnections   int
       InUse             int
       Idle              int
       WaitCount         int64
       WaitDuration      time.Duration
       MaxIdleClosed     int64
       MaxLifetimeClosed int64
       LastHealthCheck   time.Time
       HealthCheckStatus string
   }
   ```

3. **Health Monitoring**
   - Periodic health checks
   - Connection validation
   - Query execution testing
   - Status tracking

### Object Pools

1. **Byte Buffer Pool**
   - Size-class based pooling
   - Power-of-two sizing
   - Thread-safe operations
   - Zero-allocation reuse

2. **Record Builder Pool**
   - Schema-aware pooling
   - Memory allocator integration
   - Automatic schema validation
   - Resource cleanup

## Usage

### Connection Pool

```go
// Create pool configuration
config := pool.Config{
    DSN:                "duckdb.db",
    MaxOpenConnections: 25,
    MaxIdleConnections: 5,
    ConnMaxLifetime:    30 * time.Minute,
    ConnMaxIdleTime:    10 * time.Minute,
    HealthCheckPeriod:  time.Minute,
    ConnectionTimeout:  30 * time.Second,
}

// Create pool
pool, err := pool.New(config, logger)
if err != nil {
    log.Fatal(err)
}
defer pool.Close()

// Get connection
db, err := pool.Get(ctx)
if err != nil {
    log.Fatal(err)
}

// Check pool stats
stats := pool.Stats()
fmt.Printf("Active connections: %d\n", stats.InUse)
```

### Object Pools

```go
// Create byte buffer pool
bufferPool := pool.NewByteBufferPool()

// Get buffer
buf := bufferPool.Get(1024)
defer bufferPool.Put(buf)

// Create record builder pool
builderPool := pool.NewRecordBuilderPool(memory.DefaultAllocator)

// Get builder
builder := builderPool.Get(schema)
defer builderPool.Put(builder)
```

## Best Practices

1. **Connection Management**
   - Set appropriate pool sizes
   - Monitor connection usage
   - Implement proper error handling
   - Use context for timeouts

2. **Resource Cleanup**
   - Always close connections
   - Return objects to pools
   - Handle cleanup in defer
   - Monitor resource leaks

3. **Performance**
   - Size pools appropriately
   - Monitor pool statistics
   - Adjust timeouts as needed
   - Handle backpressure

4. **Monitoring**
   - Track pool statistics
   - Monitor health checks
   - Log connection issues
   - Alert on pool exhaustion

## Testing

The package includes comprehensive tests:

1. **Connection Pool Tests**
   - Connection lifecycle
   - Health checks
   - Error handling
   - Resource cleanup

2. **Object Pool Tests**
   - Buffer allocation
   - Record builder reuse
   - Thread safety
   - Memory management

## Performance Considerations

1. **Memory Usage**
   - Pool sizing
   - Buffer reuse
   - Memory fragmentation
   - Resource limits

2. **CPU Impact**
   - Pool synchronization
   - Health checks
   - Connection validation
   - Object creation

3. **Concurrency**
   - Thread safety
   - Lock contention
   - Connection limits
   - Resource sharing

## Integration Examples

### Custom Connection Wrapper

```go
type CustomConnection struct {
    *pool.ConnectionWrapper
    // Additional fields
}

func (c *CustomConnection) ExecuteWithRetry(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
    // Implement retry logic
    return c.Execute(ctx, query, args...)
}
```

### Pool Monitoring

```go
type PoolMonitor struct {
    pool   pool.ConnectionPool
    logger zerolog.Logger
}

func (m *PoolMonitor) Monitor(ctx context.Context) {
    ticker := time.NewTicker(time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            stats := m.pool.Stats()
            m.logger.Info().
                Int("open", stats.OpenConnections).
                Int("in_use", stats.InUse).
                Int("idle", stats.Idle).
                Msg("Pool statistics")
        }
    }
}
```

### Custom Object Pool

```go
type CustomObjectPool struct {
    pool sync.Pool
}

func (p *CustomObjectPool) Get() *CustomObject {
    v := p.pool.Get()
    if v == nil {
        return NewCustomObject()
    }
    return v.(*CustomObject)
}

func (p *CustomObjectPool) Put(obj *CustomObject) {
    obj.Reset()
    p.pool.Put(obj)
}
```
