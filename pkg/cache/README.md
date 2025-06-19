# Porter Cache Package

The cache package provides an in-memory caching system specifically designed for Apache Arrow record batches. It implements a thread-safe, size-bounded cache with LRU (Least Recently Used) eviction policy and comprehensive statistics tracking.

## Design Overview

### Core Components

1. **Cache Interface**

   ```go
   type Cache interface {
       Get(ctx context.Context, key string) (arrow.Record, error)
       Put(ctx context.Context, key string, record arrow.Record) error
       Delete(ctx context.Context, key string) error
       Clear(ctx context.Context) error
       Close() error
   }
   ```

2. **MemoryCache Implementation**
   - Thread-safe in-memory storage using `sync.RWMutex`
   - Size-bounded with configurable maximum size
   - LRU eviction policy
   - Arrow memory allocator integration

3. **CacheEntry Structure**

   ```go
   type CacheEntry struct {
       Record    arrow.Record
       CreatedAt time.Time
       LastUsed  time.Time
       Size      int64
   }
   ```

4. **Configuration System**

   ```go
   type Config struct {
       MaxSize    int64
       TTL        time.Duration
       Allocator  memory.Allocator
       EnableStats bool
   }
   ```

5. **Statistics Collection**

   ```go
   type Stats struct {
       Hits        uint64
       Misses      uint64
       Evictions   uint64
       Size        int64
       LastUpdated time.Time
   }
   ```

## Data Flow

### 1. Cache Initialization

```go
config := cache.DefaultConfig().
    WithMaxSize(100 * 1024 * 1024).  // 100MB
    WithTTL(5 * time.Minute).
    WithStats(true)

cache := cache.NewMemoryCache(config.MaxSize, config.Allocator)
```

### 2. Cache Operations

#### Storing Data (Put)

1. Calculate record size (including schema overhead)
2. Check if record fits in cache
3. Evict entries if necessary (LRU policy)
4. Store record with metadata
5. Update cache size
6. Record statistics

#### Retrieving Data (Get)

1. Look up entry by key
2. Update last used timestamp
3. Retain record (increment reference count)
4. Record hit/miss statistics
5. Return record

#### Eviction Process

1. Identify least recently used entry
2. Release record (decrement reference count)
3. Remove entry from cache
4. Update cache size
5. Record eviction statistics

## Memory Management

### Arrow Integration

- Uses Apache Arrow's memory allocator
- Properly manages record reference counts
- Handles Arrow buffer lifecycle

### Size Calculation

- Includes all Arrow buffers
- Accounts for schema overhead
- Tracks child data structures

## Thread Safety

- Read-Write mutex for concurrent access
- Atomic operations for statistics
- Safe record reference counting

## Performance Considerations

1. **Memory Efficiency**
   - Configurable maximum size
   - LRU eviction policy
   - Proper Arrow memory management

2. **Concurrency**
   - Read-Write lock for better concurrent access
   - Atomic statistics updates
   - Thread-safe operations

3. **Statistics**
   - Hit/miss tracking
   - Eviction monitoring
   - Size tracking
   - Hit rate calculation

## Usage Example

```go
// Create cache with default configuration
config := cache.DefaultConfig()
cache := cache.NewMemoryCache(config.MaxSize, config.Allocator)

// Store a record
err := cache.Put(ctx, "key1", record)
if err != nil {
    log.Printf("Failed to cache record: %v", err)
}

// Retrieve a record
cachedRecord, err := cache.Get(ctx, "key1")
if err != nil {
    log.Printf("Failed to retrieve record: %v", err)
}

// Get cache statistics
stats := cache.GetStats()
hitRate := cache.HitRate()
```

## Best Practices

1. **Configuration**
   - Set appropriate maximum size
   - Configure TTL based on data freshness requirements
   - Enable statistics for monitoring

2. **Memory Management**
   - Monitor cache size
   - Watch eviction rates
   - Adjust maximum size based on usage patterns

3. **Error Handling**
   - Handle cache misses gracefully
   - Monitor error rates
   - Implement fallback strategies

4. **Monitoring**
   - Track hit rates
   - Monitor eviction patterns
   - Watch memory usage
