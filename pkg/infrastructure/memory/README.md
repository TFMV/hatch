# Porter Memory Infrastructure

The memory infrastructure package provides a thread-safe memory allocation tracking system built on top of Apache Arrow's memory allocator. It enables precise monitoring of memory usage across the application while maintaining compatibility with Arrow's memory management system.

## Design Overview

### Core Components

1. **TrackedAllocator**

   ```go
   type TrackedAllocator struct {
       underlying memory.Allocator
       bytesUsed  atomic.Int64
   }
   ```

2. **Global Allocator**

   ```go
   var globalAllocator = NewTrackedAllocator(memory.NewGoAllocator())
   ```

## Implementation Details

### Memory Tracking

The `TrackedAllocator` wraps Apache Arrow's memory allocator to provide:

1. **Byte Usage Tracking**
   - Atomic counter for total allocated bytes
   - Thread-safe operations
   - Real-time memory usage monitoring

2. **Memory Operations**

   ```go
   // Allocation
   func (a *TrackedAllocator) Allocate(size int) []byte
   
   // Reallocation
   func (a *TrackedAllocator) Reallocate(size int, b []byte) []byte
   
   // Deallocation
   func (a *TrackedAllocator) Free(b []byte)
   
   // Usage Query
   func (a *TrackedAllocator) BytesUsed() int64
   ```

### Thread Safety

The implementation ensures thread safety through:

1. **Atomic Operations**
   - Atomic counter for byte tracking
   - No locks required for basic operations
   - Safe concurrent access

2. **Concurrent Usage**
   - Safe for multiple goroutines
   - Consistent memory accounting
   - No race conditions

## Usage

### Basic Usage

```go
// Get the global tracked allocator
allocator := memory.GetAllocator()

// Allocate memory
buf := allocator.Allocate(1024)

// Check memory usage
bytesUsed := allocator.(*memory.TrackedAllocator).BytesUsed()

// Free memory
allocator.Free(buf)
```

### Custom Allocator

```go
// Create a custom tracked allocator
underlying := memory.NewGoAllocator()
trackedAllocator := memory.NewTrackedAllocator(underlying)

// Use with Arrow operations
builder := array.NewInt32Builder(trackedAllocator)
```

## Memory Management

### Allocation Patterns

1. **Direct Allocation**
   - Simple memory allocation
   - Immediate tracking
   - Direct byte counting

2. **Reallocation**
   - Size adjustment tracking
   - Delta calculation
   - Accurate byte accounting

3. **Deallocation**
   - Proper cleanup
   - Usage reduction
   - Resource release

### Memory Monitoring

1. **Usage Tracking**
   - Real-time byte counting
   - Allocation patterns
   - Memory pressure detection

2. **Performance Impact**
   - Minimal overhead
   - Atomic operations
   - Efficient tracking

## Best Practices

1. **Memory Management**
   - Always free allocated memory
   - Monitor usage patterns
   - Set appropriate limits

2. **Concurrency**
   - Use atomic operations
   - Avoid manual synchronization
   - Handle concurrent access

3. **Resource Cleanup**
   - Proper deallocation
   - Usage verification
   - Leak prevention

4. **Monitoring**
   - Track usage patterns
   - Set alerts
   - Monitor trends

## Testing

The package includes comprehensive tests covering:

1. **Basic Operations**
   - Allocation
   - Reallocation
   - Deallocation

2. **Concurrency**
   - Multiple goroutines
   - Atomic operations
   - Thread safety

3. **Edge Cases**
   - Zero-size allocations
   - Large allocations
   - Reallocation patterns

## Performance Considerations

1. **Overhead**
   - Minimal tracking overhead
   - Atomic operations
   - Efficient byte counting

2. **Memory Efficiency**
   - Accurate tracking
   - Proper cleanup
   - Resource optimization

3. **Concurrency**
   - Lock-free operations
   - Scalable design
   - Thread safety

## Integration

### Arrow Integration

```go
// Use with Arrow builders
builder := array.NewInt32Builder(memory.GetAllocator())

// Use with Arrow arrays
arr := array.NewInt32Data(
    array.NewData(
        arrow.PrimitiveTypes.Int32,
        10,
        []*memory.Buffer{nil, buf},
        nil,
        0,
        0,
    ),
)
```

### Custom Integration

```go
// Create custom tracked allocator
type CustomAllocator struct {
    *memory.TrackedAllocator
    // Additional fields
}

// Implement custom allocation logic
func (a *CustomAllocator) Allocate(size int) []byte {
    // Custom allocation logic
    return a.TrackedAllocator.Allocate(size)
}
```
