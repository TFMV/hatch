package pool

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ByteBufferPool manages pools of []byte buffers for different size classes
type ByteBufferPool struct {
	pools map[int]*sync.Pool
	mu    sync.RWMutex
}

// NewByteBufferPool creates a new ByteBufferPool
func NewByteBufferPool() *ByteBufferPool {
	return &ByteBufferPool{
		pools: make(map[int]*sync.Pool),
	}
}

// Get returns a buffer of at least the requested size
func (p *ByteBufferPool) Get(size int) []byte {
	// Round up to nearest power of 2 for better reuse
	size = nextPowerOfTwo(size)

	p.mu.RLock()
	pool, exists := p.pools[size]
	p.mu.RUnlock()

	if !exists {
		p.mu.Lock()
		pool, exists = p.pools[size]
		if !exists {
			pool = &sync.Pool{
				New: func() interface{} {
					buf := make([]byte, 0, size)
					return &buf
				},
			}
			p.pools[size] = pool
		}
		p.mu.Unlock()
	}

	return *pool.Get().(*[]byte)
}

// Put returns a buffer to the pool
func (p *ByteBufferPool) Put(buf []byte) {
	if cap(buf) == 0 {
		return
	}

	size := nextPowerOfTwo(cap(buf))
	p.mu.RLock()
	pool := p.pools[size]
	p.mu.RUnlock()

	if pool != nil {
		buf = buf[:0] // Reset slice but keep capacity
		pool.Put(&buf)
	}
}

// RecordBuilderPool manages a pool of array.RecordBuilder instances
type RecordBuilderPool struct {
	pool      sync.Pool
	allocator memory.Allocator
}

// NewRecordBuilderPool creates a new RecordBuilderPool
func NewRecordBuilderPool(allocator memory.Allocator) *RecordBuilderPool {
	return &RecordBuilderPool{
		allocator: allocator,
	}
}

// Get returns a RecordBuilder for the given schema
func (p *RecordBuilderPool) Get(schema *arrow.Schema) *array.RecordBuilder {
	v := p.pool.Get()
	if v == nil {
		return array.NewRecordBuilder(p.allocator, schema)
	}

	rb := v.(*array.RecordBuilder)
	if !rb.Schema().Equal(schema) {
		rb.Release()
		return array.NewRecordBuilder(p.allocator, schema)
	}

	return rb
}

// Put returns a RecordBuilder to the pool
func (p *RecordBuilderPool) Put(rb *array.RecordBuilder) {
	if rb != nil {
		p.pool.Put(rb)
	}
}

// nextPowerOfTwo returns the next power of 2 >= n
func nextPowerOfTwo(n int) int {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
