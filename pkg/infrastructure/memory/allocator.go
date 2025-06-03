package memory

import (
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

var (
	// globalAllocator is the singleton instance of the tracked allocator
	globalAllocator = NewTrackedAllocator(memory.NewGoAllocator())
)

// TrackedAllocator wraps a memory.Allocator and tracks total allocated bytes
type TrackedAllocator struct {
	underlying memory.Allocator
	bytesUsed  atomic.Int64
}

// NewTrackedAllocator creates a new TrackedAllocator
func NewTrackedAllocator(underlying memory.Allocator) *TrackedAllocator {
	return &TrackedAllocator{
		underlying: underlying,
	}
}

// Allocate implements memory.Allocator interface
func (a *TrackedAllocator) Allocate(size int) []byte {
	a.bytesUsed.Add(int64(size))
	return a.underlying.Allocate(size)
}

// Reallocate implements memory.Allocator interface
func (a *TrackedAllocator) Reallocate(size int, b []byte) []byte {
	oldSize := len(b)
	a.bytesUsed.Add(int64(size - oldSize))
	return a.underlying.Reallocate(size, b)
}

// Free implements memory.Allocator interface
func (a *TrackedAllocator) Free(b []byte) {
	a.bytesUsed.Add(-int64(len(b)))
	a.underlying.Free(b)
}

// BytesUsed returns the current number of bytes allocated
func (a *TrackedAllocator) BytesUsed() int64 {
	return a.bytesUsed.Load()
}

// GetAllocator returns the global tracked allocator instance
func GetAllocator() memory.Allocator {
	return globalAllocator
}
