package memory

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTrackedAllocator(t *testing.T) {
	// Create a new tracked allocator for testing
	underlying := memory.NewGoAllocator()
	allocator := NewTrackedAllocator(underlying)

	t.Run("Allocate", func(t *testing.T) {
		// Test initial state
		assert.Equal(t, int64(0), allocator.BytesUsed())

		// Test allocation
		size := 1024
		buf := allocator.Allocate(size)
		require.NotNil(t, buf)
		assert.Equal(t, size, len(buf))
		assert.Equal(t, int64(size), allocator.BytesUsed())

		// Test multiple allocations
		buf2 := allocator.Allocate(size)
		require.NotNil(t, buf2)
		assert.Equal(t, size, len(buf2))
		assert.Equal(t, int64(size*2), allocator.BytesUsed())
	})

	t.Run("Reallocate", func(t *testing.T) {
		// Reset allocator state
		allocator = NewTrackedAllocator(underlying)

		// Test initial allocation
		initialSize := 512
		buf := allocator.Allocate(initialSize)
		require.NotNil(t, buf)
		assert.Equal(t, int64(initialSize), allocator.BytesUsed())

		// Test reallocation to larger size
		largerSize := 1024
		buf = allocator.Reallocate(largerSize, buf)
		require.NotNil(t, buf)
		assert.Equal(t, largerSize, len(buf))
		assert.Equal(t, int64(largerSize), allocator.BytesUsed())

		// Test reallocation to smaller size
		smallerSize := 256
		buf = allocator.Reallocate(smallerSize, buf)
		require.NotNil(t, buf)
		assert.Equal(t, smallerSize, len(buf))
		assert.Equal(t, int64(smallerSize), allocator.BytesUsed())
	})

	t.Run("Free", func(t *testing.T) {
		// Reset allocator state
		allocator = NewTrackedAllocator(underlying)

		// Test allocation and free
		size := 1024
		buf := allocator.Allocate(size)
		require.NotNil(t, buf)
		assert.Equal(t, int64(size), allocator.BytesUsed())

		allocator.Free(buf)
		assert.Equal(t, int64(0), allocator.BytesUsed())

		// Test multiple allocations and frees
		buf1 := allocator.Allocate(size)
		buf2 := allocator.Allocate(size)
		require.NotNil(t, buf1)
		require.NotNil(t, buf2)
		assert.Equal(t, int64(size*2), allocator.BytesUsed())

		allocator.Free(buf1)
		assert.Equal(t, int64(size), allocator.BytesUsed())

		allocator.Free(buf2)
		assert.Equal(t, int64(0), allocator.BytesUsed())
	})

	t.Run("GetAllocator", func(t *testing.T) {
		// Test that GetAllocator returns a non-nil allocator
		globalAlloc := GetAllocator()
		require.NotNil(t, globalAlloc)

		// Test that it's a tracked allocator
		_, ok := globalAlloc.(*TrackedAllocator)
		assert.True(t, ok, "GetAllocator should return a TrackedAllocator")
	})

	t.Run("ConcurrentAllocation", func(t *testing.T) {
		// Reset allocator state
		allocator = NewTrackedAllocator(underlying)

		// Test concurrent allocations
		const numGoroutines = 10
		const allocSize = 1024
		done := make(chan struct{})

		for i := 0; i < numGoroutines; i++ {
			go func() {
				buf := allocator.Allocate(allocSize)
				require.NotNil(t, buf)
				assert.Equal(t, allocSize, len(buf))
				allocator.Free(buf)
				done <- struct{}{}
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Verify final state
		assert.Equal(t, int64(0), allocator.BytesUsed())
	})
}
