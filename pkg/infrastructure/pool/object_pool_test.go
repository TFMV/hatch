package pool

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestByteBufferPool(t *testing.T) {
	pool := NewByteBufferPool()
	require.NotNil(t, pool)

	t.Run("Get", func(t *testing.T) {
		// Test getting buffers of different sizes
		sizes := []int{64, 128, 256, 512, 1024}
		for _, size := range sizes {
			buf := pool.Get(size)
			require.NotNil(t, buf)
			assert.Equal(t, 0, len(buf))
			assert.GreaterOrEqual(t, cap(buf), size)
		}
	})

	t.Run("Put", func(t *testing.T) {
		// Test putting buffers back
		size := 1024
		buf := pool.Get(size)
		require.NotNil(t, buf)

		// Fill the buffer
		buf = append(buf, make([]byte, size)...)
		require.Equal(t, size, len(buf))

		// Put it back
		pool.Put(buf)

		// Get it again
		newBuf := pool.Get(size)
		require.NotNil(t, newBuf)
		assert.Equal(t, 0, len(newBuf))
		assert.GreaterOrEqual(t, cap(newBuf), size)
	})

	t.Run("PowerOfTwo", func(t *testing.T) {
		// Test that buffer sizes are rounded up to powers of 2
		tests := []struct {
			requested int
			expected  int
		}{
			{1, 1},
			{2, 2},
			{3, 4},
			{5, 8},
			{9, 16},
			{17, 32},
			{33, 64},
			{65, 128},
			{129, 256},
			{257, 512},
			{513, 1024},
		}

		for _, tt := range tests {
			buf := pool.Get(tt.requested)
			assert.GreaterOrEqual(t, cap(buf), tt.expected)
		}
	})

	t.Run("ZeroCapacity", func(t *testing.T) {
		// Test that zero capacity buffers are handled correctly
		var buf []byte
		pool.Put(buf) // Should not panic
	})
}

func TestRecordBuilderPool(t *testing.T) {
	// Create a test schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "string", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// Create a test allocator
	allocator := memory.NewGoAllocator()
	pool := NewRecordBuilderPool(allocator)
	require.NotNil(t, pool)

	t.Run("Get", func(t *testing.T) {
		// Test getting a new builder
		builder := pool.Get(schema)
		require.NotNil(t, builder)
		assert.True(t, builder.Schema().Equal(schema))
	})

	t.Run("Put", func(t *testing.T) {
		// Get a builder
		builder := pool.Get(schema)
		require.NotNil(t, builder)

		// Build a record
		builder.Field(0).(*array.Int32Builder).Append(42)
		builder.Field(1).(*array.StringBuilder).Append("test")
		record := builder.NewRecord()
		require.NotNil(t, record)
		record.Release()

		// Put the builder back
		pool.Put(builder)

		// Get it again
		newBuilder := pool.Get(schema)
		require.NotNil(t, newBuilder)
		assert.True(t, newBuilder.Schema().Equal(schema))
	})

	t.Run("DifferentSchema", func(t *testing.T) {
		// Get a builder
		builder := pool.Get(schema)
		require.NotNil(t, builder)

		// Create a different schema
		diffSchema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "float64", Type: arrow.PrimitiveTypes.Float64},
			},
			nil,
		)

		// Put the builder back
		pool.Put(builder)

		// Get a builder with different schema
		newBuilder := pool.Get(diffSchema)
		require.NotNil(t, newBuilder)
		assert.True(t, newBuilder.Schema().Equal(diffSchema))
	})

	t.Run("NilBuilder", func(t *testing.T) {
		// Test that nil builders are handled correctly
		pool.Put(nil) // Should not panic
	})
}

func TestNextPowerOfTwo(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{15, 16},
		{16, 16},
		{17, 32},
		{31, 32},
		{32, 32},
		{33, 64},
		{63, 64},
		{64, 64},
		{65, 128},
		{127, 128},
		{128, 128},
		{129, 256},
		{255, 256},
		{256, 256},
		{257, 512},
		{511, 512},
		{512, 512},
		{513, 1024},
		{1023, 1024},
		{1024, 1024},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := nextPowerOfTwo(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}
