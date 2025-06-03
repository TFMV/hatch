package pool

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestFastRecordPool(t *testing.T) {
	// Create a test schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	t.Run("NewRecordPool", func(t *testing.T) {
		pool := NewFastRecordPool(nil)
		assert.NotNil(t, pool)
	})

	t.Run("Get returns valid record", func(t *testing.T) {
		pool := NewFastRecordPool(nil)
		record := pool.Get(schema)
		defer record.Release()

		assert.NotNil(t, record)
		assert.Equal(t, schema, record.Schema())
		assert.Equal(t, int64(0), record.NumRows())
		assert.Equal(t, int64(3), record.NumCols())
	})

	t.Run("Put and Get cycle", func(t *testing.T) {
		pool := NewFastRecordPool(nil)

		// Create a record with data
		builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
		builder.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)

		record := builder.NewRecord()
		builder.Release()

		// Put it back in the pool
		pool.Put(record)

		// Get a new record
		newRecord := pool.Get(schema)
		defer newRecord.Release()

		// Should be empty but with same schema
		assert.Equal(t, int64(0), newRecord.NumRows())
		assert.Equal(t, schema, newRecord.Schema())
	})

	t.Run("Put with mismatched schema", func(t *testing.T) {
		pool := NewFastRecordPool(nil)

		// Create a record with different schema
		differentSchema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "different", Type: arrow.PrimitiveTypes.Int32},
			},
			nil,
		)
		builder := array.NewRecordBuilder(memory.NewGoAllocator(), differentSchema)
		builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
		record := builder.NewRecord()
		builder.Release()

		// Put should handle mismatched schema gracefully
		pool.Put(record)

		// Get should still return a valid record with original schema
		newRecord := pool.Get(schema)
		defer newRecord.Release()
		assert.Equal(t, schema, newRecord.Schema())
	})

	t.Run("Concurrent usage", func(t *testing.T) {
		pool := NewFastRecordPool(nil)
		const goroutines = 10
		done := make(chan bool)

		for i := 0; i < goroutines; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					record := pool.Get(schema)
					assert.NotNil(t, record)
					assert.Equal(t, schema, record.Schema())
					pool.Put(record)
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < goroutines; i++ {
			<-done
		}
	})
}

func BenchmarkFastRecordPool(b *testing.B) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b.Run("Get/Put cycle", func(b *testing.B) {
		pool := NewFastRecordPool(nil)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			record := pool.Get(schema)
			pool.Put(record)
		}
	})

	b.Run("Concurrent Get/Put", func(b *testing.B) {
		pool := NewFastRecordPool(nil)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				record := pool.Get(schema)
				pool.Put(record)
			}
		})
	})

	b.Run("Get/Put with data", func(b *testing.B) {
		pool := NewFastRecordPool(nil)
		builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
		builder.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
		record := builder.NewRecord()
		builder.Release()
		defer record.Release()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pool.Put(record)
			record = pool.Get(schema)
		}
	})
}
