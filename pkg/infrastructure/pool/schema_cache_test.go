package pool

import (
	"fmt"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func createTestSchema(name string) *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: name, Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)
}

func TestSchemaCache(t *testing.T) {
	cache := NewSchemaCache(2)

	// Create test schemas
	schema1 := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col1", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)
	schema2 := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col2", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	schema3 := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col3", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	t.Run("Basic Operations", func(t *testing.T) {
		// Test Put and Get
		cache.Put(schema1)
		s, ok := cache.Get(schema1)
		assert.True(t, ok)
		assert.Equal(t, schema1, s)

		// Test non-existent schema
		s, ok = cache.Get(schema2)
		assert.False(t, ok)
		assert.Nil(t, s)
	})

	t.Run("LRU Eviction", func(t *testing.T) {
		// Fill cache
		cache.Put(schema1)
		cache.Put(schema2)
		assert.Equal(t, 2, cache.Size())

		// Add third schema, should evict schema1
		cache.Put(schema3)
		assert.Equal(t, 2, cache.Size())

		// schema1 should be evicted
		s, ok := cache.Get(schema1)
		assert.False(t, ok)
		assert.Nil(t, s)

		// schema2 and schema3 should still be present
		s, ok = cache.Get(schema2)
		assert.True(t, ok)
		assert.Equal(t, schema2, s)

		s, ok = cache.Get(schema3)
		assert.True(t, ok)
		assert.Equal(t, schema3, s)
	})

	t.Run("Cache Stats", func(t *testing.T) {
		cache.Clear()
		cache.Put(schema1)
		cache.Put(schema2)

		// Get schema1 multiple times
		for i := 0; i < 3; i++ {
			s, ok := cache.Get(schema1)
			assert.True(t, ok)
			assert.Equal(t, schema1, s)
		}

		stats := cache.Stats()
		assert.Equal(t, 2, stats.Size)
		assert.Equal(t, 2, stats.Cap)
		assert.Equal(t, int64(5), stats.TotalHits) // 1 from Put + 3 from Gets + 1 from Put refresh
		assert.True(t, stats.OldestAge > 0)
	})

	t.Run("Concurrent Access", func(t *testing.T) {
		cache.Clear()
		var wg sync.WaitGroup
		iters := 1000

		// Concurrent puts
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				cache.Put(schema1)
			}
		}()

		// Concurrent gets
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				s, ok := cache.Get(schema1)
				if ok {
					assert.Equal(t, schema1, s)
				}
			}
		}()

		wg.Wait()
		stats := cache.Stats()
		assert.Equal(t, 1, stats.Size)
		assert.True(t, stats.TotalHits > 0)
	})

	t.Run("Clear", func(t *testing.T) {
		cache.Clear()
		assert.Equal(t, 0, cache.Size())
		stats := cache.Stats()
		assert.Equal(t, 0, stats.Size)
		assert.Equal(t, int64(0), stats.TotalHits)
	})
}

func BenchmarkSchemaCache(b *testing.B) {
	cache := NewSchemaCache(100)

	// Create test schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col1", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	b.Run("Put", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Put(schema)
		}
	})

	b.Run("Get", func(b *testing.B) {
		cache.Put(schema)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s, ok := cache.Get(schema)
			if !ok {
				b.Fatal("schema not found")
			}
			if s != schema {
				b.Fatal("wrong schema")
			}
		}
	})

	b.Run("Concurrent", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cache.Put(schema)
				s, ok := cache.Get(schema)
				if !ok {
					b.Fatal("schema not found")
				}
				if s != schema {
					b.Fatal("wrong schema")
				}
			}
		})
	})
}

func BenchmarkSchemaCacheConcurrent(b *testing.B) {
	cache := NewSchemaCache(1000)
	schemas := make([]*arrow.Schema, 100)

	// Create test schemas
	for i := 0; i < 100; i++ {
		schemas[i] = createTestSchema(fmt.Sprintf("test%d", i))
	}

	b.Run("Concurrent Put", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cache.Put(schemas[b.N%100])
			}
		})
	})

	b.Run("Concurrent Get", func(b *testing.B) {
		// Pre-populate cache
		for _, schema := range schemas {
			cache.Put(schema)
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cache.Get(schemas[b.N%100])
			}
		})
	})

	b.Run("Concurrent Mixed", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			localSchemas := make([]*arrow.Schema, 10)
			for i := range localSchemas {
				localSchemas[i] = createTestSchema(fmt.Sprintf("local%d", i))
			}

			for pb.Next() {
				switch b.N % 3 {
				case 0:
					cache.Put(localSchemas[b.N%10])
				case 1:
					cache.Get(schemas[b.N%100])
				case 2:
					cache.Get(localSchemas[b.N%10])
				}
			}
		})
	})
}
