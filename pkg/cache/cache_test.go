package cache

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestRecord creates a simple Arrow record for testing
func createTestRecord(t *testing.T, alloc memory.Allocator) arrow.Record {
	t.Helper()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)

	record := builder.NewRecord()
	return record
}

func TestMemoryCache_GetPut(t *testing.T) {
	ctx := context.Background()
	cache := NewMemoryCache(1024*1024, memory.DefaultAllocator)
	defer cache.Close()

	record := createTestRecord(t, memory.DefaultAllocator)
	defer record.Release()

	// Test Put
	err := cache.Put(ctx, "test", record)
	require.NoError(t, err)

	// Test Get
	retrieved, err := cache.Get(ctx, "test")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, record.NumRows(), retrieved.NumRows())
	assert.Equal(t, record.NumCols(), retrieved.NumCols())

	// Test Get non-existent
	notFound, err := cache.Get(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, notFound)
}

func TestMemoryCache_Delete(t *testing.T) {
	ctx := context.Background()
	cache := NewMemoryCache(1024*1024, memory.DefaultAllocator)
	defer cache.Close()

	record := createTestRecord(t, memory.DefaultAllocator)
	defer record.Release()

	// Put and verify
	err := cache.Put(ctx, "test", record)
	require.NoError(t, err)

	retrieved, err := cache.Get(ctx, "test")
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	// Delete and verify
	err = cache.Delete(ctx, "test")
	require.NoError(t, err)

	notFound, err := cache.Get(ctx, "test")
	require.NoError(t, err)
	assert.Nil(t, notFound)
}

func TestMemoryCache_Clear(t *testing.T) {
	ctx := context.Background()
	cache := NewMemoryCache(1024*1024, memory.DefaultAllocator)
	defer cache.Close()

	record := createTestRecord(t, memory.DefaultAllocator)
	defer record.Release()

	// Put multiple records
	err := cache.Put(ctx, "test1", record)
	require.NoError(t, err)
	err = cache.Put(ctx, "test2", record)
	require.NoError(t, err)

	// Clear and verify
	err = cache.Clear(ctx)
	require.NoError(t, err)

	notFound1, err := cache.Get(ctx, "test1")
	require.NoError(t, err)
	assert.Nil(t, notFound1)

	notFound2, err := cache.Get(ctx, "test2")
	require.NoError(t, err)
	assert.Nil(t, notFound2)
}

func TestMemoryCache_Eviction(t *testing.T) {
	ctx := context.Background()
	// Set a small max size to force eviction
	cache := NewMemoryCache(1, memory.DefaultAllocator) // Set to 1 byte to ensure any record is too large
	defer cache.Close()

	record := createTestRecord(t, memory.DefaultAllocator)
	defer record.Release()

	// Put a record that should trigger eviction
	err := cache.Put(ctx, "test", record)
	require.NoError(t, err)

	// Verify the record was evicted
	notFound, err := cache.Get(ctx, "test")
	require.NoError(t, err)
	assert.Nil(t, notFound, "Expected record to be nil since it should be too large for cache")
}

func TestDefaultCacheKeyGenerator(t *testing.T) {
	generator := &DefaultCacheKeyGenerator{}

	// Test basic key generation
	key := generator.GenerateKey("SELECT * FROM test", nil)
	assert.Equal(t, "a5ffdb84d5677a60c947770487b6e639043ce3b47a25a4dec9a90c53103eb7aa", key)

	// Test with parameters
	params := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	key = generator.GenerateKey("SELECT * FROM test WHERE id = ?", params)
	assert.Equal(t, "63a5b43249e3b72dcbdfcbde1fb886c7d1d8f1967f0c6551b7b03253ba2307d3", key)
}

func TestDefaultCacheKeyGeneratorStableParams(t *testing.T) {
	generator := &DefaultCacheKeyGenerator{}

	query := "SELECT * FROM test WHERE id = ?"
	params1 := map[string]interface{}{"id": 1, "name": "test"}
	params2 := map[string]interface{}{"name": "test", "id": 1}

	key1 := generator.GenerateKey(query, params1)
	key2 := generator.GenerateKey(query, params2)

	assert.Equal(t, key1, key2)
	assert.Equal(t, "63a5b43249e3b72dcbdfcbde1fb886c7d1d8f1967f0c6551b7b03253ba2307d3", key1)
}

func TestCacheEntry(t *testing.T) {
	record := createTestRecord(t, memory.DefaultAllocator)
	defer record.Release()

	now := time.Now()
	entry := &CacheEntry{
		Record:    record,
		CreatedAt: now,
		LastUsed:  now,
		Size:      100,
	}

	assert.Equal(t, record, entry.Record)
	assert.Equal(t, now, entry.CreatedAt)
	assert.Equal(t, now, entry.LastUsed)
	assert.Equal(t, int64(100), entry.Size)
}
