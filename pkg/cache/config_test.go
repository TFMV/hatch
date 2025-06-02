package cache

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.Equal(t, int64(100*1024*1024), config.MaxSize) // 100MB
	assert.Equal(t, 5*time.Minute, config.TTL)
	assert.Equal(t, memory.DefaultAllocator, config.Allocator)
	assert.True(t, config.EnableStats)
}

func TestConfig_WithMaxSize(t *testing.T) {
	config := DefaultConfig()
	newSize := int64(200 * 1024 * 1024) // 200MB

	updated := config.WithMaxSize(newSize)
	assert.Equal(t, newSize, updated.MaxSize)
	assert.Equal(t, config.TTL, updated.TTL)
	assert.Equal(t, config.Allocator, updated.Allocator)
	assert.Equal(t, config.EnableStats, updated.EnableStats)
}

func TestConfig_WithTTL(t *testing.T) {
	config := DefaultConfig()
	newTTL := 10 * time.Minute

	updated := config.WithTTL(newTTL)
	assert.Equal(t, config.MaxSize, updated.MaxSize)
	assert.Equal(t, newTTL, updated.TTL)
	assert.Equal(t, config.Allocator, updated.Allocator)
	assert.Equal(t, config.EnableStats, updated.EnableStats)
}

func TestConfig_WithAllocator(t *testing.T) {
	config := DefaultConfig()
	newAllocator := memory.NewGoAllocator()

	updated := config.WithAllocator(newAllocator)
	assert.Equal(t, config.MaxSize, updated.MaxSize)
	assert.Equal(t, config.TTL, updated.TTL)
	assert.Equal(t, newAllocator, updated.Allocator)
	assert.Equal(t, config.EnableStats, updated.EnableStats)
}

func TestConfig_WithStats(t *testing.T) {
	config := DefaultConfig()

	// Test disabling stats
	updated := config.WithStats(false)
	assert.Equal(t, config.MaxSize, updated.MaxSize)
	assert.Equal(t, config.TTL, updated.TTL)
	assert.Equal(t, config.Allocator, updated.Allocator)
	assert.False(t, updated.EnableStats)

	// Test enabling stats
	updated = config.WithStats(true)
	assert.Equal(t, config.MaxSize, updated.MaxSize)
	assert.Equal(t, config.TTL, updated.TTL)
	assert.Equal(t, config.Allocator, updated.Allocator)
	assert.True(t, updated.EnableStats)
}

func TestConfig_Chaining(t *testing.T) {
	config := DefaultConfig()
	newSize := int64(200 * 1024 * 1024) // 200MB
	newTTL := 10 * time.Minute
	newAllocator := memory.NewGoAllocator()

	updated := config.
		WithMaxSize(newSize).
		WithTTL(newTTL).
		WithAllocator(newAllocator).
		WithStats(false)

	assert.Equal(t, newSize, updated.MaxSize)
	assert.Equal(t, newTTL, updated.TTL)
	assert.Equal(t, newAllocator, updated.Allocator)
	assert.False(t, updated.EnableStats)
}
