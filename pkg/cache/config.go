package cache

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Config holds the configuration for the cache
type Config struct {
	// MaxSize is the maximum size of the cache in bytes
	MaxSize int64
	// TTL is the time-to-live for cache entries
	TTL time.Duration
	// Allocator is the memory allocator to use
	Allocator memory.Allocator
	// EnableStats enables cache statistics collection
	EnableStats bool
}

// DefaultConfig returns a default cache configuration
func DefaultConfig() *Config {
	return &Config{
		MaxSize:     100 * 1024 * 1024, // 100MB
		TTL:         5 * time.Minute,
		Allocator:   memory.DefaultAllocator,
		EnableStats: true,
	}
}

// WithMaxSize sets the maximum size of the cache
func (c *Config) WithMaxSize(size int64) *Config {
	c.MaxSize = size
	return c
}

// WithTTL sets the time-to-live for cache entries
func (c *Config) WithTTL(ttl time.Duration) *Config {
	c.TTL = ttl
	return c
}

// WithAllocator sets the memory allocator
func (c *Config) WithAllocator(alloc memory.Allocator) *Config {
	c.Allocator = alloc
	return c
}

// WithStats enables or disables cache statistics
func (c *Config) WithStats(enable bool) *Config {
	c.EnableStats = enable
	return c
}
