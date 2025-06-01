package cache

import (
	"context"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Cache defines the interface for caching Arrow record batches
type Cache interface {
	// Get retrieves a record batch from the cache
	Get(ctx context.Context, key string) (arrow.Record, error)
	// Put stores a record batch in the cache
	Put(ctx context.Context, key string, record arrow.Record) error
	// Delete removes a record batch from the cache
	Delete(ctx context.Context, key string) error
	// Clear removes all entries from the cache
	Clear(ctx context.Context) error
	// Close releases any resources held by the cache
	Close() error
}

// CacheEntry represents a single cache entry with metadata
type CacheEntry struct {
	Record    arrow.Record
	CreatedAt time.Time
	LastUsed  time.Time
	Size      int64
}

// MemoryCache implements Cache interface using in-memory storage
type MemoryCache struct {
	mu       sync.RWMutex
	entries  map[string]*CacheEntry
	maxSize  int64
	currSize int64
	alloc    memory.Allocator
}

// NewMemoryCache creates a new memory cache with the specified maximum size
func NewMemoryCache(maxSize int64, alloc memory.Allocator) *MemoryCache {
	return &MemoryCache{
		entries: make(map[string]*CacheEntry),
		maxSize: maxSize,
		alloc:   alloc,
	}
}

// Get retrieves a record batch from the cache
func (c *MemoryCache) Get(ctx context.Context, key string) (arrow.Record, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if entry, ok := c.entries[key]; ok {
		entry.LastUsed = time.Now()
		return entry.Record, nil
	}
	return nil, nil
}

// Put stores a record batch in the cache
func (c *MemoryCache) Put(ctx context.Context, key string, record arrow.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Calculate size of the record
	size := int64(0)
	for i := int64(0); i < int64(record.NumCols()); i++ {
		col := record.Column(int(i))
		size += int64(col.Data().Buffers()[0].Len())
	}

	// Check if we need to evict entries to make space
	if c.currSize+size > c.maxSize {
		c.evictOldest()
	}

	// Create new entry
	entry := &CacheEntry{
		Record:    record,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
		Size:      size,
	}

	// Remove old entry if it exists
	if oldEntry, ok := c.entries[key]; ok {
		c.currSize -= oldEntry.Size
	}

	c.entries[key] = entry
	c.currSize += size
	return nil
}

// Delete removes a record batch from the cache
func (c *MemoryCache) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[key]; ok {
		c.currSize -= entry.Size
		delete(c.entries, key)
	}
	return nil
}

// Clear removes all entries from the cache
func (c *MemoryCache) Clear(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*CacheEntry)
	c.currSize = 0
	return nil
}

// Close releases any resources held by the cache
func (c *MemoryCache) Close() error {
	return c.Clear(context.Background())
}

// evictOldest removes the least recently used entry from the cache
func (c *MemoryCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for key, entry := range c.entries {
		if oldestKey == "" || entry.LastUsed.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.LastUsed
		}
	}

	if oldestKey != "" {
		c.currSize -= c.entries[oldestKey].Size
		delete(c.entries, oldestKey)
	}
}

// CacheKeyGenerator defines the interface for generating cache keys
type CacheKeyGenerator interface {
	GenerateKey(query string, params map[string]interface{}) string
}

// DefaultCacheKeyGenerator implements CacheKeyGenerator
type DefaultCacheKeyGenerator struct{}

// GenerateKey creates a cache key from a query and its parameters
func (g *DefaultCacheKeyGenerator) GenerateKey(query string, params map[string]interface{}) string {
	// TODO: Implement a more sophisticated key generation strategy
	// For now, just use the query as the key
	return query
}
