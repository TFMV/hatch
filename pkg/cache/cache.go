package cache

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
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
		entry.Record.Retain()
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
		data := col.Data()

		// Add size of each buffer
		for _, buf := range data.Buffers() {
			if buf != nil {
				size += int64(buf.Len())
			}
		}

		// Add size of any child data
		for _, child := range data.Children() {
			for _, buf := range child.Buffers() {
				if buf != nil {
					size += int64(buf.Len())
				}
			}
		}
	}

	// Add size of schema
	size += int64(record.NumCols() * 32) // Rough estimate for schema overhead

	// If the record is too large for the cache, don't store it
	if size > c.maxSize {
		return nil
	}

	// Check if we need to evict entries to make space
	for c.currSize+size > c.maxSize {
		if len(c.entries) == 0 {
			// If we can't make space, don't store the record
			return nil
		}
		c.evictOldest()
	}

	// Create new entry
	record.Retain()
	entry := &CacheEntry{
		Record:    record,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
		Size:      size,
	}

	// Remove old entry if it exists
	if oldEntry, ok := c.entries[key]; ok {
		oldEntry.Record.Release()
		c.currSize -= oldEntry.Size
		delete(c.entries, key)
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
		entry.Record.Release()
		c.currSize -= entry.Size
		delete(c.entries, key)
	}
	return nil
}

// Clear removes all entries from the cache
func (c *MemoryCache) Clear(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, entry := range c.entries {
		entry.Record.Release()
	}
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
	for c.currSize > 0 {
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
			c.entries[oldestKey].Record.Release()
			delete(c.entries, oldestKey)
		} else {
			break
		}
	}
}

// renderValue converts an arbitrary parameter value into a canonical string.
func RenderValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"

	// ── primitives ────────────────────────────────────────────────
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int, int8, int16, int32, int64:
		return strconv.FormatInt(reflect.ValueOf(t).Int(), 10)
	case uint, uint8, uint16, uint32, uint64:
		return strconv.FormatUint(reflect.ValueOf(t).Uint(), 10)
	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case string:
		return Escape(t)

	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)

	case []byte:
		return "0x" + hex.EncodeToString(t)

	// ── Arrow scalar values ───────────────────────────────────────
	case scalar.Scalar:
		return Escape(t.String())

	// ── slices / arrays (deterministic order) ────────────────────
	case []any:
		var parts []string
		for _, elem := range t {
			parts = append(parts, RenderValue(elem))
		}
		return "[" + strings.Join(parts, ",") + "]"
	default:
		rv := reflect.ValueOf(v)

		// Handle slices & arrays of specific element types
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			var parts []string
			for i := 0; i < rv.Len(); i++ {
				parts = append(parts, RenderValue(rv.Index(i).Interface()))
			}
			return "[" + strings.Join(parts, ",") + "]"
		}

		// Handle maps with sorted keys
		if rv.Kind() == reflect.Map {
			keys := rv.MapKeys()
			sort.Slice(keys, func(i, j int) bool {
				return keys[i].String() < keys[j].String()
			})
			var buf bytes.Buffer
			buf.WriteByte('{')
			for i, k := range keys {
				if i > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(RenderValue(k.Interface()))
				buf.WriteByte(':')
				buf.WriteString(RenderValue(rv.MapIndex(k).Interface()))
			}
			buf.WriteByte('}')
			return buf.String()
		}

		// Handle structs via JSON to get deterministic field ordering
		if rv.Kind() == reflect.Struct {
			// `json.Marshal` orders struct fields alphabetically by tag/name.
			b, _ := json.Marshal(v)
			return Escape(string(b))
		}

		// Fallback: Go‑syntax representation (deterministic for scalars)
		return Escape(fmt.Sprintf("%#v", v))
	}
}

// escape ensures separator characters don't leak into the output.
func Escape(s string) string {
	replacer := strings.NewReplacer(
		"\\", `\\`,
		"|", `\|`,
		"=", `\=`,
		",", `\,`,
		"{", `\{`,
		"}", `\}`,
		"[", `\[`,
		"]", `\]`,
	)
	return replacer.Replace(s)
}

// CacheKeyGenerator defines the interface for generating cache keys
type CacheKeyGenerator interface {
	GenerateKey(query string, params map[string]interface{}) string
}

// DefaultCacheKeyGenerator implements CacheKeyGenerator
type DefaultCacheKeyGenerator struct{}

// TODO:GenerateKey creates a deterministic cache‑key based on the SQL text
// (with whitespace normalised) and a stable, ordered serialisation of
// the parameter map.
//
// The resulting key is a hex‑encoded SHA‑256 digest, keeping it short
// and collision‑resistant while hiding potentially sensitive values.
func (g *DefaultCacheKeyGenerator) GenerateKey(query string, params map[string]interface{}) string {
	return query
}
