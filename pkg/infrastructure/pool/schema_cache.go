package pool

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// entry holds bookkeeping for one schema.
type entry struct {
	schema    *arrow.Schema
	createdAt time.Time
	lastUsed  atomic.Int64 // unix‑nanos
	hits      atomic.Int64
}

// SchemaCache is an O(1) LRU cache (pointer‑keyed, thread‑safe).
type SchemaCache struct {
	cap   int
	mu    sync.Mutex
	lru   *list.List                      // front = most‑recent
	items map[*arrow.Schema]*list.Element // schema → *list.Element
}

// NewSchemaCache returns a cache with a given maximum size (>0).
func NewSchemaCache(max int) *SchemaCache {
	if max <= 0 {
		max = 100
	}
	return &SchemaCache{
		cap:   max,
		lru:   list.New(),
		items: make(map[*arrow.Schema]*list.Element, max),
	}
}

// Get returns the cached entry (updates LRU & stats) or nil/false.
func (c *SchemaCache) Get(s *arrow.Schema) (*arrow.Schema, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, ok := c.items[s]; ok {
		c.lru.MoveToFront(ele)
		e := ele.Value.(*entry)
		e.hits.Add(1)
		e.lastUsed.Store(time.Now().UnixNano())
		return e.schema, true
	}
	return nil, false
}

// Put inserts the schema or refreshes its position if already cached.
func (c *SchemaCache) Put(s *arrow.Schema) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, ok := c.items[s]; ok {
		c.lru.MoveToFront(ele)
		e := ele.Value.(*entry)
		e.hits.Add(1)
		e.lastUsed.Store(time.Now().UnixNano())
		return
	}

	e := &entry{
		schema:    s,
		createdAt: time.Now(),
	}
	e.lastUsed.Store(e.createdAt.UnixNano())
	e.hits.Store(1)

	ele := c.lru.PushFront(e)
	c.items[s] = ele

	if len(c.items) > c.cap {
		c.evictOldest()
	}
}

// evictOldest removes the LRU element (caller holds the lock).
func (c *SchemaCache) evictOldest() {
	ele := c.lru.Back()
	if ele == nil {
		return
	}
	c.lru.Remove(ele)
	e := ele.Value.(*entry)
	delete(c.items, e.schema)
}

// Clear empties the cache.
func (c *SchemaCache) Clear() {
	c.mu.Lock()
	c.lru.Init()
	c.items = make(map[*arrow.Schema]*list.Element, c.cap)
	c.mu.Unlock()
}

// Size returns the current number of cached schemas.
func (c *SchemaCache) Size() int {
	c.mu.Lock()
	n := len(c.items)
	c.mu.Unlock()
	return n
}

// CacheStats contains live statistics.
type CacheStats struct {
	Size      int
	Cap       int
	TotalHits int64
	OldestAge time.Duration
}

// Stats gathers statistics (O(n), called rarely).
func (c *SchemaCache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	var (
		total int64
		old   time.Time
		now   = time.Now()
	)

	for e := c.lru.Back(); e != nil; e = e.Prev() {
		ent := e.Value.(*entry)
		total += ent.hits.Load()
		if old.IsZero() || ent.createdAt.Before(old) {
			old = ent.createdAt
		}
	}

	age := time.Duration(0)
	if !old.IsZero() {
		age = now.Sub(old)
	}
	return CacheStats{
		Size:      len(c.items),
		Cap:       c.cap,
		TotalHits: total,
		OldestAge: age,
	}
}
