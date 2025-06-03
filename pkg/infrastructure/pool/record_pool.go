package pool

import (
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// poolConfig holds configuration for the record pool
type poolConfig struct {
	minSize     int     // minimum batch size
	maxSize     int     // maximum batch size
	targetAlloc int64   // target allocation per batch
	growthRate  float64 // how fast to grow batch size
}

// defaultConfig returns sensible defaults
func defaultConfig() poolConfig {
	return poolConfig{
		minSize:     1024,
		maxSize:     32768,
		targetAlloc: 4 * 1024 * 1024, // 4MB target
		growthRate:  1.5,
	}
}

// FastRecordPool is a concurrent, multi‑schema pool optimised for throughput.
type FastRecordPool struct {
	alloc  memory.Allocator // usually the Arrow global allocator
	pools  sync.Map         // *arrow.Schema → *sync.Pool
	config poolConfig
	stats  struct {
		hits   atomic.Int64
		misses atomic.Int64
		allocs atomic.Int64
	}
}

// NewFastRecordPool constructs a pool; pass nil for the default Go allocator.
func NewFastRecordPool(alloc memory.Allocator) *FastRecordPool {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	return &FastRecordPool{
		alloc:  alloc,
		config: defaultConfig(),
	}
}

// getPool returns the per‑schema sync.Pool, creating it if necessary.
func (p *FastRecordPool) getPool(schema *arrow.Schema) *sync.Pool {
	if v, ok := p.pools.Load(schema); ok {
		return v.(*sync.Pool)
	}

	// First writer wins; others reuse the stored pool.
	newPool := &sync.Pool{
		New: func() interface{} {
			p.stats.allocs.Add(1)
			cols := make([]arrow.Array, len(schema.Fields()))
			for i, f := range schema.Fields() {
				cols[i] = array.MakeArrayOfNull(p.alloc, f.Type, 0)
			}
			return array.NewRecord(schema, cols, 0)
		},
	}
	actual, _ := p.pools.LoadOrStore(schema, newPool)
	return actual.(*sync.Pool)
}

// Get returns an empty record for the given schema.
func (p *FastRecordPool) Get(schema *arrow.Schema) arrow.Record {
	rec := p.getPool(schema).Get().(arrow.Record)
	if rec != nil {
		p.stats.hits.Add(1)
		return rec
	}
	p.stats.misses.Add(1)
	return nil
}

// Put resets the record to zero‑length and puts it back.
func (p *FastRecordPool) Put(rec arrow.Record) {
	if rec == nil {
		return
	}
	v, ok := p.pools.Load(rec.Schema())
	if !ok {
		rec.Release()
		return
	}
	pool := v.(*sync.Pool)

	// Re‑slice columns to length 0 – O(1), no new buffers.
	cols := make([]arrow.Array, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		col := rec.Column(i)
		cols[i] = array.NewSlice(col, 0, 0) // shares buffers, cheap
		col.Release()                       // drop the old reference
	}

	// Replace the record with the zero‑length version.
	empty := array.NewRecord(rec.Schema(), cols, 0)
	pool.Put(empty)
}

// Stats returns current pool statistics
type Stats struct {
	Hits   int64
	Misses int64
	Allocs int64
}

// Stats returns current statistics for the pool
func (p *FastRecordPool) Stats() Stats {
	return Stats{
		Hits:   p.stats.hits.Load(),
		Misses: p.stats.misses.Load(),
		Allocs: p.stats.allocs.Load(),
	}
}
