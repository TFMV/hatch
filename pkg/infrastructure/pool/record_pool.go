package pool

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FastRecordPool is a concurrent, multi‑schema pool optimised for throughput.
type FastRecordPool struct {
	alloc memory.Allocator // usually the Arrow global allocator
	pools sync.Map         // *arrow.Schema → *sync.Pool
}

// New constructs a pool; pass nil for the default Go allocator.
func NewFastRecordPool(alloc memory.Allocator) *FastRecordPool {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	return &FastRecordPool{alloc: alloc}
}

// getPool returns the per‑schema sync.Pool, creating it if necessary.
func (p *FastRecordPool) getPool(schema *arrow.Schema) *sync.Pool {
	if v, ok := p.pools.Load(schema); ok {
		return v.(*sync.Pool)
	}

	// First writer wins; others reuse the stored pool.
	newPool := &sync.Pool{
		New: func() interface{} {
			cols := make([]arrow.Array, len(schema.Fields()))
			for i, f := range schema.Fields() {
				cols[i] = array.MakeArrayOfNull(p.alloc, f.Type, 0) // first time only
			}
			return array.NewRecord(schema, cols, 0)
		},
	}
	actual, _ := p.pools.LoadOrStore(schema, newPool)
	return actual.(*sync.Pool)
}

// Get returns an empty record for the given schema.
func (p *FastRecordPool) Get(schema *arrow.Schema) arrow.Record {
	return p.getPool(schema).Get().(arrow.Record)
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
