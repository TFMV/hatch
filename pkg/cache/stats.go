package cache

import (
	"sync/atomic"
	"time"
)

// Stats holds cache statistics
type Stats struct {
	Hits        uint64
	Misses      uint64
	Evictions   uint64
	Size        int64
	LastUpdated time.Time
}

// StatsCollector collects and reports cache statistics
type StatsCollector struct {
	stats Stats
}

// NewStatsCollector creates a new statistics collector
func NewStatsCollector() *StatsCollector {
	return &StatsCollector{
		stats: Stats{
			LastUpdated: time.Now(),
		},
	}
}

// RecordHit records a cache hit
func (c *StatsCollector) RecordHit() {
	atomic.AddUint64(&c.stats.Hits, 1)
	c.stats.LastUpdated = time.Now()
}

// RecordMiss records a cache miss
func (c *StatsCollector) RecordMiss() {
	atomic.AddUint64(&c.stats.Misses, 1)
	c.stats.LastUpdated = time.Now()
}

// RecordEviction records a cache eviction
func (c *StatsCollector) RecordEviction() {
	atomic.AddUint64(&c.stats.Evictions, 1)
	c.stats.LastUpdated = time.Now()
}

// UpdateSize updates the current cache size
func (c *StatsCollector) UpdateSize(size int64) {
	atomic.StoreInt64(&c.stats.Size, size)
	c.stats.LastUpdated = time.Now()
}

// GetStats returns the current cache statistics
func (c *StatsCollector) GetStats() Stats {
	return Stats{
		Hits:        atomic.LoadUint64(&c.stats.Hits),
		Misses:      atomic.LoadUint64(&c.stats.Misses),
		Evictions:   atomic.LoadUint64(&c.stats.Evictions),
		Size:        atomic.LoadInt64(&c.stats.Size),
		LastUpdated: c.stats.LastUpdated,
	}
}

// HitRate returns the cache hit rate
func (c *StatsCollector) HitRate() float64 {
	hits := atomic.LoadUint64(&c.stats.Hits)
	misses := atomic.LoadUint64(&c.stats.Misses)
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}
