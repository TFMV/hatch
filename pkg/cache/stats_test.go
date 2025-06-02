package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStatsCollector_RecordHit(t *testing.T) {
	collector := NewStatsCollector()

	// Record multiple hits
	for i := 0; i < 5; i++ {
		collector.RecordHit()
	}

	stats := collector.GetStats()
	assert.Equal(t, uint64(5), stats.Hits)
	assert.Equal(t, uint64(0), stats.Misses)
	assert.Equal(t, uint64(0), stats.Evictions)
}

func TestStatsCollector_RecordMiss(t *testing.T) {
	collector := NewStatsCollector()

	// Record multiple misses
	for i := 0; i < 3; i++ {
		collector.RecordMiss()
	}

	stats := collector.GetStats()
	assert.Equal(t, uint64(0), stats.Hits)
	assert.Equal(t, uint64(3), stats.Misses)
	assert.Equal(t, uint64(0), stats.Evictions)
}

func TestStatsCollector_RecordEviction(t *testing.T) {
	collector := NewStatsCollector()

	// Record multiple evictions
	for i := 0; i < 2; i++ {
		collector.RecordEviction()
	}

	stats := collector.GetStats()
	assert.Equal(t, uint64(0), stats.Hits)
	assert.Equal(t, uint64(0), stats.Misses)
	assert.Equal(t, uint64(2), stats.Evictions)
}

func TestStatsCollector_UpdateSize(t *testing.T) {
	collector := NewStatsCollector()

	// Update size multiple times
	sizes := []int64{100, 200, 300}
	for _, size := range sizes {
		collector.UpdateSize(size)
	}

	stats := collector.GetStats()
	assert.Equal(t, sizes[len(sizes)-1], stats.Size)
}

func TestStatsCollector_HitRate(t *testing.T) {
	collector := NewStatsCollector()

	// Test with no hits or misses
	assert.Equal(t, 0.0, collector.HitRate())

	// Test with only hits
	collector.RecordHit()
	collector.RecordHit()
	assert.Equal(t, 1.0, collector.HitRate())

	// Test with hits and misses
	collector.RecordMiss()
	collector.RecordMiss()
	assert.Equal(t, 0.5, collector.HitRate())

	// Test with only misses
	collector = NewStatsCollector()
	collector.RecordMiss()
	collector.RecordMiss()
	assert.Equal(t, 0.0, collector.HitRate())
}

func TestStatsCollector_Concurrent(t *testing.T) {
	collector := NewStatsCollector()
	var wg sync.WaitGroup

	// Spawn multiple goroutines to update stats concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.RecordHit()
			collector.RecordMiss()
			collector.RecordEviction()
			collector.UpdateSize(100)
		}()
	}

	wg.Wait()

	stats := collector.GetStats()
	assert.Equal(t, uint64(10), stats.Hits)
	assert.Equal(t, uint64(10), stats.Misses)
	assert.Equal(t, uint64(10), stats.Evictions)
	assert.Equal(t, int64(100), stats.Size)
}

func TestStatsCollector_LastUpdated(t *testing.T) {
	collector := NewStatsCollector()
	initialStats := collector.GetStats()

	// Wait a bit to ensure time difference
	time.Sleep(10 * time.Millisecond)

	// Record a hit and check LastUpdated
	collector.RecordHit()
	updatedStats := collector.GetStats()

	assert.True(t, updatedStats.LastUpdated.After(initialStats.LastUpdated))
}
