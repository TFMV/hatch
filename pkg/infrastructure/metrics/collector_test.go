package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNoOpCollector_IncrementCounter(t *testing.T) {
	collector := NewNoOpCollector()
	// Should not panic
	collector.IncrementCounter("test_counter", "label1", "value1")
}

func TestNoOpCollector_RecordHistogram(t *testing.T) {
	collector := NewNoOpCollector()
	// Should not panic
	collector.RecordHistogram("test_histogram", 42.0, "label1", "value1")
}

func TestNoOpCollector_RecordGauge(t *testing.T) {
	collector := NewNoOpCollector()
	// Should not panic
	collector.RecordGauge("test_gauge", 42.0, "label1", "value1")
}

func TestNoOpCollector_StartTimer(t *testing.T) {
	collector := NewNoOpCollector()
	timer := collector.StartTimer("test_timer")

	// Sleep a bit to ensure measurable duration
	time.Sleep(10 * time.Millisecond)

	duration := timer.Stop()
	assert.Greater(t, duration, 0.0, "Timer duration should be greater than 0")
	assert.Less(t, duration, 1.0, "Timer duration should be less than 1 second")
}

func TestNoOpTimer_Stop(t *testing.T) {
	timer := &noOpTimer{start: time.Now()}
	time.Sleep(10 * time.Millisecond)

	duration := timer.Stop()
	assert.Greater(t, duration, 0.0, "Timer duration should be greater than 0")
	assert.Less(t, duration, 1.0, "Timer duration should be less than 1 second")
}
