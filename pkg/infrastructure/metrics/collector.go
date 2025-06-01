// Package metrics provides metrics collection for the Flight SQL server.
package metrics

import (
	"time"
)

// Collector defines the interface for collecting metrics.
type Collector interface {
	// IncrementCounter increments a counter metric.
	IncrementCounter(name string, labels ...string)

	// RecordHistogram records a value in a histogram metric.
	RecordHistogram(name string, value float64, labels ...string)

	// RecordGauge records a gauge metric value.
	RecordGauge(name string, value float64, labels ...string)

	// StartTimer starts a timer for measuring duration.
	StartTimer(name string) Timer
}

// Timer represents a timing measurement.
type Timer interface {
	// Stop stops the timer and returns the duration in seconds.
	Stop() float64
}

// NoOpCollector is a no-op implementation of Collector.
type NoOpCollector struct{}

// NewNoOpCollector creates a new no-op collector.
func NewNoOpCollector() Collector {
	return &NoOpCollector{}
}

// IncrementCounter does nothing.
func (n *NoOpCollector) IncrementCounter(name string, labels ...string) {}

// RecordHistogram does nothing.
func (n *NoOpCollector) RecordHistogram(name string, value float64, labels ...string) {}

// RecordGauge does nothing.
func (n *NoOpCollector) RecordGauge(name string, value float64, labels ...string) {}

// StartTimer returns a no-op timer.
func (n *NoOpCollector) StartTimer(name string) Timer {
	return &noOpTimer{start: time.Now()}
}

// noOpTimer is a no-op implementation of Timer.
type noOpTimer struct {
	start time.Time
}

// Stop returns the elapsed time in seconds.
func (t *noOpTimer) Stop() float64 {
	return time.Since(t.start).Seconds()
}
