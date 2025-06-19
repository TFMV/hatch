package utils

import (
	server "github.com/TFMV/porter/pkg/server"
)

type Timer interface {
	Stop() float64
}

// MetricsCollector defines the interface for collecting metrics.
// This should match the server.MetricsCollector interface.
type MetricsCollector interface {
	IncrementCounter(name string, labels ...string)
	RecordHistogram(name string, value float64, labels ...string)
	RecordGauge(name string, value float64, labels ...string)
	StartTimer(name string) Timer
}

// noOpTimer implements the server.Timer interface with no operations.
type noOpTimer struct{}

// Stop does nothing and returns 0.
func (t *noOpTimer) Stop() float64 {
	return 0.0
}

// NoOpMetricsCollector implements the server.MetricsCollector interface with no operations.
// It will be used in tests where metrics collection is not the focus.
// server.MetricsCollector expects:
//
//	IncrementCounter(name string, labels ...string)
//	RecordHistogram(name string, value float64, labels ...string)
//	RecordGauge(name string, value float64, labels ...string)
//	StartTimer(name string) server.Timer
type NoOpMetricsCollector struct{}

// NewNoOpMetricsCollector creates a new NoOpMetricsCollector.
func NewNoOpMetricsCollector() *NoOpMetricsCollector {
	return &NoOpMetricsCollector{}
}

// IncrementCounter does nothing.
func (c *NoOpMetricsCollector) IncrementCounter(name string, labels ...string) {}

// RecordHistogram does nothing.
func (c *NoOpMetricsCollector) RecordHistogram(name string, value float64, labels ...string) {}

// RecordGauge does nothing.
func (c *NoOpMetricsCollector) RecordGauge(name string, value float64, labels ...string) {}

// StartTimer returns a no-op timer that satisfies server.Timer.
func (c *NoOpMetricsCollector) StartTimer(name string) server.Timer { // Return type is now server.Timer
	return &noOpTimer{}
}
