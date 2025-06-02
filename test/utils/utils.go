package utils

import (
	// "time" // time import is not strictly needed for noOpTimer anymore
	server "github.com/TFMV/flight/cmd/server/server" // Import the server package
)

// Assuming server.Timer and server.MetricsCollector are accessible via this path
// If not, the path to the server package might need adjustment based on its actual location
// For example, it might be "github.com/TFMV/flight/cmd/server/server"
// Based on previous context, server.MetricsCollector is in "github.com/TFMV/flight/cmd/server/server"
// However, to avoid circular dependencies if this utils is used by server tests,
// we should define the interface here or use a more common metrics interface.
// For now, let's assume the interface is defined in a way that's accessible.
// We'll define a compatible interface here for NoOpMetricsCollector to implement.

// Forward declaration of the interface parts needed from the server package
// to avoid direct import if it causes issues, or if a more generic approach is taken later.
// Ideally, these would come from the actual server package.

// Timer defines the interface for a timing measurement.
// This should match the server.Timer interface.
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
