package handlers

import "github.com/TFMV/porter/pkg/infrastructure/metrics"

// metricsAdapter adapts metrics.Collector to the MetricsCollector interface used in handlers.
type metricsAdapter struct {
	collector metrics.Collector
}

func newMetricsAdapter(c metrics.Collector) MetricsCollector {
	return &metricsAdapter{collector: c}
}

func (m *metricsAdapter) IncrementCounter(name string, labels ...string) {
	m.collector.IncrementCounter(name, labels...)
}

func (m *metricsAdapter) RecordHistogram(name string, value float64, labels ...string) {
	m.collector.RecordHistogram(name, value, labels...)
}

func (m *metricsAdapter) RecordGauge(name string, value float64, labels ...string) {
	m.collector.RecordGauge(name, value, labels...)
}

func (m *metricsAdapter) StartTimer(name string) Timer {
	return &metricsTimerAdapter{timer: m.collector.StartTimer(name)}
}

type metricsTimerAdapter struct {
	timer metrics.Timer
}

func (t *metricsTimerAdapter) Stop() {
	t.timer.Stop()
}
