package metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PrometheusCollector implements Collector using Prometheus.
type PrometheusCollector struct {
	counters   map[string]*prometheus.CounterVec
	histograms map[string]*prometheus.HistogramVec
	gauges     map[string]*prometheus.GaugeVec
}

// NewPrometheusCollector creates a new Prometheus collector.
func NewPrometheusCollector() Collector {
	return &PrometheusCollector{
		counters:   make(map[string]*prometheus.CounterVec),
		histograms: make(map[string]*prometheus.HistogramVec),
		gauges:     make(map[string]*prometheus.GaugeVec),
	}
}

// IncrementCounter increments a counter metric.
func (p *PrometheusCollector) IncrementCounter(name string, labels ...string) {
	labelNames, labelValues := parseLabelPairs(labels)

	counter, exists := p.counters[name]
	if !exists {
		counter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: name,
				Help: fmt.Sprintf("Counter for %s", name),
			},
			labelNames,
		)
		prometheus.MustRegister(counter)
		p.counters[name] = counter
	}

	counter.WithLabelValues(labelValues...).Inc()
}

// RecordHistogram records a value in a histogram metric.
func (p *PrometheusCollector) RecordHistogram(name string, value float64, labels ...string) {
	labelNames, labelValues := parseLabelPairs(labels)

	histogram, exists := p.histograms[name]
	if !exists {
		histogram = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    name,
				Help:    fmt.Sprintf("Histogram for %s", name),
				Buckets: prometheus.DefBuckets,
			},
			labelNames,
		)
		prometheus.MustRegister(histogram)
		p.histograms[name] = histogram
	}

	histogram.WithLabelValues(labelValues...).Observe(value)
}

// RecordGauge records a gauge metric value.
func (p *PrometheusCollector) RecordGauge(name string, value float64, labels ...string) {
	labelNames, labelValues := parseLabelPairs(labels)

	gauge, exists := p.gauges[name]
	if !exists {
		gauge = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: name,
				Help: fmt.Sprintf("Gauge for %s", name),
			},
			labelNames,
		)
		prometheus.MustRegister(gauge)
		p.gauges[name] = gauge
	}

	gauge.WithLabelValues(labelValues...).Set(value)
}

// StartTimer starts a timer for measuring duration.
func (p *PrometheusCollector) StartTimer(name string) Timer {
	return &prometheusTimer{
		start: time.Now(),
		name:  name,
	}
}

// prometheusTimer implements Timer for Prometheus.
type prometheusTimer struct {
	start time.Time
	name  string
}

// Stop returns the elapsed time in seconds.
func (t *prometheusTimer) Stop() float64 {
	return time.Since(t.start).Seconds()
}

// parseLabelPairs parses label pairs from variadic string arguments.
// Expected format: "key1", "value1", "key2", "value2", ...
func parseLabelPairs(labels []string) ([]string, []string) {
	if len(labels)%2 != 0 {
		// If odd number of labels, ignore the last one
		labels = labels[:len(labels)-1]
	}

	labelNames := make([]string, 0, len(labels)/2)
	labelValues := make([]string, 0, len(labels)/2)

	for i := 0; i < len(labels); i += 2 {
		labelNames = append(labelNames, labels[i])
		labelValues = append(labelValues, labels[i+1])
	}

	return labelNames, labelValues
}

// MetricsServer provides an HTTP server for Prometheus metrics.
type MetricsServer struct {
	address string
	server  *http.Server
}

// NewMetricsServer creates a new metrics server.
func NewMetricsServer(address string) *MetricsServer {
	return &MetricsServer{
		address: address,
	}
}

// Start starts the metrics server.
func (s *MetricsServer) Start() error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	s.server = &http.Server{
		Addr:    s.address,
		Handler: mux,
	}

	return s.server.ListenAndServe()
}

// Stop stops the metrics server.
func (s *MetricsServer) Stop() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}
