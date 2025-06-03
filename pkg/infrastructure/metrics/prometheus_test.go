package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestPrometheusCollector_IncrementCounter(t *testing.T) {
	// Reset the default registry to avoid conflicts
	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	collector := NewPrometheusCollector()
	collector.IncrementCounter("test_counter", "label1", "value1")
	collector.IncrementCounter("test_counter", "label1", "value1")

	counter := collector.(*PrometheusCollector).counters["test_counter"]
	assert.NotNil(t, counter, "Counter should be created")

	// Verify the counter value
	value := testutil.ToFloat64(counter.WithLabelValues("value1"))
	assert.Equal(t, float64(2), value, "Counter should be incremented twice")
}

func TestPrometheusCollector_RecordHistogram(t *testing.T) {
	// Reset the default registry to avoid conflicts
	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	collector := NewPrometheusCollector()
	collector.RecordHistogram("test_histogram", 42.0, "label1", "value1")

	histogram := collector.(*PrometheusCollector).histograms["test_histogram"]
	assert.NotNil(t, histogram, "Histogram should be created")

	// Verify the histogram has observations
	count := testutil.CollectAndCount(histogram)
	assert.Equal(t, 1, count, "Histogram should have one observation")
}

func TestPrometheusCollector_RecordGauge(t *testing.T) {
	// Reset the default registry to avoid conflicts
	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	collector := NewPrometheusCollector()
	collector.RecordGauge("test_gauge", 42.0, "label1", "value1")

	gauge := collector.(*PrometheusCollector).gauges["test_gauge"]
	assert.NotNil(t, gauge, "Gauge should be created")

	// Verify the gauge value
	value := testutil.ToFloat64(gauge.WithLabelValues("value1"))
	assert.Equal(t, 42.0, value, "Gauge should be set to 42.0")
}

func TestPrometheusCollector_StartTimer(t *testing.T) {
	collector := NewPrometheusCollector()
	timer := collector.StartTimer("test_timer")

	time.Sleep(10 * time.Millisecond)

	duration := timer.Stop()
	assert.Greater(t, duration, 0.0, "Timer duration should be greater than 0")
	assert.Less(t, duration, 1.0, "Timer duration should be less than 1 second")
}

func TestParseLabelPairs(t *testing.T) {
	tests := []struct {
		name        string
		labels      []string
		wantNames   []string
		wantValues  []string
		description string
	}{
		{
			name:        "empty labels",
			labels:      []string{},
			wantNames:   []string{},
			wantValues:  []string{},
			description: "Empty labels should return empty slices",
		},
		{
			name:        "single pair",
			labels:      []string{"key1", "value1"},
			wantNames:   []string{"key1"},
			wantValues:  []string{"value1"},
			description: "Single pair should be parsed correctly",
		},
		{
			name:        "multiple pairs",
			labels:      []string{"key1", "value1", "key2", "value2"},
			wantNames:   []string{"key1", "key2"},
			wantValues:  []string{"value1", "value2"},
			description: "Multiple pairs should be parsed correctly",
		},
		{
			name:        "odd number of labels",
			labels:      []string{"key1", "value1", "key2"},
			wantNames:   []string{"key1"},
			wantValues:  []string{"value1"},
			description: "Odd number of labels should ignore the last one",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			names, values := parseLabelPairs(tt.labels)
			assert.Equal(t, tt.wantNames, names, tt.description)
			assert.Equal(t, tt.wantValues, values, tt.description)
		})
	}
}

func TestMetricsServer(t *testing.T) {
	server := NewMetricsServer(":0") // Use port 0 to let the OS assign a port

	// Start server in a goroutine since it blocks
	go func() {
		err := server.Start()
		if err != nil {
			// Ignore error from server.Close()
			if err.Error() != "http: Server closed" {
				t.Errorf("unexpected error: %v", err)
			}
		}
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Stop the server
	err := server.Stop()
	assert.NoError(t, err, "Server should stop without error")
}

func TestMetricsServer_StopWithoutStart(t *testing.T) {
	server := NewMetricsServer(":0")
	err := server.Stop()
	assert.NoError(t, err, "Stopping an unstarted server should not error")
}
