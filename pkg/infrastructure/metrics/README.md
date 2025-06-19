# Porter Metrics Infrastructure

The metrics infrastructure package provides a flexible and extensible metrics collection system for the Flight SQL server. It supports multiple metric types, labels, and includes a Prometheus integration for monitoring and alerting.

## Design Overview

### Core Components

1. **Collector Interface**

   ```go
   type Collector interface {
       IncrementCounter(name string, labels ...string)
       RecordHistogram(name string, value float64, labels ...string)
       RecordGauge(name string, value float64, labels ...string)
       StartTimer(name string) Timer
   }
   ```

2. **Timer Interface**

   ```go
   type Timer interface {
       Stop() float64
   }
   ```

3. **PrometheusCollector**

   ```go
   type PrometheusCollector struct {
       counters   map[string]*prometheus.CounterVec
       histograms map[string]*prometheus.HistogramVec
       gauges     map[string]*prometheus.GaugeVec
   }
   ```

4. **MetricsServer**

   ```go
   type MetricsServer struct {
       address string
       server  *http.Server
       mu      sync.RWMutex
   }
   ```

## Implementation Details

### Metric Types

1. **Counters**
   - Monotonically increasing values
   - Perfect for request counts, errors, etc.
   - Thread-safe increment operations

2. **Histograms**
   - Distribution of values
   - Useful for latencies, sizes, etc.
   - Configurable buckets

3. **Gauges**
   - Point-in-time measurements
   - Good for resource usage, queue sizes, etc.
   - Can increase or decrease

4. **Timers**
   - Duration measurements
   - Automatically converts to seconds
   - Convenient for operation timing

### Label Support

The system supports dynamic label creation:

```go
// Label format: "key1", "value1", "key2", "value2", ...
collector.IncrementCounter("requests_total", 
    "method", "GET",
    "endpoint", "/api/v1/users",
    "status", "200")
```

## Usage

### Basic Usage

```go
// Create a Prometheus collector
collector := metrics.NewPrometheusCollector()

// Record metrics
collector.IncrementCounter("requests_total", "method", "GET")
collector.RecordGauge("queue_size", 42.0, "queue", "default")
collector.RecordHistogram("request_duration", 0.42, "endpoint", "/api/users")

// Time an operation
timer := collector.StartTimer("operation_duration")
// ... perform operation ...
duration := timer.Stop()
```

### Metrics Server

```go
// Create and start metrics server
server := metrics.NewMetricsServer(":9090")
go server.Start()

// Stop server when done
defer server.Stop()
```

## Prometheus Integration

### Metric Registration

Metrics are automatically registered with Prometheus:

1. **Counter Registration**

   ```go
   counter := prometheus.NewCounterVec(
       prometheus.CounterOpts{
           Name: name,
           Help: fmt.Sprintf("Counter for %s", name),
       },
       labelNames,
   )
   ```

2. **Histogram Registration**

   ```go
   histogram := prometheus.NewHistogramVec(
       prometheus.HistogramOpts{
           Name:    name,
           Help:    fmt.Sprintf("Histogram for %s", name),
           Buckets: prometheus.DefBuckets,
       },
       labelNames,
   )
   ```

3. **Gauge Registration**

   ```go
   gauge := prometheus.NewGaugeVec(
       prometheus.GaugeOpts{
           Name: name,
           Help: fmt.Sprintf("Gauge for %s", name),
       },
       labelNames,
   )
   ```

### HTTP Endpoint

The metrics server exposes a `/metrics` endpoint for Prometheus scraping:

```go
mux := http.NewServeMux()
mux.Handle("/metrics", promhttp.Handler())
```

## Best Practices

1. **Metric Naming**
   - Use consistent naming conventions
   - Include units in names
   - Use descriptive names

2. **Label Usage**
   - Keep cardinality in check
   - Use meaningful label names
   - Be consistent with label values

3. **Performance**
   - Minimize metric creation
   - Use appropriate metric types
   - Monitor metric cardinality

4. **Monitoring**
   - Set up alerts
   - Create dashboards
   - Monitor metric growth

## Testing

The package includes comprehensive tests:

1. **Collector Tests**
   - Basic metric operations
   - Label handling
   - Timer functionality

2. **Prometheus Tests**
   - Metric registration
   - Label parsing
   - HTTP endpoint

3. **Server Tests**
   - Start/stop functionality
   - HTTP handling
   - Concurrency safety

## Performance Considerations

1. **Memory Usage**
   - Metric cardinality
   - Label combinations
   - Storage efficiency

2. **CPU Impact**
   - Metric creation overhead
   - Label parsing
   - HTTP serving

3. **Concurrency**
   - Thread-safe operations
   - Lock contention
   - Atomic operations

## Integration Examples

### Custom Collector

```go
type CustomCollector struct {
    metrics.Collector
    // Additional fields
}

func (c *CustomCollector) IncrementCounter(name string, labels ...string) {
    // Custom logic
    c.Collector.IncrementCounter(name, labels...)
}
```

### Metric Aggregation

```go
type AggregatingCollector struct {
    metrics.Collector
    aggregations map[string]float64
}

func (c *AggregatingCollector) RecordHistogram(name string, value float64, labels ...string) {
    // Aggregate values
    c.aggregations[name] += value
    c.Collector.RecordHistogram(name, value, labels...)
}
```

### Metric Filtering

```go
type FilteringCollector struct {
    metrics.Collector
    filters map[string]bool
}

func (c *FilteringCollector) IncrementCounter(name string, labels ...string) {
    if c.filters[name] {
        c.Collector.IncrementCounter(name, labels...)
    }
}
```
