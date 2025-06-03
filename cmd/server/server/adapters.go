// Package server implements the Flight SQL server.
package server

import (
	"time"

	"github.com/TFMV/hatch/pkg/handlers"
	"github.com/TFMV/hatch/pkg/services"
	"github.com/rs/zerolog"
)

// loggerAdapter adapts zerolog to the handlers/services Logger interface.
type loggerAdapter struct {
	logger zerolog.Logger
}

func (l *loggerAdapter) Debug(msg string, keysAndValues ...interface{}) {
	event := l.logger.Debug()
	l.addFields(event, keysAndValues...)
	event.Msg(msg)
}

func (l *loggerAdapter) Info(msg string, keysAndValues ...interface{}) {
	event := l.logger.Info()
	l.addFields(event, keysAndValues...)
	event.Msg(msg)
}

func (l *loggerAdapter) Warn(msg string, keysAndValues ...interface{}) {
	event := l.logger.Warn()
	l.addFields(event, keysAndValues...)
	event.Msg(msg)
}

func (l *loggerAdapter) Error(msg string, keysAndValues ...interface{}) {
	event := l.logger.Error()
	l.addFields(event, keysAndValues...)
	event.Msg(msg)
}

func (l *loggerAdapter) addFields(event *zerolog.Event, keysAndValues ...interface{}) {
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := keysAndValues[i].(string)
			value := keysAndValues[i+1]

			switch v := value.(type) {
			case string:
				event.Str(key, v)
			case int:
				event.Int(key, v)
			case int32:
				event.Int32(key, v)
			case int64:
				event.Int64(key, v)
			case float64:
				event.Float64(key, v)
			case bool:
				event.Bool(key, v)
			case error:
				event.Err(v)
			case time.Duration:
				event.Dur(key, v)
			case time.Time:
				event.Time(key, v)
			default:
				event.Interface(key, v)
			}
		}
	}
}

// handlerMetricsAdapter adapts MetricsCollector to the handlers.MetricsCollector interface.
type handlerMetricsAdapter struct {
	collector MetricsCollector
}

func (m *handlerMetricsAdapter) IncrementCounter(name string, tags ...string) {
	m.collector.IncrementCounter(name, tags...)
}

func (m *handlerMetricsAdapter) RecordHistogram(name string, value float64, tags ...string) {
	m.collector.RecordHistogram(name, value, tags...)
}

func (m *handlerMetricsAdapter) RecordGauge(name string, value float64, tags ...string) {
	m.collector.RecordGauge(name, value, tags...)
}

func (m *handlerMetricsAdapter) StartTimer(name string) handlers.Timer {
	return &handlerTimerAdapter{timer: m.collector.StartTimer(name)}
}

// handlerTimerAdapter adapts Timer to handlers.Timer interface.
type handlerTimerAdapter struct {
	timer Timer
}

func (t *handlerTimerAdapter) Stop() {
	t.timer.Stop()
}

// serviceMetricsAdapter adapts MetricsCollector to the services.MetricsCollector interface.
type serviceMetricsAdapter struct {
	collector MetricsCollector
}

func (m *serviceMetricsAdapter) IncrementCounter(name string, labels ...string) {
	m.collector.IncrementCounter(name, labels...)
}

func (m *serviceMetricsAdapter) RecordHistogram(name string, value float64, labels ...string) {
	m.collector.RecordHistogram(name, value, labels...)
}

func (m *serviceMetricsAdapter) RecordGauge(name string, value float64, labels ...string) {
	m.collector.RecordGauge(name, value, labels...)
}

func (m *serviceMetricsAdapter) StartTimer(name string) services.Timer {
	return &serviceTimerAdapter{timer: m.collector.StartTimer(name), start: time.Now()}
}

// serviceTimerAdapter adapts Timer to services.Timer interface.
type serviceTimerAdapter struct {
	timer Timer
	start time.Time
}

func (t *serviceTimerAdapter) Stop() time.Duration {
	t.timer.Stop()
	return time.Since(t.start)
}

// TransactionServiceCloser extends TransactionService with a Stop method.
type TransactionServiceCloser interface {
	Stop()
}
