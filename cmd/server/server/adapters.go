// Package server implements the Flight SQL server.
package server

import (
	"time"

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

// metricsAdapter adapts MetricsCollector to the handlers/services MetricsCollector interface.
type metricsAdapter struct {
	collector MetricsCollector
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

func (m *metricsAdapter) StartTimer(name string) metricsTimer {
	return &timerAdapter{timer: m.collector.StartTimer(name)}
}

// metricsTimer wraps the Timer interface for the adapters.
type metricsTimer interface {
	Stop() time.Duration
}

// timerAdapter adapts Timer to return time.Duration.
type timerAdapter struct {
	timer Timer
}

func (t *timerAdapter) Stop() time.Duration {
	seconds := t.timer.Stop()
	return time.Duration(seconds * float64(time.Second))
}

// TransactionServiceCloser extends TransactionService with a Stop method.
type TransactionServiceCloser interface {
	Stop()
}
