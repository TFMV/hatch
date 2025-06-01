package middleware

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MetricsCollector defines the interface for collecting metrics.
type MetricsCollector interface {
	IncrementCounter(name string, labels ...string)
	RecordHistogram(name string, value float64, labels ...string)
	RecordGauge(name string, value float64, labels ...string)
	StartTimer(name string) Timer
}

// Timer represents a timing measurement.
type Timer interface {
	Stop() float64
}

// MetricsMiddleware provides metrics collection middleware.
type MetricsMiddleware struct {
	collector MetricsCollector
}

// NewMetricsMiddleware creates a new metrics middleware.
func NewMetricsMiddleware(collector MetricsCollector) *MetricsMiddleware {
	return &MetricsMiddleware{
		collector: collector,
	}
}

// UnaryInterceptor returns a unary server interceptor for metrics.
func (m *MetricsMiddleware) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		timer := m.collector.StartTimer("grpc_request_duration")
		defer func() {
			duration := timer.Stop()
			m.collector.RecordHistogram("grpc_request_duration_seconds", duration, "method", info.FullMethod, "type", "unary")
		}()

		// Increment request counter
		m.collector.IncrementCounter("grpc_requests_total", "method", info.FullMethod, "type", "unary")

		// Call handler
		resp, err := handler(ctx, req)

		// Record response status
		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}
		m.collector.IncrementCounter("grpc_responses_total", "method", info.FullMethod, "type", "unary", "code", code.String())

		return resp, err
	}
}

// StreamInterceptor returns a stream server interceptor for metrics.
func (m *MetricsMiddleware) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		timer := m.collector.StartTimer("grpc_stream_duration")
		defer func() {
			duration := timer.Stop()
			m.collector.RecordHistogram("grpc_stream_duration_seconds", duration, "method", info.FullMethod)
		}()

		// Increment stream counter
		m.collector.IncrementCounter("grpc_streams_total", "method", info.FullMethod)

		// Wrap stream to track messages
		wrappedStream := &metricsServerStream{
			ServerStream: ss,
			collector:    m.collector,
			method:       info.FullMethod,
		}

		// Call handler
		err := handler(srv, wrappedStream)

		// Record stream status
		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}
		m.collector.IncrementCounter("grpc_stream_status_total", "method", info.FullMethod, "code", code.String())

		return err
	}
}

// metricsServerStream wraps a ServerStream to track message metrics.
type metricsServerStream struct {
	grpc.ServerStream
	collector MetricsCollector
	method    string
}

func (s *metricsServerStream) SendMsg(m interface{}) error {
	timer := s.collector.StartTimer("grpc_stream_send_duration")
	err := s.ServerStream.SendMsg(m)
	duration := timer.Stop()

	s.collector.RecordHistogram("grpc_stream_send_duration_seconds", duration, "method", s.method)
	s.collector.IncrementCounter("grpc_stream_messages_sent_total", "method", s.method)

	if err != nil {
		s.collector.IncrementCounter("grpc_stream_send_errors_total", "method", s.method)
	}

	return err
}

func (s *metricsServerStream) RecvMsg(m interface{}) error {
	timer := s.collector.StartTimer("grpc_stream_recv_duration")
	err := s.ServerStream.RecvMsg(m)
	duration := timer.Stop()

	s.collector.RecordHistogram("grpc_stream_recv_duration_seconds", duration, "method", s.method)
	s.collector.IncrementCounter("grpc_stream_messages_received_total", "method", s.method)

	if err != nil {
		s.collector.IncrementCounter("grpc_stream_recv_errors_total", "method", s.method)
	}

	return err
}
