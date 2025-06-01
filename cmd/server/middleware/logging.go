package middleware

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LoggingMiddleware provides request logging middleware.
type LoggingMiddleware struct {
	logger zerolog.Logger
}

// NewLoggingMiddleware creates a new logging middleware.
func NewLoggingMiddleware(logger zerolog.Logger) *LoggingMiddleware {
	return &LoggingMiddleware{
		logger: logger,
	}
}

// UnaryInterceptor returns a unary server interceptor for logging.
func (m *LoggingMiddleware) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		// Get user from context if available
		user, _ := GetUser(ctx)

		// Call handler
		resp, err := handler(ctx, req)

		// Log request
		duration := time.Since(start)
		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}

		event := m.logger.Info()
		if err != nil && code != codes.Canceled {
			event = m.logger.Error().Err(err)
		}

		event.
			Str("method", info.FullMethod).
			Str("user", user).
			Dur("duration", duration).
			Str("code", code.String()).
			Msg("Unary request")

		return resp, err
	}
}

// StreamInterceptor returns a stream server interceptor for logging.
func (m *LoggingMiddleware) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		// Get user from context if available
		user, _ := GetUser(ss.Context())

		// Wrap stream to track messages
		wrappedStream := &loggingServerStream{
			ServerStream: ss,
			method:       info.FullMethod,
			logger:       m.logger,
		}

		// Call handler
		err := handler(srv, wrappedStream)

		// Log request
		duration := time.Since(start)
		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}

		event := m.logger.Info()
		if err != nil && code != codes.Canceled {
			event = m.logger.Error().Err(err)
		}

		event.
			Str("method", info.FullMethod).
			Str("user", user).
			Dur("duration", duration).
			Str("code", code.String()).
			Int("messages_sent", wrappedStream.messagesSent).
			Int("messages_received", wrappedStream.messagesReceived).
			Msg("Stream request")

		return err
	}
}

// loggingServerStream wraps a ServerStream to track message counts.
type loggingServerStream struct {
	grpc.ServerStream
	method           string
	logger           zerolog.Logger
	messagesSent     int
	messagesReceived int
}

func (s *loggingServerStream) SendMsg(m interface{}) error {
	err := s.ServerStream.SendMsg(m)
	if err == nil {
		s.messagesSent++
	}
	return err
}

func (s *loggingServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		s.messagesReceived++
	}
	return err
}
