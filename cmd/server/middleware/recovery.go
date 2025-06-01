package middleware

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryMiddleware provides panic recovery middleware.
type RecoveryMiddleware struct {
	logger zerolog.Logger
}

// NewRecoveryMiddleware creates a new recovery middleware.
func NewRecoveryMiddleware(logger zerolog.Logger) *RecoveryMiddleware {
	return &RecoveryMiddleware{
		logger: logger,
	}
}

// UnaryInterceptor returns a unary server interceptor for panic recovery.
func (m *RecoveryMiddleware) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				m.handlePanic(r, info.FullMethod)
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		return handler(ctx, req)
	}
}

// StreamInterceptor returns a stream server interceptor for panic recovery.
func (m *RecoveryMiddleware) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				m.handlePanic(r, info.FullMethod)
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		return handler(srv, ss)
	}
}

// handlePanic logs panic information.
func (m *RecoveryMiddleware) handlePanic(r interface{}, method string) {
	stack := debug.Stack()

	m.logger.Error().
		Str("method", method).
		Interface("panic", r).
		Str("stack", string(stack)).
		Msg("Panic recovered")

	// Also print to stderr for debugging
	fmt.Fprintf(stderr, "PANIC in %s: %v\n%s\n", method, r, stack)
}

// stderr is used for panic output
var stderr = os.Stderr
