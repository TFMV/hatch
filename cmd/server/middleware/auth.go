// Package middleware provides gRPC middleware for the Flight SQL server.
package middleware

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"strings"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/TFMV/flight/cmd/server/config"
)

// AuthMiddleware provides authentication middleware.
type AuthMiddleware struct {
	config config.AuthConfig
	logger zerolog.Logger
}

// NewAuthMiddleware creates a new authentication middleware.
func NewAuthMiddleware(cfg config.AuthConfig, logger zerolog.Logger) *AuthMiddleware {
	return &AuthMiddleware{
		config: cfg,
		logger: logger,
	}
}

// UnaryInterceptor returns a unary server interceptor for authentication.
func (m *AuthMiddleware) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Skip auth for health checks
		if strings.Contains(info.FullMethod, "grpc.health") {
			return handler(ctx, req)
		}

		authCtx, err := m.authenticate(ctx)
		if err != nil {
			m.logger.Warn().Err(err).Str("method", info.FullMethod).Msg("Authentication failed")
			return nil, err
		}

		return handler(authCtx, req)
	}
}

// StreamInterceptor returns a stream server interceptor for authentication.
func (m *AuthMiddleware) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Skip auth for health checks
		if strings.Contains(info.FullMethod, "grpc.health") {
			return handler(srv, ss)
		}

		authCtx, err := m.authenticate(ss.Context())
		if err != nil {
			m.logger.Warn().Err(err).Str("method", info.FullMethod).Msg("Authentication failed")
			return err
		}

		// Wrap the stream with authenticated context
		wrappedStream := &authServerStream{
			ServerStream: ss,
			ctx:          authCtx,
		}

		return handler(srv, wrappedStream)
	}
}

// authenticate performs authentication based on configured type.
func (m *AuthMiddleware) authenticate(ctx context.Context) (context.Context, error) {
	if !m.config.Enabled {
		return ctx, nil
	}

	switch m.config.Type {
	case "basic":
		return m.authenticateBasic(ctx)
	case "bearer":
		return m.authenticateBearer(ctx)
	case "jwt":
		return m.authenticateJWT(ctx)
	default:
		return nil, status.Errorf(codes.Internal, "unsupported auth type: %s", m.config.Type)
	}
}

// authenticateBasic performs basic authentication.
func (m *AuthMiddleware) authenticateBasic(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing authorization header")
	}

	authHeader := authHeaders[0]
	if !strings.HasPrefix(authHeader, "Basic ") {
		return nil, status.Error(codes.Unauthenticated, "invalid authorization header")
	}

	// Decode credentials
	encoded := strings.TrimPrefix(authHeader, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials encoding")
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials format")
	}

	username := parts[0]
	password := parts[1]

	// Validate credentials
	userInfo, ok := m.config.BasicAuth.Users[username]
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	// Constant time comparison to prevent timing attacks
	if subtle.ConstantTimeCompare([]byte(password), []byte(userInfo.Password)) != 1 {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	// Add user info to context
	ctx = context.WithValue(ctx, contextKeyUser, username)
	ctx = context.WithValue(ctx, contextKeyRoles, userInfo.Roles)

	return ctx, nil
}

// authenticateBearer performs bearer token authentication.
func (m *AuthMiddleware) authenticateBearer(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing authorization header")
	}

	authHeader := authHeaders[0]
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, status.Error(codes.Unauthenticated, "invalid authorization header")
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")

	// Validate token
	username, ok := m.config.BearerAuth.Tokens[token]
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}

	// Add user info to context
	ctx = context.WithValue(ctx, contextKeyUser, username)

	return ctx, nil
}

// authenticateJWT performs JWT authentication.
func (m *AuthMiddleware) authenticateJWT(ctx context.Context) (context.Context, error) {
	// TODO: Implement JWT authentication
	return nil, status.Error(codes.Unimplemented, "JWT authentication not yet implemented")
}

// Context keys for authentication
type contextKey string

const (
	contextKeyUser  contextKey = "user"
	contextKeyRoles contextKey = "roles"
)

// GetUser extracts the authenticated user from context.
func GetUser(ctx context.Context) (string, bool) {
	user, ok := ctx.Value(contextKeyUser).(string)
	return user, ok
}

// GetRoles extracts the user's roles from context.
func GetRoles(ctx context.Context) ([]string, bool) {
	roles, ok := ctx.Value(contextKeyRoles).([]string)
	return roles, ok
}

// authServerStream wraps a ServerStream with authenticated context.
type authServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authServerStream) Context() context.Context {
	return s.ctx
}
