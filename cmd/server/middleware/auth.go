// Package middleware provides gRPC middleware for the Flight SQL server.
package middleware

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
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
	HSKey  []byte
	RSKey  interface{}
	Iss    string
	Aud    string
}

// NewAuthMiddleware creates a new authentication middleware.
func NewAuthMiddleware(cfg config.AuthConfig, logger zerolog.Logger) *AuthMiddleware {
	m := &AuthMiddleware{
		config: cfg,
		logger: logger,
	}

	// Initialize JWT-related fields if JWT auth is enabled
	if cfg.Type == "jwt" {
		if cfg.JWTAuth.Secret != "" {
			m.HSKey = []byte(cfg.JWTAuth.Secret)
		}
		m.Iss = cfg.JWTAuth.Issuer
		m.Aud = cfg.JWTAuth.Audience
	}

	return m
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
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, status.Error(codes.Unauthenticated, "missing metadata")
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return ctx, status.Error(codes.Unauthenticated, "no authorization header")
	}

	// We only inspect the first header; multiple are unusual for gRPC.
	raw := strings.TrimSpace(authHeaders[0])
	const bearer = "bearer "
	if len(raw) < len(bearer) || !strings.EqualFold(raw[:len(bearer)], bearer) {
		return ctx, status.Error(codes.Unauthenticated, "authorization header is not Bearer")
	}
	tokenString := strings.TrimSpace(raw[len(bearer):])
	if tokenString == "" {
		return ctx, status.Error(codes.Unauthenticated, "empty bearer token")
	}

	// Custom key‑func that selects the proper key based on signing method.
	keyFunc := func(t *jwt.Token) (any, error) {
		switch t.Method.(type) {
		case *jwt.SigningMethodHMAC:
			if len(m.HSKey) == 0 {
				return nil, fmt.Errorf("unexpected HMAC token")
			}
			return m.HSKey, nil
		case *jwt.SigningMethodRSA, *jwt.SigningMethodECDSA:
			if m.RSKey == nil {
				return nil, fmt.Errorf("unexpected asymmetric‑key token")
			}
			return m.RSKey, nil
		default:
			return nil, fmt.Errorf("unsupported signing algorithm %s", t.Method.Alg())
		}
	}

	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, keyFunc,
		jwt.WithValidMethods([]string{
			jwt.SigningMethodHS256.Alg(),
			jwt.SigningMethodHS384.Alg(),
			jwt.SigningMethodHS512.Alg(),
			jwt.SigningMethodRS256.Alg(),
			jwt.SigningMethodRS384.Alg(),
			jwt.SigningMethodRS512.Alg(),
			jwt.SigningMethodES256.Alg(),
			jwt.SigningMethodES384.Alg(),
			jwt.SigningMethodES512.Alg(),
		}))
	if err != nil || !token.Valid {
		return ctx, status.Error(codes.Unauthenticated, "invalid JWT: "+err.Error())
	}

	// ---- additional claim checks ------------------------------------------------
	if iss := m.Iss; iss != "" {
		if iclaim, _ := claims["iss"].(string); iclaim != iss {
			return ctx, status.Error(codes.Unauthenticated, "issuer mismatch")
		}
	}
	if aud := m.Aud; aud != "" {
		switch v := claims["aud"].(type) {
		case string:
			if v != aud {
				return ctx, status.Error(codes.Unauthenticated, "audience mismatch")
			}
		case []any:
			found := false
			for _, a := range v {
				if s, ok := a.(string); ok && s == aud {
					found = true
					break
				}
			}
			if !found {
				return ctx, status.Error(codes.Unauthenticated, "audience mismatch")
			}
		}
	}
	// Expiry is validated automatically by jwt.ParseWithClaims, but we ensure
	// the claim exists so Missing exp causes rejection.
	if _, ok := claims["exp"]; !ok {
		return ctx, status.Error(codes.Unauthenticated, "missing exp claim")
	}
	exp, ok := claims["exp"].(float64)
	if !ok {
		return ctx, status.Error(codes.Unauthenticated, "invalid exp claim type")
	}
	if time.Unix(int64(exp), 0).Before(time.Now()) {
		return ctx, status.Error(codes.Unauthenticated, "token expired")
	}

	// Subject becomes the logical user identity.
	sub, _ := claims["sub"].(string)
	return context.WithValue(ctx, ctxUserKey{}, sub), nil
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

// ctxUserKey is the context key under which the authenticated user ( sub  claim)
// is stored after successful JWT verification.
type ctxUserKey struct{}

// AuthenticatedUser returns the user‐id (JWT "sub") previously placed in the
// context by the authentication middleware, or "" if absent.
func AuthenticatedUser(ctx context.Context) string {
	if v, ok := ctx.Value(ctxUserKey{}).(string); ok {
		return v
	}
	return ""
}
