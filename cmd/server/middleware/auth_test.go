package middleware

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"io"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/TFMV/porter/cmd/server/config"
)

func setupTestAuthMiddleware(t *testing.T, authType string) (*AuthMiddleware, config.AuthConfig) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	cfg := config.AuthConfig{
		Enabled: true,
		Type:    authType,
	}

	switch authType {
	case "basic":
		cfg.BasicAuth.Users = map[string]config.UserInfo{
			"testuser": {
				Password: "testpass",
				Roles:    []string{"admin"},
			},
		}
	case "bearer":
		cfg.BearerAuth.Tokens = map[string]string{
			"test-token": "testuser",
		}
	case "jwt":
		cfg.JWTAuth = config.JWTAuthConfig{
			Secret:   "test-secret",
			Issuer:   "test-issuer",
			Audience: "test-audience",
		}
	}

	middleware := NewAuthMiddleware(cfg, logger)
	return middleware, cfg
}

func TestNewAuthMiddleware(t *testing.T) {
	t.Run("basic auth", func(t *testing.T) {
		middleware, _ := setupTestAuthMiddleware(t, "basic")
		assert.NotNil(t, middleware)
		assert.True(t, middleware.config.Enabled)
		assert.Equal(t, "basic", middleware.config.Type)
	})

	t.Run("bearer auth", func(t *testing.T) {
		middleware, _ := setupTestAuthMiddleware(t, "bearer")
		assert.NotNil(t, middleware)
		assert.True(t, middleware.config.Enabled)
		assert.Equal(t, "bearer", middleware.config.Type)
	})

	t.Run("jwt auth", func(t *testing.T) {
		middleware, _ := setupTestAuthMiddleware(t, "jwt")
		assert.NotNil(t, middleware)
		assert.True(t, middleware.config.Enabled)
		assert.Equal(t, "jwt", middleware.config.Type)
		assert.Equal(t, "test-secret", string(middleware.HSKey))
		assert.Equal(t, "test-issuer", middleware.Iss)
		assert.Equal(t, "test-audience", middleware.Aud)
	})
}

func TestAuthMiddleware_AuthenticateBasic(t *testing.T) {
	middleware, _ := setupTestAuthMiddleware(t, "basic")

	t.Run("successful authentication", func(t *testing.T) {
		// Create valid basic auth header
		auth := base64.StdEncoding.EncodeToString([]byte("testuser:testpass"))
		md := metadata.New(map[string]string{
			"authorization": "Basic " + auth,
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		// Test authentication
		authCtx, err := middleware.authenticateBasic(ctx)
		require.NoError(t, err)

		// Verify user info in context
		user, ok := GetUser(authCtx)
		assert.True(t, ok)
		assert.Equal(t, "testuser", user)

		roles, ok := GetRoles(authCtx)
		assert.True(t, ok)
		assert.Equal(t, []string{"admin"}, roles)
	})

	t.Run("missing metadata", func(t *testing.T) {
		ctx := context.Background()
		_, err := middleware.authenticateBasic(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("missing authorization header", func(t *testing.T) {
		md := metadata.New(nil)
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, err := middleware.authenticateBasic(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("invalid authorization header", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"authorization": "Invalid test",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, err := middleware.authenticateBasic(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("invalid credentials", func(t *testing.T) {
		auth := base64.StdEncoding.EncodeToString([]byte("testuser:wrongpass"))
		md := metadata.New(map[string]string{
			"authorization": "Basic " + auth,
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, err := middleware.authenticateBasic(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})
}

func TestAuthMiddleware_AuthenticateBearer(t *testing.T) {
	middleware, _ := setupTestAuthMiddleware(t, "bearer")

	t.Run("successful authentication", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"authorization": "Bearer test-token",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		authCtx, err := middleware.authenticateBearer(ctx)
		require.NoError(t, err)

		user, ok := GetUser(authCtx)
		assert.True(t, ok)
		assert.Equal(t, "testuser", user)
	})

	t.Run("missing metadata", func(t *testing.T) {
		ctx := context.Background()
		_, err := middleware.authenticateBearer(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("missing authorization header", func(t *testing.T) {
		md := metadata.New(nil)
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, err := middleware.authenticateBearer(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("invalid token", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"authorization": "Bearer invalid-token",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, err := middleware.authenticateBearer(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})
}

func TestAuthMiddleware_AuthenticateJWT(t *testing.T) {
	middleware, _ := setupTestAuthMiddleware(t, "jwt")

	t.Run("successful authentication with HMAC", func(t *testing.T) {
		// Create a valid JWT token
		claims := jwt.MapClaims{
			"sub": "testuser",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iss": "test-issuer",
			"aud": "test-audience",
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tokenString, err := token.SignedString(middleware.HSKey)
		require.NoError(t, err)

		md := metadata.New(map[string]string{
			"authorization": "Bearer " + tokenString,
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		authCtx, err := middleware.authenticateJWT(ctx)
		require.NoError(t, err)

		user := AuthenticatedUser(authCtx)
		assert.Equal(t, "testuser", user)
	})

	t.Run("successful authentication with RSA", func(t *testing.T) {
		// Generate RSA key pair
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		middleware.RSKey = privateKey.Public()

		// Create a valid JWT token
		claims := jwt.MapClaims{
			"sub": "testuser",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iss": "test-issuer",
			"aud": "test-audience",
		}
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		tokenString, err := token.SignedString(privateKey)
		require.NoError(t, err)

		md := metadata.New(map[string]string{
			"authorization": "Bearer " + tokenString,
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		authCtx, err := middleware.authenticateJWT(ctx)
		require.NoError(t, err)

		user := AuthenticatedUser(authCtx)
		assert.Equal(t, "testuser", user)
	})

	t.Run("successful authentication with ECDSA", func(t *testing.T) {
		// Generate ECDSA key pair
		privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		middleware.RSKey = privateKey.Public()

		// Create a valid JWT token
		claims := jwt.MapClaims{
			"sub": "testuser",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iss": "test-issuer",
			"aud": "test-audience",
		}
		token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
		tokenString, err := token.SignedString(privateKey)
		require.NoError(t, err)

		md := metadata.New(map[string]string{
			"authorization": "Bearer " + tokenString,
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		authCtx, err := middleware.authenticateJWT(ctx)
		require.NoError(t, err)

		user := AuthenticatedUser(authCtx)
		assert.Equal(t, "testuser", user)
	})

	t.Run("missing metadata", func(t *testing.T) {
		ctx := context.Background()
		_, err := middleware.authenticateJWT(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("missing authorization header", func(t *testing.T) {
		md := metadata.New(nil)
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, err := middleware.authenticateJWT(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("invalid token", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"authorization": "Bearer invalid.token.here",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, err := middleware.authenticateJWT(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("expired token", func(t *testing.T) {
		claims := jwt.MapClaims{
			"sub": "testuser",
			"exp": time.Now().Add(-time.Hour).Unix(),
			"iss": "test-issuer",
			"aud": "test-audience",
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tokenString, err := token.SignedString(middleware.HSKey)
		require.NoError(t, err)

		md := metadata.New(map[string]string{
			"authorization": "Bearer " + tokenString,
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		_, err = middleware.authenticateJWT(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("invalid issuer", func(t *testing.T) {
		claims := jwt.MapClaims{
			"sub": "testuser",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iss": "wrong-issuer",
			"aud": "test-audience",
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tokenString, err := token.SignedString(middleware.HSKey)
		require.NoError(t, err)

		md := metadata.New(map[string]string{
			"authorization": "Bearer " + tokenString,
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		_, err = middleware.authenticateJWT(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("invalid audience", func(t *testing.T) {
		claims := jwt.MapClaims{
			"sub": "testuser",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iss": "test-issuer",
			"aud": "wrong-audience",
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tokenString, err := token.SignedString(middleware.HSKey)
		require.NoError(t, err)

		md := metadata.New(map[string]string{
			"authorization": "Bearer " + tokenString,
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		_, err = middleware.authenticateJWT(ctx)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})
}

func TestAuthMiddleware_ValidateRedirectURI(t *testing.T) {
	tests := []struct {
		name          string
		allowedURIs   []string
		redirectURI   string
		expectError   bool
		errorContains string
	}{
		{
			name:        "valid redirect URI",
			allowedURIs: []string{"http://localhost:8080/callback", "https://myapp.com/oauth/callback"},
			redirectURI: "http://localhost:8080/callback",
			expectError: false,
		},
		{
			name:        "valid redirect URI with https",
			allowedURIs: []string{"http://localhost:8080/callback", "https://myapp.com/oauth/callback"},
			redirectURI: "https://myapp.com/oauth/callback",
			expectError: false,
		},
		{
			name:          "empty redirect URI",
			allowedURIs:   []string{"http://localhost:8080/callback"},
			redirectURI:   "",
			expectError:   true,
			errorContains: "redirect URI is required",
		},
		{
			name:          "invalid URL format",
			allowedURIs:   []string{"http://localhost:8080/callback"},
			redirectURI:   "://missing-scheme.com/callback",
			expectError:   true,
			errorContains: "invalid redirect URI format",
		},
		{
			name:          "malformed percent-encoding",
			allowedURIs:   []string{"http://localhost:8080/callback"},
			redirectURI:   "http://%41:8080/",
			expectError:   true,
			errorContains: "invalid redirect URI format",
		},
		{
			name:          "unsupported scheme",
			allowedURIs:   []string{"http://localhost:8080/callback"},
			redirectURI:   "ftp://example.com/callback",
			expectError:   true,
			errorContains: "redirect URI must use http or https scheme",
		},
		{
			name:          "redirect URI not in allowed list",
			allowedURIs:   []string{"http://localhost:8080/callback"},
			redirectURI:   "http://malicious.com/callback",
			expectError:   true,
			errorContains: "redirect URI not in allowed list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.AuthConfig{
				Enabled: true,
				Type:    "oauth2",
				OAuth2Auth: config.OAuth2Config{
					ClientID:            "test-client",
					ClientSecret:        "test-secret",
					AllowedRedirectURIs: tt.allowedURIs,
				},
			}

			logger := zerolog.New(io.Discard)
			middleware := NewAuthMiddleware(cfg, logger)

			err := middleware.validateRedirectURI(tt.redirectURI)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAuthMiddleware_UnaryInterceptor(t *testing.T) {
	middleware, _ := setupTestAuthMiddleware(t, "basic")

	t.Run("health check bypass", func(t *testing.T) {
		info := &grpc.UnaryServerInfo{
			FullMethod: "/grpc.health.v1.Health/Check",
		}
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return "ok", nil
		}

		resp, err := middleware.UnaryInterceptor()(context.Background(), nil, info, handler)
		require.NoError(t, err)
		assert.Equal(t, "ok", resp)
	})

	t.Run("authentication required", func(t *testing.T) {
		info := &grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Method",
		}
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return "ok", nil
		}

		_, err := middleware.UnaryInterceptor()(context.Background(), nil, info, handler)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})
}

func TestAuthMiddleware_StreamInterceptor(t *testing.T) {
	middleware, _ := setupTestAuthMiddleware(t, "basic")

	t.Run("health check bypass", func(t *testing.T) {
		info := &grpc.StreamServerInfo{
			FullMethod: "/grpc.health.v1.Health/Watch",
		}
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			return nil
		}

		stream := &mockServerStream{
			ctx: context.Background(),
		}
		err := middleware.StreamInterceptor()(nil, stream, info, handler)
		require.NoError(t, err)
	})

	t.Run("authentication required", func(t *testing.T) {
		info := &grpc.StreamServerInfo{
			FullMethod: "/test.Service/Stream",
		}
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			return nil
		}

		stream := &mockServerStream{
			ctx: context.Background(),
		}
		err := middleware.StreamInterceptor()(nil, stream, info, handler)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})
}

// mockServerStream implements grpc.ServerStream for testing
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *mockServerStream) Context() context.Context {
	return s.ctx
}
