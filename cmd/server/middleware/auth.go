// Package middleware provides gRPC middleware for the Flight SQL server.
package middleware

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/TFMV/porter/cmd/server/config"
)

// OAuth2Token represents an OAuth2 token with additional metadata
type OAuth2Token struct {
	AccessToken  string    `json:"access_token"`
	TokenType    string    `json:"token_type"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	ExpiresAt    time.Time `json:"expires_at"`
	Scope        string    `json:"scope,omitempty"`
	UserID       string    `json:"user_id,omitempty"`
}

// TokenStore manages OAuth2 tokens
type TokenStore struct {
	mu                sync.RWMutex
	accessTokens      map[string]*OAuth2Token
	refreshTokens     map[string]*OAuth2Token
	authorizationCode map[string]authorizationCode
}

type authorizationCode struct {
	code        string
	clientID    string
	redirectURI string
	scope       string
	expiresAt   time.Time
	userID      string
}

// AuthMiddleware provides authentication middleware.
type AuthMiddleware struct {
	config        config.AuthConfig
	logger        zerolog.Logger
	HSKey         []byte
	RSKey         interface{}
	Iss           string
	Aud           string
	tokenStore    *TokenStore
	sessionTokens map[string]string
	mu            sync.RWMutex
}

// NewAuthMiddleware creates a new authentication middleware.
func NewAuthMiddleware(cfg config.AuthConfig, logger zerolog.Logger) *AuthMiddleware {
	m := &AuthMiddleware{
		config: cfg,
		logger: logger,
		tokenStore: &TokenStore{
			accessTokens:      make(map[string]*OAuth2Token),
			refreshTokens:     make(map[string]*OAuth2Token),
			authorizationCode: make(map[string]authorizationCode),
		},
		sessionTokens: make(map[string]string),
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
	case "oauth2":
		return m.authenticateOAuth2(ctx)
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

	// Validate token against configured tokens or session tokens
	username, ok := m.config.BearerAuth.Tokens[token]
	if !ok {
		m.mu.RLock()
		username, ok = m.sessionTokens[token]
		m.mu.RUnlock()
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}
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

// authenticateOAuth2 performs OAuth2 authentication
func (m *AuthMiddleware) authenticateOAuth2(ctx context.Context) (context.Context, error) {
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

	// Validate access token
	m.tokenStore.mu.RLock()
	tokenInfo, exists := m.tokenStore.accessTokens[token]
	m.tokenStore.mu.RUnlock()

	if !exists {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}

	if time.Now().After(tokenInfo.ExpiresAt) {
		return nil, status.Error(codes.Unauthenticated, "token expired")
	}

	// Add user info to context
	ctx = context.WithValue(ctx, contextKeyUser, tokenInfo.UserID)
	return ctx, nil
}

// HandleOAuth2Authorize handles the OAuth2 authorization endpoint
func (m *AuthMiddleware) HandleOAuth2Authorize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clientID := r.URL.Query().Get("client_id")
	redirectURI := r.URL.Query().Get("redirect_uri")
	scope := r.URL.Query().Get("scope")
	responseType := r.URL.Query().Get("response_type")

	if clientID != m.config.OAuth2Auth.ClientID {
		http.Error(w, "Invalid client ID", http.StatusBadRequest)
		return
	}

	if responseType != "code" {
		http.Error(w, "Unsupported response type", http.StatusBadRequest)
		return
	}

	// Generate authorization code
	code := generateRandomString(32)
	expiresAt := time.Now().Add(10 * time.Minute)

	authCode := authorizationCode{
		code:        code,
		clientID:    clientID,
		redirectURI: redirectURI,
		scope:       scope,
		expiresAt:   expiresAt,
		userID:      "user123", // In a real implementation, this would come from user authentication
	}

	m.tokenStore.mu.Lock()
	m.tokenStore.authorizationCode[code] = authCode
	m.tokenStore.mu.Unlock()

	// Redirect back to client with authorization code
	redirectURL := fmt.Sprintf("%s?code=%s", redirectURI, code)
	http.Redirect(w, r, redirectURL, http.StatusFound)
}

// HandleOAuth2Token handles the OAuth2 token endpoint
func (m *AuthMiddleware) HandleOAuth2Token(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	grantType := r.FormValue("grant_type")

	switch grantType {
	case "authorization_code":
		m.handleAuthorizationCodeGrant(w, r)
	case "refresh_token":
		m.handleRefreshTokenGrant(w, r)
	case "client_credentials":
		m.handleClientCredentialsGrant(w, r)
	default:
		http.Error(w, "Unsupported grant type", http.StatusBadRequest)
	}
}

func (m *AuthMiddleware) handleAuthorizationCodeGrant(w http.ResponseWriter, r *http.Request) {
	code := r.FormValue("code")
	clientID := r.FormValue("client_id")
	clientSecret := r.FormValue("client_secret")
	redirectURI := r.FormValue("redirect_uri")

	// Validate client credentials
	if clientID != m.config.OAuth2Auth.ClientID || clientSecret != m.config.OAuth2Auth.ClientSecret {
		http.Error(w, "Invalid client credentials", http.StatusUnauthorized)
		return
	}

	// Validate authorization code
	m.tokenStore.mu.Lock()
	authCode, exists := m.tokenStore.authorizationCode[code]
	delete(m.tokenStore.authorizationCode, code) // Remove used code
	m.tokenStore.mu.Unlock()

	if !exists || time.Now().After(authCode.expiresAt) || authCode.redirectURI != redirectURI {
		http.Error(w, "Invalid authorization code", http.StatusBadRequest)
		return
	}

	// Generate tokens
	token := m.generateOAuth2Token(authCode.userID, authCode.scope)
	m.tokenStore.mu.Lock()
	m.tokenStore.accessTokens[token.AccessToken] = token
	if token.RefreshToken != "" {
		m.tokenStore.refreshTokens[token.RefreshToken] = token
	}
	m.tokenStore.mu.Unlock()

	// Return token response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(token); err != nil {
		m.logger.Error().Err(err).Msg("Failed to encode token response")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

func (m *AuthMiddleware) handleRefreshTokenGrant(w http.ResponseWriter, r *http.Request) {
	refreshToken := r.FormValue("refresh_token")
	clientID := r.FormValue("client_id")
	clientSecret := r.FormValue("client_secret")

	// Validate client credentials
	if clientID != m.config.OAuth2Auth.ClientID || clientSecret != m.config.OAuth2Auth.ClientSecret {
		http.Error(w, "Invalid client credentials", http.StatusUnauthorized)
		return
	}

	// Validate refresh token
	m.tokenStore.mu.Lock()
	oldToken, exists := m.tokenStore.refreshTokens[refreshToken]
	delete(m.tokenStore.refreshTokens, refreshToken)        // Remove used refresh token
	delete(m.tokenStore.accessTokens, oldToken.AccessToken) // Remove old access token
	m.tokenStore.mu.Unlock()

	if !exists {
		http.Error(w, "Invalid refresh token", http.StatusBadRequest)
		return
	}

	// Generate new tokens
	token := m.generateOAuth2Token(oldToken.UserID, oldToken.Scope)
	m.tokenStore.mu.Lock()
	m.tokenStore.accessTokens[token.AccessToken] = token
	if token.RefreshToken != "" {
		m.tokenStore.refreshTokens[token.RefreshToken] = token
	}
	m.tokenStore.mu.Unlock()

	// Return token response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(token); err != nil {
		m.logger.Error().Err(err).Msg("Failed to encode token response")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

func (m *AuthMiddleware) handleClientCredentialsGrant(w http.ResponseWriter, r *http.Request) {
	clientID := r.FormValue("client_id")
	clientSecret := r.FormValue("client_secret")
	scope := r.FormValue("scope")

	// Validate client credentials
	if clientID != m.config.OAuth2Auth.ClientID || clientSecret != m.config.OAuth2Auth.ClientSecret {
		http.Error(w, "Invalid client credentials", http.StatusUnauthorized)
		return
	}

	// Generate token without refresh token
	token := m.generateOAuth2Token(clientID, scope)
	token.RefreshToken = "" // Client credentials grant doesn't use refresh tokens

	m.tokenStore.mu.Lock()
	m.tokenStore.accessTokens[token.AccessToken] = token
	m.tokenStore.mu.Unlock()

	// Return token response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(token); err != nil {
		m.logger.Error().Err(err).Msg("Failed to encode token response")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

func (m *AuthMiddleware) generateOAuth2Token(userID, scope string) *OAuth2Token {
	token := &OAuth2Token{
		AccessToken:  generateRandomString(32),
		TokenType:    "Bearer",
		RefreshToken: generateRandomString(32),
		ExpiresAt:    time.Now().Add(m.config.OAuth2Auth.AccessTokenTTL),
		Scope:        scope,
		UserID:       userID,
	}
	return token
}

func generateRandomString(length int) string {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

// ----- Handshake helpers ----------------------------------------------------

// ValidateHandshakePayload validates the handshake payload and returns the
// authenticated user identity.
func (m *AuthMiddleware) ValidateHandshakePayload(payload []byte) (string, error) {
	if !m.config.Enabled {
		return "", nil
	}
	switch m.config.Type {
	case "basic":
		decoded, err := base64.StdEncoding.DecodeString(string(payload))
		if err != nil {
			return "", fmt.Errorf("invalid credentials encoding")
		}
		parts := strings.SplitN(string(decoded), ":", 2)
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid credentials format")
		}
		user := parts[0]
		pass := parts[1]
		info, ok := m.config.BasicAuth.Users[user]
		if !ok || subtle.ConstantTimeCompare([]byte(pass), []byte(info.Password)) != 1 {
			return "", fmt.Errorf("invalid credentials")
		}
		return user, nil
	case "bearer":
		token := string(payload)
		user, ok := m.config.BearerAuth.Tokens[token]
		if !ok {
			return "", fmt.Errorf("invalid token")
		}
		return user, nil
	default:
		return "", fmt.Errorf("handshake not supported for %s", m.config.Type)
	}
}

// CreateSessionToken stores a new session token for the given user.
func (m *AuthMiddleware) CreateSessionToken(user string) string {
	token := generateRandomString(32)
	m.mu.Lock()
	m.sessionTokens[token] = user
	m.mu.Unlock()
	return token
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
