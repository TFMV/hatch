// Package config provides configuration structures for the Flight SQL server.
package config

import (
	"fmt"
	"time"
)

// Config represents the server configuration.
type Config struct {
	// Server settings
	Address           string        `yaml:"address" json:"address"`
	Database          string        `yaml:"database" json:"database"`
	Token             string        `yaml:"token" json:"token"`
	LogLevel          string        `yaml:"log_level" json:"log_level"`
	MaxConnections    int           `yaml:"max_connections" json:"max_connections"`
	ConnectionTimeout time.Duration `yaml:"connection_timeout" json:"connection_timeout"`
	QueryTimeout      time.Duration `yaml:"query_timeout" json:"query_timeout"`
	MaxMessageSize    int64         `yaml:"max_message_size" json:"max_message_size"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`

	// TLS configuration
	TLS TLSConfig `yaml:"tls" json:"tls"`

	// Authentication configuration
	Auth AuthConfig `yaml:"auth" json:"auth"`

	// Metrics configuration
	Metrics MetricsConfig `yaml:"metrics" json:"metrics"`

	// Health check configuration
	Health HealthConfig `yaml:"health" json:"health"`

	// gRPC reflection
	Reflection bool `yaml:"reflection" json:"reflection"`

	// Connection pool configuration
	ConnectionPool ConnectionPoolConfig `yaml:"connection_pool" json:"connection_pool"`

	// Transaction configuration
	Transaction TransactionConfig `yaml:"transaction" json:"transaction"`

	// Cache configuration
	Cache CacheConfig `yaml:"cache" json:"cache"`

	// Performance options
	SafeCopy bool `yaml:"safe_copy" json:"safe_copy"`
}

// TLSConfig represents TLS configuration.
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	CertFile string `yaml:"cert_file" json:"cert_file"`
	KeyFile  string `yaml:"key_file" json:"key_file"`
	CAFile   string `yaml:"ca_file" json:"ca_file"`
	// Mutual TLS
	ClientAuth         bool   `yaml:"client_auth" json:"client_auth"`
	ClientCACertFile   string `yaml:"client_ca_cert_file" json:"client_ca_cert_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify" json:"insecure_skip_verify"`
}

// AuthConfig represents authentication configuration.
type AuthConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Type    string `yaml:"type" json:"type"` // basic, bearer, mtls, oauth2

	// Basic auth
	BasicAuth BasicAuthConfig `yaml:"basic_auth" json:"basic_auth"`

	// Bearer token auth
	BearerAuth BearerAuthConfig `yaml:"bearer_auth" json:"bearer_auth"`

	// JWT auth
	JWTAuth JWTAuthConfig `yaml:"jwt_auth" json:"jwt_auth"`

	// OAuth2 auth
	OAuth2Auth OAuth2Config `yaml:"oauth2_auth" json:"oauth2_auth"`
}

// BasicAuthConfig represents basic authentication configuration.
type BasicAuthConfig struct {
	UsersFile string              `yaml:"users_file" json:"users_file"`
	Users     map[string]UserInfo `yaml:"users" json:"users"`
}

// UserInfo represents user information.
type UserInfo struct {
	Password string   `yaml:"password" json:"password"`
	Roles    []string `yaml:"roles" json:"roles"`
}

// BearerAuthConfig represents bearer token authentication configuration.
type BearerAuthConfig struct {
	TokensFile string            `yaml:"tokens_file" json:"tokens_file"`
	Tokens     map[string]string `yaml:"tokens" json:"tokens"` // token -> username
}

// JWTAuthConfig represents JWT authentication configuration.
type JWTAuthConfig struct {
	Secret   string `yaml:"secret" json:"secret"`
	Issuer   string `yaml:"issuer" json:"issuer"`
	Audience string `yaml:"audience" json:"audience"`
}

// OAuth2Config represents OAuth2 authentication configuration.
type OAuth2Config struct {
	ClientID            string        `yaml:"client_id" json:"client_id"`
	ClientSecret        string        `yaml:"client_secret" json:"client_secret"`
	AuthorizeEndpoint   string        `yaml:"authorize_endpoint" json:"authorize_endpoint"`
	TokenEndpoint       string        `yaml:"token_endpoint" json:"token_endpoint"`
	RedirectURL         string        `yaml:"redirect_url" json:"redirect_url"`
	AllowedRedirectURIs []string      `yaml:"allowed_redirect_uris" json:"allowed_redirect_uris"`
	Scopes              []string      `yaml:"scopes" json:"scopes"`
	AccessTokenTTL      time.Duration `yaml:"access_token_ttl" json:"access_token_ttl"`
	RefreshTokenTTL     time.Duration `yaml:"refresh_token_ttl" json:"refresh_token_ttl"`
	AllowedGrantTypes   []string      `yaml:"allowed_grant_types" json:"allowed_grant_types"`
}

// MetricsConfig represents metrics configuration.
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Address string `yaml:"address" json:"address"`
	Path    string `yaml:"path" json:"path"`
}

// HealthConfig represents health check configuration.
type HealthConfig struct {
	Enabled  bool          `yaml:"enabled" json:"enabled"`
	Interval time.Duration `yaml:"interval" json:"interval"`
}

// ConnectionPoolConfig represents connection pool configuration.
type ConnectionPoolConfig struct {
	MaxOpenConnections int           `yaml:"max_open_connections" json:"max_open_connections"`
	MaxIdleConnections int           `yaml:"max_idle_connections" json:"max_idle_connections"`
	ConnMaxLifetime    time.Duration `yaml:"conn_max_lifetime" json:"conn_max_lifetime"`
	ConnMaxIdleTime    time.Duration `yaml:"conn_max_idle_time" json:"conn_max_idle_time"`
	HealthCheckPeriod  time.Duration `yaml:"health_check_period" json:"health_check_period"`
}

// TransactionConfig represents transaction configuration.
type TransactionConfig struct {
	DefaultIsolationLevel string        `yaml:"default_isolation_level" json:"default_isolation_level"`
	MaxTransactionAge     time.Duration `yaml:"max_transaction_age" json:"max_transaction_age"`
	CleanupInterval       time.Duration `yaml:"cleanup_interval" json:"cleanup_interval"`
}

// CacheConfig represents cache configuration.
type CacheConfig struct {
	Enabled            bool            `yaml:"enabled" json:"enabled"`
	MaxSize            int64           `yaml:"max_size" json:"max_size"`
	TTL                time.Duration   `yaml:"ttl" json:"ttl"`
	CleanupInterval    time.Duration   `yaml:"cleanup_interval" json:"cleanup_interval"`
	EnableStats        bool            `yaml:"enable_stats" json:"enable_stats"`
	PreparedStatements CacheItemConfig `yaml:"prepared_statements" json:"prepared_statements"`
	QueryResults       CacheItemConfig `yaml:"query_results" json:"query_results"`
}

// CacheItemConfig represents cache item configuration.
type CacheItemConfig struct {
	Enabled bool          `yaml:"enabled" json:"enabled"`
	MaxSize int64         `yaml:"max_size" json:"max_size"`
	TTL     time.Duration `yaml:"ttl" json:"ttl"`
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Address == "" {
		return fmt.Errorf("address is required")
	}

	if c.MaxConnections <= 0 {
		c.MaxConnections = 100
	}

	if c.ConnectionTimeout <= 0 {
		c.ConnectionTimeout = 30 * time.Second
	}

	if c.QueryTimeout <= 0 {
		c.QueryTimeout = 5 * time.Minute
	}

	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = 16 * 1024 * 1024 // 16MB
	}

	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}

	// Validate TLS
	if c.TLS.Enabled {
		if c.TLS.CertFile == "" || c.TLS.KeyFile == "" {
			return fmt.Errorf("TLS cert and key files are required when TLS is enabled")
		}
	}

	// Validate auth
	if c.Auth.Enabled {
		switch c.Auth.Type {
		case "basic":
			if len(c.Auth.BasicAuth.Users) == 0 && c.Auth.BasicAuth.UsersFile == "" {
				return fmt.Errorf("basic auth requires users or users file")
			}
		case "bearer":
			if len(c.Auth.BearerAuth.Tokens) == 0 && c.Auth.BearerAuth.TokensFile == "" {
				return fmt.Errorf("bearer auth requires tokens or tokens file")
			}
		case "jwt":
			if c.Auth.JWTAuth.Secret == "" {
				return fmt.Errorf("JWT auth requires secret")
			}
		case "oauth2":
			if c.Auth.OAuth2Auth.ClientID == "" || c.Auth.OAuth2Auth.ClientSecret == "" {
				return fmt.Errorf("OAuth2 auth requires client ID and secret")
			}
			if c.Auth.OAuth2Auth.AuthorizeEndpoint == "" || c.Auth.OAuth2Auth.TokenEndpoint == "" {
				return fmt.Errorf("OAuth2 auth requires authorize and token endpoints")
			}
			if len(c.Auth.OAuth2Auth.AllowedRedirectURIs) == 0 {
				return fmt.Errorf("OAuth2 auth requires at least one allowed redirect URI for security")
			}
			if c.Auth.OAuth2Auth.AccessTokenTTL <= 0 {
				c.Auth.OAuth2Auth.AccessTokenTTL = 1 * time.Hour
			}
			if c.Auth.OAuth2Auth.RefreshTokenTTL <= 0 {
				c.Auth.OAuth2Auth.RefreshTokenTTL = 24 * time.Hour
			}
			if len(c.Auth.OAuth2Auth.AllowedGrantTypes) == 0 {
				c.Auth.OAuth2Auth.AllowedGrantTypes = []string{"authorization_code", "refresh_token", "client_credentials"}
			}
		default:
			return fmt.Errorf("unsupported auth type: %s", c.Auth.Type)
		}
	}

	// Set defaults for connection pool
	if c.ConnectionPool.MaxOpenConnections <= 0 {
		c.ConnectionPool.MaxOpenConnections = 25
	}
	if c.ConnectionPool.MaxIdleConnections <= 0 {
		c.ConnectionPool.MaxIdleConnections = 5
	}
	if c.ConnectionPool.ConnMaxLifetime <= 0 {
		c.ConnectionPool.ConnMaxLifetime = 30 * time.Minute
	}
	if c.ConnectionPool.ConnMaxIdleTime <= 0 {
		c.ConnectionPool.ConnMaxIdleTime = 10 * time.Minute
	}
	if c.ConnectionPool.HealthCheckPeriod <= 0 {
		c.ConnectionPool.HealthCheckPeriod = 1 * time.Minute
	}

	// Set defaults for transactions
	if c.Transaction.MaxTransactionAge <= 0 {
		c.Transaction.MaxTransactionAge = 1 * time.Hour
	}
	if c.Transaction.CleanupInterval <= 0 {
		c.Transaction.CleanupInterval = 5 * time.Minute
	}

	// Set defaults for metrics
	if c.Metrics.Path == "" {
		c.Metrics.Path = "/metrics"
	}

	return nil
}

// LoadFromFile loads configuration from a YAML or JSON file.
func LoadFromFile(path string) (*Config, error) {
	// Implementation would load from file
	// For now, return default config
	return DefaultConfig(), nil
}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Config {
	return &Config{
		Address:           "0.0.0.0:8815",
		Database:          ":memory:",
		Token:             "",
		LogLevel:          "info",
		MaxConnections:    100,
		ConnectionTimeout: 30 * time.Second,
		QueryTimeout:      5 * time.Minute,
		MaxMessageSize:    16 * 1024 * 1024,
		ShutdownTimeout:   30 * time.Second,
		TLS: TLSConfig{
			Enabled: false,
		},
		Auth: AuthConfig{
			Enabled: false,
			Type:    "basic",
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Address: ":9090",
			Path:    "/metrics",
		},
		Health: HealthConfig{
			Enabled:  true,
			Interval: 10 * time.Second,
		},
		Reflection: true,
		ConnectionPool: ConnectionPoolConfig{
			MaxOpenConnections: 25,
			MaxIdleConnections: 5,
			ConnMaxLifetime:    30 * time.Minute,
			ConnMaxIdleTime:    10 * time.Minute,
			HealthCheckPeriod:  1 * time.Minute,
		},
		Transaction: TransactionConfig{
			DefaultIsolationLevel: "READ_COMMITTED",
			MaxTransactionAge:     1 * time.Hour,
			CleanupInterval:       5 * time.Minute,
		},
		Cache: CacheConfig{
			Enabled:         true,
			MaxSize:         100 * 1024 * 1024, // 100MB
			TTL:             5 * time.Minute,
			CleanupInterval: 1 * time.Minute,
			EnableStats:     true,
			PreparedStatements: CacheItemConfig{
				Enabled: true,
				MaxSize: 10 * 1024 * 1024, // 10MB
				TTL:     1 * time.Hour,
			},
			QueryResults: CacheItemConfig{
				Enabled: true,
				MaxSize: 50 * 1024 * 1024, // 50MB
				TTL:     5 * time.Minute,
			},
		},
		SafeCopy: false,
	}
}
