// Package pool provides database connection pooling for DuckDB.
package pool

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/rs/zerolog"

	pkgerrors "github.com/TFMV/flight/pkg/errors"
)

// Config represents pool configuration.
type Config struct {
	DSN                string        `json:"dsn"`
	MaxOpenConnections int           `json:"max_open_connections"`
	MaxIdleConnections int           `json:"max_idle_connections"`
	ConnMaxLifetime    time.Duration `json:"conn_max_lifetime"`
	ConnMaxIdleTime    time.Duration `json:"conn_max_idle_time"`
	HealthCheckPeriod  time.Duration `json:"health_check_period"`
	ConnectionTimeout  time.Duration `json:"connection_timeout"`
}

// ConnectionPool manages database connections.
type ConnectionPool interface {
	// Get returns a database connection.
	Get(ctx context.Context) (*sql.DB, error)
	// Stats returns pool statistics.
	Stats() PoolStats
	// HealthCheck performs a health check on the pool.
	HealthCheck(ctx context.Context) error
	// Close closes the connection pool.
	Close() error
}

// PoolStats represents connection pool statistics.
type PoolStats struct {
	OpenConnections   int           `json:"open_connections"`
	InUse             int           `json:"in_use"`
	Idle              int           `json:"idle"`
	WaitCount         int64         `json:"wait_count"`
	WaitDuration      time.Duration `json:"wait_duration"`
	MaxIdleClosed     int64         `json:"max_idle_closed"`
	MaxLifetimeClosed int64         `json:"max_lifetime_closed"`
	LastHealthCheck   time.Time     `json:"last_health_check"`
	HealthCheckStatus string        `json:"health_check_status"`
}

type connectionPool struct {
	db     *sql.DB
	config Config
	logger zerolog.Logger

	// Use atomic.Bool for lock-free closed state
	closed atomic.Bool

	// Health check state with atomic operations
	lastHealthCheck atomic.Int64 // Unix timestamp
	healthStatus    atomic.Value // string

	// Context for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc

	// Metrics
	waitCount    atomic.Int64
	waitDuration atomic.Int64
}

// New creates a new connection pool.
func New(cfg Config, logger zerolog.Logger) (ConnectionPool, error) {
	if cfg.DSN == "" {
		cfg.DSN = ":memory:" // Default to in-memory database
	}

	// Set defaults
	if cfg.MaxOpenConnections <= 0 {
		cfg.MaxOpenConnections = 25
	}
	if cfg.MaxIdleConnections <= 0 {
		cfg.MaxIdleConnections = 5
	}
	if cfg.ConnMaxLifetime <= 0 {
		cfg.ConnMaxLifetime = 30 * time.Minute
	}
	if cfg.ConnMaxIdleTime <= 0 {
		cfg.ConnMaxIdleTime = 10 * time.Minute
	}
	if cfg.ConnectionTimeout <= 0 {
		cfg.ConnectionTimeout = 30 * time.Second
	}

	logger.Info().
		Str("dsn", maskDSN(cfg.DSN)).
		Int("max_open", cfg.MaxOpenConnections).
		Int("max_idle", cfg.MaxIdleConnections).
		Dur("conn_lifetime", cfg.ConnMaxLifetime).
		Dur("conn_idle_time", cfg.ConnMaxIdleTime).
		Msg("Creating DuckDB connection pool")

	db, err := sql.Open("duckdb", cfg.DSN)
	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.CodeInternal, "failed to open database")
	}

	// Configure connection pool
	db.SetMaxOpenConns(cfg.MaxOpenConnections)
	db.SetMaxIdleConns(cfg.MaxIdleConnections)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	ctx, cancel := context.WithCancel(context.Background())

	pool := &connectionPool{
		db:     db,
		config: cfg,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}

	// Initialize health status
	pool.healthStatus.Store("unknown")

	// Verify connection
	connCtx, connCancel := context.WithTimeout(context.Background(), cfg.ConnectionTimeout)
	defer connCancel()

	if err := pool.HealthCheck(connCtx); err != nil {
		db.Close()
		cancel()
		return nil, pkgerrors.Wrap(err, pkgerrors.CodeConnectionFailed, "initial health check failed")
	}

	// Start health check routine if configured
	if cfg.HealthCheckPeriod > 0 {
		go pool.healthCheckRoutine(ctx)
	}

	logger.Info().Msg("DuckDB connection pool created successfully")

	return pool, nil
}

// Get returns a database connection.
func (p *connectionPool) Get(ctx context.Context) (*sql.DB, error) {
	if p.closed.Load() {
		return nil, pkgerrors.New(pkgerrors.CodeUnavailable, "connection pool is closed")
	}

	// Track wait time
	start := time.Now()
	p.waitCount.Add(1)
	defer func() {
		p.waitDuration.Add(int64(time.Since(start)))
	}()

	// Verify connection is alive
	if err := p.db.PingContext(ctx); err != nil {
		p.logger.Error().Err(err).Msg("Database ping failed")
		return nil, pkgerrors.Wrap(err, pkgerrors.CodeConnectionFailed, "database connection failed")
	}

	return p.db, nil
}

// Stats returns pool statistics.
func (p *connectionPool) Stats() PoolStats {
	dbStats := p.db.Stats()

	return PoolStats{
		OpenConnections:   dbStats.OpenConnections,
		InUse:             dbStats.InUse,
		Idle:              dbStats.Idle,
		WaitCount:         p.waitCount.Load(),
		WaitDuration:      time.Duration(p.waitDuration.Load()),
		MaxIdleClosed:     dbStats.MaxIdleClosed,
		MaxLifetimeClosed: dbStats.MaxLifetimeClosed,
		LastHealthCheck:   time.Unix(p.lastHealthCheck.Load(), 0),
		HealthCheckStatus: p.getHealthStatus(),
	}
}

// HealthCheck performs a health check on the pool.
func (p *connectionPool) HealthCheck(ctx context.Context) error {
	if p.closed.Load() {
		return pkgerrors.New(pkgerrors.CodeUnavailable, "connection pool is closed")
	}

	// Test connection
	if err := p.db.PingContext(ctx); err != nil {
		p.updateHealthStatus("unhealthy", err.Error())
		return pkgerrors.Wrap(err, pkgerrors.CodeConnectionFailed, "health check ping failed")
	}

	// Test query execution
	var result int
	err := p.db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil || result != 1 {
		p.updateHealthStatus("unhealthy", "query test failed")
		return pkgerrors.Wrap(err, pkgerrors.CodeConnectionFailed, "health check query failed")
	}

	p.updateHealthStatus("healthy", "")
	return nil
}

// Close closes the connection pool.
func (p *connectionPool) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	p.logger.Info().Msg("Closing DuckDB connection pool")

	// Cancel the context to stop health check routine
	p.cancel()

	if err := p.db.Close(); err != nil {
		return pkgerrors.Wrap(err, pkgerrors.CodeInternal, "failed to close database")
	}

	return nil
}

// healthCheckRoutine performs periodic health checks until ctx is cancelled.
func (p *connectionPool) healthCheckRoutine(ctx context.Context) {
	ticker := time.NewTicker(p.config.HealthCheckPeriod)
	defer ticker.Stop()

	p.logger.Info().Dur("period", p.config.HealthCheckPeriod).Msg("Health check routine started")

	for {
		select {
		case <-ctx.Done():
			p.logger.Info().Msg("Health check routine stopped")
			return
		case <-ticker.C:
			// Create a per-probe timeout
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := p.HealthCheck(probeCtx); err != nil && !errors.Is(err, context.Canceled) {
				p.logger.Error().Err(err).Msg("Periodic health check failed")
			}
			cancel() // Immediate cleanup
		}
	}
}

// updateHealthStatus updates the health status using atomic operations.
func (p *connectionPool) updateHealthStatus(status, detail string) {
	p.lastHealthCheck.Store(time.Now().Unix())
	p.healthStatus.Store(status)

	if status == "unhealthy" && detail != "" {
		p.logger.Warn().
			Str("status", status).
			Str("detail", detail).
			Msg("Connection pool health status changed")
	}
}

// getHealthStatus safely retrieves the current health status.
func (p *connectionPool) getHealthStatus() string {
	if v := p.healthStatus.Load(); v != nil {
		return v.(string)
	}
	return "unknown"
}

// maskDSN masks sensitive information in DSN.
func maskDSN(dsn string) string {
	if dsn == ":memory:" || dsn == "" {
		return dsn
	}
	// Simple masking - in production, use more sophisticated parsing
	if len(dsn) > 10 {
		return dsn[:5] + "***" + dsn[len(dsn)-5:]
	}
	return "***"
}

// ConnectionWrapper wraps a database connection with additional functionality.
type ConnectionWrapper struct {
	db     *sql.DB
	pool   *connectionPool
	logger zerolog.Logger
}

// NewConnectionWrapper creates a new connection wrapper.
func NewConnectionWrapper(db *sql.DB, pool *connectionPool, logger zerolog.Logger) *ConnectionWrapper {
	return &ConnectionWrapper{
		db:     db,
		pool:   pool,
		logger: logger,
	}
}

// Execute executes a query with retry logic.
func (w *ConnectionWrapper) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	result, err := w.db.ExecContext(ctx, query, args...)

	w.logger.Debug().
		Dur("duration", time.Since(start)).
		Str("query", truncateQuery(query)).
		Bool("success", err == nil).
		Msg("Executed query")

	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.CodeQueryFailed, "query execution failed")
	}

	return result, nil
}

// Query executes a query and returns rows.
func (w *ConnectionWrapper) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := w.db.QueryContext(ctx, query, args...)

	w.logger.Debug().
		Dur("duration", time.Since(start)).
		Str("query", truncateQuery(query)).
		Bool("success", err == nil).
		Msg("Executed query")

	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.CodeQueryFailed, "query execution failed")
	}

	return rows, nil
}

// truncateQuery truncates long queries for logging.
func truncateQuery(query string) string {
	const maxLen = 100
	if len(query) <= maxLen {
		return query
	}
	return query[:maxLen] + "..."
}
