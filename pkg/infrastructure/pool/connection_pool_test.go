package pool

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))

	tests := []struct {
		name        string
		config      Config
		wantErr     bool
		errContains string
	}{
		{
			name: "default config",
			config: Config{
				DSN: ":memory:",
			},
		},
		{
			name: "custom config",
			config: Config{
				DSN:                ":memory:",
				MaxOpenConnections: 10,
				MaxIdleConnections: 5,
				ConnMaxLifetime:    time.Hour,
				ConnMaxIdleTime:    30 * time.Minute,
				HealthCheckPeriod:  time.Minute,
				ConnectionTimeout:  10 * time.Second,
			},
		},
		{
			name: "invalid DSN",
			config: Config{
				DSN: "invalid://dsn",
			},
			wantErr:     true,
			errContains: "failed to open database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := New(tt.config, logger)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pool)

			// Clean up
			err = pool.Close()
			require.NoError(t, err)
		})
	}
}

func TestConnectionPool_Get(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	pool, err := New(Config{DSN: ":memory:"}, logger)
	require.NoError(t, err)
	defer pool.Close()

	t.Run("get connection", func(t *testing.T) {
		ctx := context.Background()
		db, err := pool.Get(ctx)
		require.NoError(t, err)
		require.NotNil(t, db)

		// Test the connection works
		var result int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, 1, result)
	})

	t.Run("get after close", func(t *testing.T) {
		err := pool.Close()
		require.NoError(t, err)

		_, err = pool.Get(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "connection pool is closed")
	})
}

func TestConnectionPool_Stats(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	pool, err := New(Config{DSN: ":memory:"}, logger)
	require.NoError(t, err)
	defer pool.Close()

	// Get initial stats
	stats := pool.Stats()
	assert.Equal(t, 0, stats.InUse)
	assert.Equal(t, int64(0), stats.WaitCount)

	// Get a connection and check stats
	db, err := pool.Get(context.Background())
	require.NoError(t, err)
	require.NotNil(t, db)

	stats = pool.Stats()
	assert.GreaterOrEqual(t, stats.OpenConnections, 1)
	assert.Equal(t, int64(1), stats.WaitCount)
	assert.NotEmpty(t, stats.HealthCheckStatus)
}

func TestConnectionPool_HealthCheck(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	pool, err := New(Config{
		DSN:               ":memory:",
		HealthCheckPeriod: time.Second,
	}, logger)
	require.NoError(t, err)
	defer pool.Close()

	t.Run("manual health check", func(t *testing.T) {
		err := pool.HealthCheck(context.Background())
		require.NoError(t, err)

		stats := pool.Stats()
		assert.Equal(t, "healthy", stats.HealthCheckStatus)
	})

	t.Run("health check after close", func(t *testing.T) {
		err := pool.Close()
		require.NoError(t, err)

		err = pool.HealthCheck(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "connection pool is closed")
	})
}

func TestConnectionPool_Close(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	pool, err := New(Config{DSN: ":memory:"}, logger)
	require.NoError(t, err)

	// First close should succeed
	err = pool.Close()
	require.NoError(t, err)

	// Second close should be no-op
	err = pool.Close()
	require.NoError(t, err)

	// Operations after close should fail
	_, err = pool.Get(context.Background())
	require.Error(t, err)
}

func TestConnectionWrapper(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	pool, err := New(Config{DSN: ":memory:"}, logger)
	require.NoError(t, err)
	defer pool.Close()

	db, err := pool.Get(context.Background())
	require.NoError(t, err)

	wrapper := NewConnectionWrapper(db, pool.(*connectionPool), logger)
	require.NotNil(t, wrapper)

	t.Run("execute", func(t *testing.T) {
		ctx := context.Background()
		result, err := wrapper.Execute(ctx, "CREATE TABLE test (id INT)")
		require.NoError(t, err)
		require.NotNil(t, result)

		result, err = wrapper.Execute(ctx, "INSERT INTO test VALUES (?)", 42)
		require.NoError(t, err)
		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)
	})

	t.Run("query", func(t *testing.T) {
		ctx := context.Background()
		rows, err := wrapper.Query(ctx, "SELECT id FROM test WHERE id = ?", 42)
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var id int
		err = rows.Scan(&id)
		require.NoError(t, err)
		assert.Equal(t, 42, id)
	})
}

func TestMaskDSN(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "memory dsn",
			dsn:  ":memory:",
			want: ":memory:",
		},
		{
			name: "file dsn",
			dsn:  "test.db",
			want: "***",
		},
		{
			name: "url with password",
			dsn:  "duckdb://user:password@localhost:3306/db",
			want: "duckdb://user:%2A%2A%2A%2A%2A@localhost:3306/db",
		},
		{
			name: "url with query params",
			dsn:  "duckdb://localhost/db?password=secret&timeout=30",
			want: "duckdb://localhost/db?password=%2A%2A%2A%2A%2A&timeout=30",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskDSN(tt.dsn)
			assert.Equal(t, tt.want, got)
		})
	}
}
