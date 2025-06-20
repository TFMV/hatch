package clickhouse

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TFMV/porter/pkg/infrastructure/pool"
)

func TestQueryRepository_Basic(t *testing.T) {
	// Skip if no ClickHouse available
	t.Skip("ClickHouse integration test - requires running ClickHouse instance")

	logger := zerolog.New(zerolog.NewTestWriter(t))
	allocator := memory.NewGoAllocator()

	// TODO:This would need a real ClickHouse connection
	// For now, just test that the repository can be created
	config := pool.Config{
		DSN: "clickhouse://localhost:9000/default",
	}

	connectionPool, err := pool.New(config, logger)
	if err != nil {
		t.Skipf("Could not connect to ClickHouse: %v", err)
	}
	defer connectionPool.Close()

	repo := NewQueryRepository(connectionPool, allocator, logger)
	require.NotNil(t, repo)

	// Test basic query execution
	ctx := context.Background()
	result, err := repo.ExecuteQuery(ctx, "SELECT 1 as test_column", nil)
	if err != nil {
		t.Logf("ClickHouse query failed (expected if no server): %v", err)
		return
	}

	require.NotNil(t, result)
	assert.NotNil(t, result.Schema)
	assert.NotNil(t, result.Records)
}
