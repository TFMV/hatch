package services

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TFMV/porter/pkg/errors"
	"github.com/TFMV/porter/pkg/models"
	"github.com/TFMV/porter/pkg/repositories"
)

// mockTransactionRepo implements repositories.TransactionRepository
type mockTransactionRepo struct {
	beginFunc    func(ctx context.Context, opts models.TransactionOptions) (repositories.Transaction, error)
	getFunc      func(ctx context.Context, id string) (repositories.Transaction, error)
	commitFunc   func(ctx context.Context, id string) error
	rollbackFunc func(ctx context.Context, id string) error
	listFunc     func(ctx context.Context) ([]repositories.Transaction, error)
	removeFunc   func(ctx context.Context, id string) error
}

func (m *mockTransactionRepo) Begin(ctx context.Context, opts models.TransactionOptions) (repositories.Transaction, error) {
	return m.beginFunc(ctx, opts)
}

func (m *mockTransactionRepo) Get(ctx context.Context, id string) (repositories.Transaction, error) {
	return m.getFunc(ctx, id)
}

func (m *mockTransactionRepo) Commit(ctx context.Context, id string) error {
	return m.commitFunc(ctx, id)
}

func (m *mockTransactionRepo) Rollback(ctx context.Context, id string) error {
	return m.rollbackFunc(ctx, id)
}

func (m *mockTransactionRepo) List(ctx context.Context) ([]repositories.Transaction, error) {
	return m.listFunc(ctx)
}

func (m *mockTransactionRepo) Remove(ctx context.Context, id string) error {
	return m.removeFunc(ctx, id)
}

// mockTransaction implements repositories.Transaction
type mockTransaction struct {
	id       string
	active   bool
	commit   func(ctx context.Context) error
	rollback func(ctx context.Context) error
	dbTx     *sql.Tx
}

func (m *mockTransaction) ID() string {
	return m.id
}

func (m *mockTransaction) IsActive() bool {
	return m.active
}

func (m *mockTransaction) Commit(ctx context.Context) error {
	return m.commit(ctx)
}

func (m *mockTransaction) Rollback(ctx context.Context) error {
	return m.rollback(ctx)
}

func (m *mockTransaction) GetDBTx() *sql.Tx {
	return m.dbTx
}

// mockLogger implements Logger
type mockLogger struct {
	debugFunc func(msg string, keysAndValues ...interface{})
	infoFunc  func(msg string, keysAndValues ...interface{})
	warnFunc  func(msg string, keysAndValues ...interface{})
	errorFunc func(msg string, keysAndValues ...interface{})
}

func (m *mockLogger) Debug(msg string, keysAndValues ...interface{}) {
	if m.debugFunc != nil {
		m.debugFunc(msg, keysAndValues...)
	}
}

func (m *mockLogger) Info(msg string, keysAndValues ...interface{}) {
	if m.infoFunc != nil {
		m.infoFunc(msg, keysAndValues...)
	}
}

func (m *mockLogger) Warn(msg string, keysAndValues ...interface{}) {
	if m.warnFunc != nil {
		m.warnFunc(msg, keysAndValues...)
	}
}

func (m *mockLogger) Error(msg string, keysAndValues ...interface{}) {
	if m.errorFunc != nil {
		m.errorFunc(msg, keysAndValues...)
	}
}

// mockMetricsCollector implements MetricsCollector
type mockMetricsCollector struct {
	incrementCounterFunc func(name string, labels ...string)
	recordHistogramFunc  func(name string, value float64, labels ...string)
	recordGaugeFunc      func(name string, value float64, labels ...string)
	startTimerFunc       func(name string) Timer
}

func (m *mockMetricsCollector) IncrementCounter(name string, labels ...string) {
	if m.incrementCounterFunc != nil {
		m.incrementCounterFunc(name, labels...)
	}
}

func (m *mockMetricsCollector) RecordHistogram(name string, value float64, labels ...string) {
	if m.recordHistogramFunc != nil {
		m.recordHistogramFunc(name, value, labels...)
	}
}

func (m *mockMetricsCollector) RecordGauge(name string, value float64, labels ...string) {
	if m.recordGaugeFunc != nil {
		m.recordGaugeFunc(name, value, labels...)
	}
}

func (m *mockMetricsCollector) StartTimer(name string) Timer {
	if m.startTimerFunc != nil {
		return m.startTimerFunc(name)
	}
	return &mockTimer{}
}

// mockTimer implements Timer
type mockTimer struct{}

func (m *mockTimer) Stop() time.Duration {
	return 0
}

func setupTestTransactionService() (TransactionService, *mockTransactionRepo, *mockLogger, *mockMetricsCollector) {
	repo := &mockTransactionRepo{}
	logger := &mockLogger{}
	metrics := &mockMetricsCollector{}
	service := NewTransactionService(repo, 30*time.Minute, logger, metrics)
	return service, repo, logger, metrics
}

func TestTransactionService_Begin(t *testing.T) {
	service, repo, _, _ := setupTestTransactionService()

	t.Run("successful begin", func(t *testing.T) {
		// Setup mock response
		repo.beginFunc = func(ctx context.Context, opts models.TransactionOptions) (repositories.Transaction, error) {
			return &mockTransaction{
				id:     "txn123",
				active: true,
			}, nil
		}

		// Test
		opts := models.TransactionOptions{
			IsolationLevel: "READ COMMITTED",
			ReadOnly:       false,
		}
		txnID, err := service.Begin(context.Background(), opts)
		require.NoError(t, err)
		assert.Equal(t, "txn123", txnID)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.beginFunc = func(ctx context.Context, opts models.TransactionOptions) (repositories.Transaction, error) {
			return nil, assert.AnError
		}

		// Test
		opts := models.TransactionOptions{
			IsolationLevel: models.IsolationLevelReadCommitted,
			ReadOnly:       false,
		}
		txnID, err := service.Begin(context.Background(), opts)
		assert.Error(t, err)
		assert.Empty(t, txnID)
	})
}

func TestTransactionService_Commit(t *testing.T) {
	service, repo, _, _ := setupTestTransactionService()

	t.Run("successful commit", func(t *testing.T) {
		// Setup mock response
		repo.getFunc = func(ctx context.Context, id string) (repositories.Transaction, error) {
			return &mockTransaction{
				id:     "txn123",
				active: true,
				commit: func(ctx context.Context) error {
					return nil
				},
			}, nil
		}
		repo.commitFunc = func(ctx context.Context, id string) error {
			return nil
		}
		repo.removeFunc = func(ctx context.Context, id string) error {
			return nil
		}

		// Test
		err := service.Commit(context.Background(), "txn123")
		require.NoError(t, err)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getFunc = func(ctx context.Context, id string) (repositories.Transaction, error) {
			return nil, assert.AnError
		}

		// Test
		err := service.Commit(context.Background(), "txn123")
		assert.Error(t, err)
	})
}

func TestTransactionService_Rollback(t *testing.T) {
	service, repo, _, _ := setupTestTransactionService()

	t.Run("successful rollback", func(t *testing.T) {
		// Setup mock response
		repo.getFunc = func(ctx context.Context, id string) (repositories.Transaction, error) {
			return &mockTransaction{
				id:     "txn123",
				active: true,
				rollback: func(ctx context.Context) error {
					return nil
				},
			}, nil
		}
		repo.rollbackFunc = func(ctx context.Context, id string) error {
			return nil
		}
		repo.removeFunc = func(ctx context.Context, id string) error {
			return nil
		}

		// Test
		err := service.Rollback(context.Background(), "txn123")
		require.NoError(t, err)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getFunc = func(ctx context.Context, id string) (repositories.Transaction, error) {
			return nil, assert.AnError
		}

		// Test
		err := service.Rollback(context.Background(), "txn123")
		assert.Error(t, err)
	})
}

func TestTransactionService_Get(t *testing.T) {
	service, repo, _, _ := setupTestTransactionService()

	t.Run("successful get", func(t *testing.T) {
		// Setup mock response
		expectedTxn := &mockTransaction{
			id:     "txn123",
			active: true,
		}
		repo.getFunc = func(ctx context.Context, id string) (repositories.Transaction, error) {
			return expectedTxn, nil
		}

		// Test
		txn, err := service.Get(context.Background(), "txn123")
		require.NoError(t, err)
		assert.Equal(t, expectedTxn, txn)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getFunc = func(ctx context.Context, id string) (repositories.Transaction, error) {
			return nil, errors.ErrTransactionNotFound.WithDetail("transaction_id", id)
		}

		// Clear any active transactions
		service.(*transactionService).activeTransactions = &sync.Map{}

		// Test
		txn, err := service.Get(context.Background(), "txn123")
		assert.Error(t, err)
		assert.Nil(t, txn)
		assert.True(t, errors.IsNotFound(err))
	})
}

func TestTransactionService_List(t *testing.T) {
	service, repo, _, _ := setupTestTransactionService()

	t.Run("successful list", func(t *testing.T) {
		// Setup mock response
		expectedTxns := []repositories.Transaction{
			&mockTransaction{
				id:     "txn123",
				active: true,
			},
		}
		repo.listFunc = func(ctx context.Context) ([]repositories.Transaction, error) {
			return expectedTxns, nil
		}

		// Test
		txns, err := service.List(context.Background())
		require.NoError(t, err)
		assert.Equal(t, expectedTxns, txns)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.listFunc = func(ctx context.Context) ([]repositories.Transaction, error) {
			return nil, assert.AnError
		}

		// Test
		txns, err := service.List(context.Background())
		assert.Error(t, err)
		assert.Nil(t, txns)
	})
}
