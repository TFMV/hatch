// Package duckdb provides DuckDB-specific repository implementations.
package duckdb

import (
	"context"
	"database/sql"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/TFMV/flight/pkg/errors"
	"github.com/TFMV/flight/pkg/infrastructure/pool"
	"github.com/TFMV/flight/pkg/models"
	"github.com/TFMV/flight/pkg/repositories"
)

// transactionRepository implements repositories.TransactionRepository for DuckDB.
type transactionRepository struct {
	pool         pool.ConnectionPool
	transactions sync.Map // map[string]*duckdbTransaction
	logger       zerolog.Logger
}

// NewTransactionRepository creates a new DuckDB transaction repository.
func NewTransactionRepository(pool pool.ConnectionPool, logger zerolog.Logger) repositories.TransactionRepository {
	return &transactionRepository{
		pool:   pool,
		logger: logger,
	}
}

// Begin starts a new transaction.
func (r *transactionRepository) Begin(ctx context.Context, opts models.TransactionOptions) (repositories.Transaction, error) {
	r.logger.Debug().
		Str("isolation_level", string(opts.IsolationLevel)).
		Bool("read_only", opts.ReadOnly).
		Msg("Beginning transaction")

	// Get connection from pool
	db, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get database connection")
	}

	// Start transaction with options
	txOpts := &sql.TxOptions{
		Isolation: r.mapIsolationLevel(opts.IsolationLevel),
		ReadOnly:  opts.ReadOnly,
	}

	tx, err := db.BeginTx(ctx, txOpts)
	if err != nil {
		r.logger.Error().Err(err).Msg("Failed to begin transaction")
		return nil, errors.Wrap(err, errors.CodeTransactionFailed, "failed to begin transaction")
	}

	// Create transaction wrapper
	txn := &duckdbTransaction{
		id:       uuid.New().String(),
		tx:       tx,
		active:   true,
		readOnly: opts.ReadOnly,
	}

	// Store in map
	r.transactions.Store(txn.id, txn)

	r.logger.Info().Str("transaction_id", txn.id).Msg("Transaction started")

	return txn, nil
}

// Get retrieves a transaction by ID.
func (r *transactionRepository) Get(ctx context.Context, id string) (repositories.Transaction, error) {
	r.logger.Debug().Str("transaction_id", id).Msg("Getting transaction")

	if val, ok := r.transactions.Load(id); ok {
		txn := val.(*duckdbTransaction)
		if txn.IsActive() {
			return txn, nil
		}
		// Remove inactive transaction
		r.transactions.Delete(id)
	}

	return nil, errors.ErrTransactionNotFound.WithDetail("transaction_id", id)
}

// List returns all active transactions.
func (r *transactionRepository) List(ctx context.Context) ([]repositories.Transaction, error) {
	r.logger.Debug().Msg("Listing transactions")

	var transactions []repositories.Transaction

	r.transactions.Range(func(key, value interface{}) bool {
		txn := value.(*duckdbTransaction)
		if txn.IsActive() {
			transactions = append(transactions, txn)
		} else {
			// Clean up inactive transaction
			r.transactions.Delete(key)
		}
		return true
	})

	return transactions, nil
}

// Remove removes a transaction from the repository.
func (r *transactionRepository) Remove(ctx context.Context, id string) error {
	r.logger.Debug().Str("transaction_id", id).Msg("Removing transaction")

	r.transactions.Delete(id)
	return nil
}

// mapIsolationLevel maps our isolation level to sql.IsolationLevel.
func (r *transactionRepository) mapIsolationLevel(level models.IsolationLevel) sql.IsolationLevel {
	switch level {
	case models.IsolationLevelReadUncommitted:
		return sql.LevelReadUncommitted
	case models.IsolationLevelReadCommitted:
		return sql.LevelReadCommitted
	case models.IsolationLevelRepeatableRead:
		return sql.LevelRepeatableRead
	case models.IsolationLevelSerializable:
		return sql.LevelSerializable
	default:
		return sql.LevelDefault
	}
}

// duckdbTransaction implements repositories.Transaction for DuckDB.
type duckdbTransaction struct {
	id       string
	tx       *sql.Tx
	active   bool
	readOnly bool
	mu       sync.RWMutex
}

// ID returns the transaction ID.
func (t *duckdbTransaction) ID() string {
	return t.id
}

// Commit commits the transaction.
func (t *duckdbTransaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return errors.New(errors.CodeTransactionFailed, "transaction is not active")
	}

	if err := t.tx.Commit(); err != nil {
		return errors.Wrap(err, errors.CodeTransactionFailed, "failed to commit transaction")
	}

	t.active = false
	return nil
}

// Rollback rolls back the transaction.
func (t *duckdbTransaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return errors.New(errors.CodeTransactionFailed, "transaction is not active")
	}

	if err := t.tx.Rollback(); err != nil {
		return errors.Wrap(err, errors.CodeTransactionFailed, "failed to rollback transaction")
	}

	t.active = false
	return nil
}

// IsActive returns true if the transaction is still active.
func (t *duckdbTransaction) IsActive() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.active
}

// GetDBTx returns the underlying database transaction.
func (t *duckdbTransaction) GetDBTx() *sql.Tx {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.active {
		return nil
	}
	return t.tx
}
