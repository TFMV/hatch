// Package duckdb provides DuckDB‑specific repository implementations.
package duckdb

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/TFMV/hatch/pkg/errors"
	"github.com/TFMV/hatch/pkg/infrastructure/pool"
	"github.com/TFMV/hatch/pkg/models"
	"github.com/TFMV/hatch/pkg/repositories"
)

//───────────────────────────────────
// Repository
//───────────────────────────────────

// transactionRepository implements repositories.TransactionRepository.
type transactionRepository struct {
	pool  pool.ConnectionPool
	log   zerolog.Logger
	txMap sync.Map // key = id, val = *duckdbTransaction
}

func NewTransactionRepository(p pool.ConnectionPool, lg zerolog.Logger) repositories.TransactionRepository {
	return &transactionRepository{
		pool: p,
		log:  lg.With().Str("repo", "txn").Logger(),
	}
}

// Begin starts a new SQL transaction.
func (r *transactionRepository) Begin(ctx context.Context, opt models.TransactionOptions) (repositories.Transaction, error) {
	r.log.Debug().
		Str("iso", string(opt.IsolationLevel)).
		Bool("ro", opt.ReadOnly).
		Msg("begin")

	conn, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "get conn")
	}

	sqlTx, err := conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: mapIsolation(opt.IsolationLevel),
		ReadOnly:  opt.ReadOnly,
	})
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeTransactionFailed, "begin tx")
	}

	sqlConn, err := conn.Conn(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "get conn")
	}

	txn := &duckdbTransaction{
		id:       uuid.NewString(),
		sqlTx:    sqlTx,
		conn:     sqlConn,
		readOnly: opt.ReadOnly,
		started:  time.Now(),
	}
	r.store(txn)

	r.log.Info().Str("id", txn.id).Msg("txn started")
	return txn, nil
}

// Get returns an active transaction or ErrTransactionNotFound.
func (r *transactionRepository) Get(_ context.Context, id string) (repositories.Transaction, error) {
	if txn, ok := r.load(id); ok && txn.IsActive() {
		return txn, nil
	}
	return nil, errors.ErrTransactionNotFound.WithDetail("transaction_id", id)
}

// List returns all active transactions.
func (r *transactionRepository) List(_ context.Context) ([]repositories.Transaction, error) {
	var list []repositories.Transaction
	r.txMap.Range(func(_, v any) bool {
		t := v.(*duckdbTransaction)
		if t.IsActive() {
			list = append(list, t)
		} else {
			r.delete(t.id)
		}
		return true
	})
	return list, nil
}

// Remove deletes a transaction record (no commit/rollback).
func (r *transactionRepository) Remove(_ context.Context, id string) error {
	r.delete(id)
	return nil
}

//───────────────────────────────────
// internal map helpers
//───────────────────────────────────

func (r *transactionRepository) store(t *duckdbTransaction) { r.txMap.Store(t.id, t) }
func (r *transactionRepository) load(id string) (*duckdbTransaction, bool) {
	v, ok := r.txMap.Load(id)
	if !ok {
		return nil, false
	}
	return v.(*duckdbTransaction), true
}
func (r *transactionRepository) delete(id string) { r.txMap.Delete(id) }

//───────────────────────────────────
// isolation mapping
//───────────────────────────────────

func mapIsolation(lvl models.IsolationLevel) sql.IsolationLevel {
	switch lvl {
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

//───────────────────────────────────
// duckdbTransaction
//───────────────────────────────────

type duckdbTransaction struct {
	id       string
	sqlTx    *sql.Tx
	conn     *sql.Conn // returned to pool on Commit/Rollback
	readOnly bool

	started time.Time
	active  atomic.Bool
}

func (t *duckdbTransaction) ID() string { return t.id }

func (t *duckdbTransaction) Commit(ctx context.Context) error {
	if !t.setInactive() {
		return errors.New(errors.CodeTransactionFailed, "transaction not active")
	}
	if err := t.sqlTx.Commit(); err != nil {
		return errors.Wrap(err, errors.CodeTransactionFailed, "commit")
	}
	_ = t.conn.Close()
	return nil
}

func (t *duckdbTransaction) Rollback(ctx context.Context) error {
	if !t.setInactive() {
		return errors.New(errors.CodeTransactionFailed, "transaction not active")
	}
	if err := t.sqlTx.Rollback(); err != nil {
		return errors.Wrap(err, errors.CodeTransactionFailed, "rollback")
	}
	_ = t.conn.Close()
	return nil
}

func (t *duckdbTransaction) IsActive() bool { return t.active.Load() }

func (t *duckdbTransaction) GetDBTx() *sql.Tx {
	if t.IsActive() {
		return t.sqlTx
	}
	return nil
}

// setInactive atomically flips the active flag; returns true if it was active.
func (t *duckdbTransaction) setInactive() bool { return t.active.Swap(false) }

// ensure active on creation
func init() {
	var dummy duckdbTransaction
	dummy.active.Store(true)
}
