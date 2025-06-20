// Package clickhouse provides ClickHouse‑specific repository implementations.
package clickhouse

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	"github.com/TFMV/porter/pkg/errors"
	"github.com/TFMV/porter/pkg/infrastructure/converter"
	"github.com/TFMV/porter/pkg/infrastructure/pool"
	"github.com/TFMV/porter/pkg/models"
	"github.com/TFMV/porter/pkg/repositories"
)

//——————————————————————————————————————————
// Repository
//——————————————————————————————————————————

// preparedStatementRepository implements repositories.PreparedStatementRepository.
type preparedStatementRepository struct {
	pool       pool.ConnectionPool
	alloc      memory.Allocator
	log        zerolog.Logger
	statements sync.Map // map[string]*preparedStatement
}

func NewPreparedStatementRepository(p pool.ConnectionPool, a memory.Allocator, lg zerolog.Logger) repositories.PreparedStatementRepository {
	return &preparedStatementRepository{
		pool:  p,
		alloc: a,
		log:   lg.With().Str("repo", "clickhouse-prepared_stmt").Logger(),
	}
}

//——————————————————————————————————————————
// Public API
//——————————————————————————————————————————

func (r *preparedStatementRepository) Store(_ context.Context, ps *models.PreparedStatement) error {
	r.log.Debug().Str("handle", ps.Handle).Msg("store prepared stmt")
	r.statements.Store(ps.Handle, &preparedStatement{model: ps, createdAt: now()})
	return nil
}

func (r *preparedStatementRepository) Get(_ context.Context, h string) (*models.PreparedStatement, error) {
	ps, ok := r.loadPS(h)
	if !ok {
		return nil, errors.ErrStatementNotFound.WithDetail("handle", h)
	}
	return ps.model, nil
}

func (r *preparedStatementRepository) Remove(_ context.Context, h string) error {
	ps, ok := r.deletePS(h)
	if !ok {
		return errors.ErrStatementNotFound.WithDetail("handle", h)
	}
	if ps.stmt != nil {
		_ = ps.stmt.Close() // log only on failure
	}
	return nil
}

func (r *preparedStatementRepository) ExecuteQuery(ctx context.Context, h string, params [][]interface{}) (*models.QueryResult, error) {
	ps, ok := r.loadPS(h)
	if !ok {
		return nil, errors.ErrStatementNotFound.WithDetail("handle", h)
	}

	// acquire db connection & prepare stmt lazily
	var rows *sql.Rows
	if err := r.withConn(ctx, func(conn *sql.Conn) (err error) {
		if err = ps.ensurePrepared(ctx, conn); err != nil {
			return err
		}

		if batch := firstBatch(params); batch != nil {
			rows, err = ps.stmt.QueryContext(ctx, batch...)
		} else {
			rows, err = ps.stmt.QueryContext(ctx)
		}
		return
	}); err != nil {
		return nil, err
	}

	reader, err := converter.NewBatchReader(r.alloc, rows, r.log)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeInternal, "new batch reader")
	}

	out := make(chan arrow.Record, 8)
	go streamRecords(ctx, reader, out, r.log)

	ps.bumpUsage()
	return &models.QueryResult{Schema: reader.Schema(), Records: out}, nil
}

func (r *preparedStatementRepository) ExecuteUpdate(ctx context.Context, h string, params [][]interface{}) (*models.UpdateResult, error) {
	ps, ok := r.loadPS(h)
	if !ok {
		return nil, errors.ErrStatementNotFound.WithDetail("handle", h)
	}

	var (
		result sql.Result
		start  = time.Now()
	)

	err := r.withConn(ctx, func(conn *sql.Conn) error {
		if err := ps.ensurePrepared(ctx, conn); err != nil {
			return err
		}
		var execErr error
		if batch := firstBatch(params); batch != nil {
			result, execErr = ps.stmt.ExecContext(ctx, batch...)
		} else {
			result, execErr = ps.stmt.ExecContext(ctx)
		}
		return execErr
	})
	if err != nil {
		return nil, err
	}

	rows, _ := result.RowsAffected() // ignore error, ‑1 on unknown
	ps.bumpUsage()

	dur := time.Since(start)
	r.log.Debug().
		Str("handle", h).
		Int64("rows", rows).
		Dur("elapsed", dur).
		Msg("clickhouse update ok")

	return &models.UpdateResult{RowsAffected: rows, ExecutionTime: dur}, nil
}

func (r *preparedStatementRepository) List(_ context.Context, txID string) ([]*models.PreparedStatement, error) {
	var list []*models.PreparedStatement
	r.statements.Range(func(_, v any) bool {
		ps := v.(*preparedStatement)
		if txID == "" || ps.model.TransactionID == txID {
			list = append(list, ps.model)
		}
		return true
	})
	return list, nil
}

//——————————————————————————————————————————
// internal helpers
//——————————————————————————————————————————

func (r *preparedStatementRepository) withConn(ctx context.Context, fn func(*sql.Conn) error) error {
	db, err := r.pool.Get(ctx)
	if err != nil {
		return errors.Wrap(err, errors.CodeConnectionFailed, "get conn")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return errors.Wrap(err, errors.CodeConnectionFailed, "get conn")
	}
	return fn(conn)
}

func (r *preparedStatementRepository) loadPS(h string) (*preparedStatement, bool) {
	if v, ok := r.statements.Load(h); ok {
		return v.(*preparedStatement), true
	}
	return nil, false
}
func (r *preparedStatementRepository) deletePS(h string) (*preparedStatement, bool) {
	if v, ok := r.statements.LoadAndDelete(h); ok {
		return v.(*preparedStatement), true
	}
	return nil, false
}

func firstBatch(b [][]interface{}) []interface{} {
	if len(b) > 0 && len(b[0]) > 0 {
		return b[0]
	}
	return nil
}

// streamRecords copies Arrow records from reader to chan with proper Retain/Release.
func streamRecords(ctx context.Context, br *converter.BatchReader, out chan arrow.Record, log zerolog.Logger) {
	defer close(out)
	defer br.Release()

	var total int64
	for br.Next() {
		rec := br.Record()
		if rec == nil {
			continue
		}
		rec.Retain()
		total += rec.NumRows()

		select {
		case out <- rec:
			// consumer will Release()
		case <-ctx.Done():
			rec.Release()
			return
		}
	}
	if err := br.Err(); err != nil {
		log.Error().Err(err).Msg("batch reader")
	}
	log.Debug().Int64("rows", total).Msg("clickhouse stream complete")
}

// now is var for deterministic tests.
var now = time.Now

//——————————————————————————————————————————
// internal prepared stmt wrapper
//——————————————————————————————————————————

type preparedStatement struct {
	model     *models.PreparedStatement
	stmt      *sql.Stmt
	createdAt time.Time
	once      sync.Once
}

func (p *preparedStatement) ensurePrepared(ctx context.Context, conn *sql.Conn) error {
	var err error
	p.once.Do(func() {
		p.stmt, err = conn.PrepareContext(ctx, p.model.Query)
	})
	return err
}

func (p *preparedStatement) bumpUsage() {
	atomic.AddInt64(&p.model.ExecutionCount, 1)
	p.model.LastUsedAt = now()
}
