// Package duckdb provides DuckDB‑specific repository implementations.
package duckdb

import (
	"context"
	"database/sql"
	"regexp"
	"strconv"
	"strings"
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

//───────────────────────────────────
// Repository
//───────────────────────────────────

// queryRepository implements repositories.QueryRepository for DuckDB.
type queryRepository struct {
	pool  pool.ConnectionPool
	alloc memory.Allocator
	log   zerolog.Logger
}

func NewQueryRepository(p pool.ConnectionPool, a memory.Allocator, lg zerolog.Logger) repositories.QueryRepository {
	return &queryRepository{
		pool:  p,
		alloc: a,
		log:   lg.With().Str("repo", "query").Logger(),
	}
}

//───────────────────────────────────
// Public API
//───────────────────────────────────

func (r *queryRepository) ExecuteQuery(
	ctx context.Context,
	query string,
	txn repositories.Transaction,
	args ...interface{},
) (*models.QueryResult, error) {

	r.log.Debug().
		Str("sql", truncate(query, 120)).
		Bool("in_tx", txn != nil).
		Int("args", len(args)).
		Msg("execute query")

	qr, err := withQuerier(r, ctx, txn, func(q querier) (*models.QueryResult, error) {
		rows, err := q.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, errors.Wrap(err, errors.CodeQueryFailed, "query")
		}

		reader, err := converter.NewBatchReader(r.alloc, rows, r.log)
		if err != nil {
			return nil, errors.Wrap(err, errors.CodeInternal, "batch reader")
		}

		out := make(chan arrow.Record, 8)
		go streamQueryRecords(ctx, reader, out, r.log)

		return &models.QueryResult{
			Schema:  reader.Schema(),
			Records: out,
		}, nil
	})
	return qr, err
}

func (r *queryRepository) ExecuteUpdate(
	ctx context.Context,
	sqlStmt string,
	txn repositories.Transaction,
	args ...interface{},
) (*models.UpdateResult, error) {

	r.log.Debug().
		Str("sql", truncate(sqlStmt, 120)).
		Bool("in_tx", txn != nil).
		Int("args", len(args)).
		Msg("execute update")

	start := time.Now()

	ur, err := withQuerier(r, ctx, txn, func(q querier) (*models.UpdateResult, error) {
		res, err := q.ExecContext(ctx, sqlStmt, args...)
		if err != nil {
			return nil, errors.Wrap(err, errors.CodeQueryFailed, "exec")
		}
		rows, _ := res.RowsAffected() // ignore error, ‑1 on unknown
		return &models.UpdateResult{
			RowsAffected:  rows,
			ExecutionTime: time.Since(start),
		}, nil
	})

	if err == nil {
		r.log.Debug().
			Int64("rows", ur.RowsAffected).
			Dur("elapsed", ur.ExecutionTime).
			Msg("update ok")
	}
	return ur, err
}

// Explain returns an execution plan for the given query.
func (r *queryRepository) Explain(
	ctx context.Context,
	query string,
	txn repositories.Transaction,
) (*models.ExplainResult, error) {
	r.log.Debug().
		Str("sql", truncate(query, 120)).
		Bool("in_tx", txn != nil).
		Msg("explain")

	er, err := withQuerier(r, ctx, txn, func(q querier) (*models.ExplainResult, error) {
		rows, err := q.QueryContext(ctx, "EXPLAIN "+query)
		if err != nil {
			return nil, errors.Wrap(err, errors.CodeQueryFailed, "explain")
		}
		defer rows.Close()

		var lines []string
		for rows.Next() {
			var line string
			if scanErr := rows.Scan(&line); scanErr != nil {
				return nil, errors.Wrap(scanErr, errors.CodeInternal, "scan explain")
			}
			lines = append(lines, line)
		}

		res := parseDuckDBExplain(lines)
		return res, nil
	})
	return er, err
}

func (r *queryRepository) Prepare(
	ctx context.Context,
	query string,
	txn repositories.Transaction,
) (*sql.Stmt, error) {

	r.log.Debug().
		Str("sql", truncate(query, 120)).
		Bool("in_tx", txn != nil).
		Msg("prepare")

	stmt, err := withQuerier(r, ctx, txn, func(q querier) (*sql.Stmt, error) {
		ps, err := q.PrepareContext(ctx, query)
		return ps, errors.Wrap(err, errors.CodeQueryFailed, "prepare")
	})
	return stmt, err
}

//───────────────────────────────────
// Internal helpers
//───────────────────────────────────

// querier is the common subset implemented by *sql.Conn and *sql.Tx.
type querier interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}

// withQuerier executes a function with either a transaction or connection.
func withQuerier[T any](
	r *queryRepository,
	ctx context.Context,
	txn repositories.Transaction,
	fn func(querier) (T, error),
) (T, error) {
	// When returning early we need zero value of T
	var zero T

	if txn != nil {
		tx := txn.GetDBTx()
		if tx == nil {
			return zero, errors.New(errors.CodeTransactionFailed, "nil sql.Tx in Transaction")
		}
		return fn(tx)
	}

	conn, err := r.pool.Get(ctx)
	if err != nil {
		return zero, errors.Wrap(err, errors.CodeConnectionFailed, "get conn")
	}
	// Do not defer conn.Close(); rows/stmt hold the conn until closed.

	return fn(conn)
}

// streamQueryRecords sends Arrow records to channel with correct ref‑count handling.
func streamQueryRecords(ctx context.Context, br *converter.BatchReader, out chan arrow.Record, log zerolog.Logger) {
	defer close(out)
	defer br.Release()

	var rows int64
	for br.Next() {
		rec := br.Record()
		if rec == nil {
			continue
		}
		rec.Retain()
		rows += rec.NumRows()

		select {
		case out <- rec:
			// consumer is now owner
		case <-ctx.Done():
			rec.Release()
			return
		}
	}
	if err := br.Err(); err != nil {
		log.Error().Err(err).Msg("batch reader")
	}
	log.Debug().Int64("rows", rows).Msg("stream complete")
}

// truncate shortens long SQL strings for logs.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// parseDuckDBExplain converts raw EXPLAIN output lines into an ExplainResult.
func parseDuckDBExplain(lines []string) *models.ExplainResult {
	res := &models.ExplainResult{
		Backend:     "duckdb",
		CacheStatus: "cold",
	}
	if len(lines) == 0 {
		return res
	}
	res.PlanSummary = strings.Join(lines, "\n")
	for _, l := range lines {
		if m := rowsRegexp.FindStringSubmatch(l); len(m) == 2 {
			if v, err := strconv.Atoi(m[1]); err == nil {
				res.EstimatedRows = v
				break
			}
		}
	}
	if res.ExecutionMode == "" {
		res.ExecutionMode = "streamed"
	}
	return res
}

var rowsRegexp = regexp.MustCompile(`rows=([0-9]+)`) // best effort
