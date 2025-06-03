// Package duckdb provides DuckDB‑specific repository implementations.
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rs/zerolog"

	"github.com/TFMV/hatch/pkg/errors"
	"github.com/TFMV/hatch/pkg/infrastructure"
	"github.com/TFMV/hatch/pkg/infrastructure/pool"
	"github.com/TFMV/hatch/pkg/models"
	"github.com/TFMV/hatch/pkg/repositories"
)

// metadataRepository implements repositories.MetadataRepository for DuckDB.
type metadataRepository struct {
	pool    pool.ConnectionPool
	sqlInfo *infrastructure.SQLInfoProvider
	log     zerolog.Logger
}

// NewMetadataRepository constructs a DuckDB metadata repository.
func NewMetadataRepository(p pool.ConnectionPool, info *infrastructure.SQLInfoProvider, lg zerolog.Logger) repositories.MetadataRepository {
	return &metadataRepository{
		pool:    p,
		sqlInfo: info,
		log:     lg.With().Str("repo", "metadata").Logger(),
	}
}

//───────────────────────────────────
// public API
//───────────────────────────────────

func (r *metadataRepository) GetCatalogs(context.Context) ([]models.Catalog, error) {
	// DuckDB has a single in‑memory catalog.
	return []models.Catalog{{
		Name:        "memory",
		Description: "DuckDB in‑memory database",
	}}, nil
}

func (r *metadataRepository) GetSchemas(ctx context.Context, catalog, pattern string) ([]models.Schema, error) {
	const q = `
SELECT DISTINCT schema_name
FROM   information_schema.schemata
WHERE  ($1 = '' OR schema_name LIKE $1)
ORDER  BY schema_name`

	db, err := r.conn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, q, like(pattern))
	if err != nil {
		return nil, r.wrapDBErr(err, q)
	}
	defer rows.Close()

	return scanSchemas(rows, catalog)
}

func (r *metadataRepository) GetTables(ctx context.Context, opt models.GetTablesOptions) ([]models.Table, error) {
	var sb strings.Builder
	sb.WriteString(`
SELECT table_catalog, table_schema, table_name, table_type
FROM   information_schema.tables
WHERE  1=1`)
	args := make([]interface{}, 0, 4)

	if opt.SchemaFilterPattern != nil && !isWild(strPtr(opt.SchemaFilterPattern)) {
		sb.WriteString(" AND table_schema LIKE ?")
		args = append(args, likeDeref(opt.SchemaFilterPattern))
	}
	if !isWild(strPtr(opt.TableNameFilterPattern)) {
		sb.WriteString(" AND table_name LIKE ?")
		args = append(args, likeDeref(opt.TableNameFilterPattern))
	}
	if len(opt.TableTypes) > 0 {
		sb.WriteString(" AND table_type IN (")
		for i, t := range opt.TableTypes {
			if i > 0 {
				sb.WriteRune(',')
			}
			sb.WriteRune('?')
			args = append(args, t)
		}
		sb.WriteRune(')')
	}
	sb.WriteString(" ORDER BY table_schema, table_name")

	db, err := r.conn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, r.wrapDBErr(err, sb.String())
	}
	defer rows.Close()

	return scanTables(rows)
}

func (r *metadataRepository) GetTableTypes(context.Context) ([]string, error) {
	return []string{"BASE TABLE", "VIEW", "LOCAL TEMPORARY"}, nil
}

func (r *metadataRepository) GetColumns(ctx context.Context, ref models.TableRef) ([]models.Column, error) {
	var sb strings.Builder
	sb.WriteString(`
SELECT table_catalog, table_schema, table_name,
       column_name, ordinal_position, column_default,
       is_nullable, data_type,
       character_maximum_length, numeric_precision,
       numeric_scale, datetime_precision
FROM   information_schema.columns
WHERE  table_name = ?`)
	args := []interface{}{ref.Table}

	if s := strPtr(ref.DBSchema); s != "" {
		sb.WriteString(" AND table_schema = ?")
		args = append(args, s)
	}
	if c := strPtr(ref.Catalog); c != "" {
		sb.WriteString(" AND table_catalog = ?")
		args = append(args, c)
	}
	sb.WriteString(" ORDER BY ordinal_position")

	db, err := r.conn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, r.wrapDBErr(err, sb.String())
	}
	defer rows.Close()

	return scanColumns(rows)
}

// Foreign‑key helpers below remain unimplemented in DuckDB.
func (*metadataRepository) GetPrimaryKeys(context.Context, models.TableRef) ([]models.Key, error) {
	return nil, nil
}
func (*metadataRepository) GetImportedKeys(context.Context, models.TableRef) ([]models.ForeignKey, error) {
	return nil, nil
}
func (*metadataRepository) GetExportedKeys(context.Context, models.TableRef) ([]models.ForeignKey, error) {
	return nil, nil
}
func (*metadataRepository) GetCrossReference(context.Context, models.CrossTableRef) ([]models.ForeignKey, error) {
	return nil, nil
}

func (r *metadataRepository) GetTypeInfo(context.Context, *int32) ([]models.XdbcTypeInfo, error) {
	// TODO: XDBC provider integration.
	return nil, nil
}

func (r *metadataRepository) GetSQLInfo(ctx context.Context, ids []uint32) ([]models.SQLInfo, error) {
	return r.sqlInfo.GetSQLInfo(ids)
}

//───────────────────────────────────
// small helpers
//───────────────────────────────────

func (r *metadataRepository) conn(ctx context.Context) (*sql.Conn, error) {
	db, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "get conn")
	}
	return db.Conn(ctx)
}

func (r *metadataRepository) wrapDBErr(err error, sql string) error {
	return errors.Wrap(err, errors.CodeQueryFailed, fmt.Sprintf("duckdb query: %s", sql))
}

//───────────────────────────────────
// scanning helpers
//───────────────────────────────────

func scanSchemas(rows *sql.Rows, catalog string) ([]models.Schema, error) {
	var out []models.Schema
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, models.Schema{CatalogName: catalog, Name: name})
	}
	return out, rows.Err()
}

func scanTables(rows *sql.Rows) ([]models.Table, error) {
	var tbls []models.Table
	for rows.Next() {
		var t models.Table
		if err := rows.Scan(&t.CatalogName, &t.SchemaName, &t.Name, &t.Type); err != nil {
			return nil, err
		}
		tbls = append(tbls, t)
	}
	return tbls, rows.Err()
}

func scanColumns(rows *sql.Rows) ([]models.Column, error) {
	var cols []models.Column
	for rows.Next() {
		var c models.Column
		var nullable string
		if err := rows.Scan(
			&c.CatalogName, &c.SchemaName, &c.TableName,
			&c.Name, &c.OrdinalPosition, &c.DefaultValue,
			&nullable, &c.DataType, &c.CharMaxLength,
			&c.NumericPrecision, &c.NumericScale, &c.DateTimePrecision,
		); err != nil {
			return nil, err
		}
		c.IsNullable = nullable == "YES"
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

//───────────────────────────────────
// tiny util funcs
//───────────────────────────────────

func like(s string) string {
	if isWildPtr(&s) {
		return "%"
	}
	return s
}
func likeDeref(ptr *string) string { return like(strPtr(ptr)) }
func isWildPtr(p *string) bool     { return p == nil || isWild(*p) }
func isWild(s string) bool         { return s == "" || s == "%" }
func strPtr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
