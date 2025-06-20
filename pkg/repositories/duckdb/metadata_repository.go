// Package duckdb provides DuckDB‑specific repository implementations.
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rs/zerolog"

	"github.com/TFMV/porter/pkg/errors"
	"github.com/TFMV/porter/pkg/infrastructure"
	"github.com/TFMV/porter/pkg/infrastructure/converter"
	"github.com/TFMV/porter/pkg/infrastructure/pool"
	"github.com/TFMV/porter/pkg/models"
	"github.com/TFMV/porter/pkg/repositories"
	"github.com/apache/arrow-go/v18/arrow"
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
		Description: "DuckDB in-memory database",
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

func (r *metadataRepository) GetTableSchema(ctx context.Context, ref models.TableRef) (*arrow.Schema, error) {
	var sb strings.Builder
	sb.WriteString("SELECT * FROM ")
	if s := strPtr(ref.DBSchema); s != "" {
		sb.WriteString(quoteIdentifier(s))
		sb.WriteRune('.')
	}
	sb.WriteString(quoteIdentifier(ref.Table))
	sb.WriteString(" LIMIT 0")

	db, err := r.conn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, sb.String())
	if err != nil {
		return nil, r.wrapDBErr(err, sb.String())
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	tc := converter.New(r.log)
	schema, err := tc.ConvertToArrowSchema(cols)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// Foreign‑key helpers below remain unimplemented in DuckDB.
func (r *metadataRepository) GetPrimaryKeys(ctx context.Context, ref models.TableRef) ([]models.Key, error) {
	// TODO: query catalog tables once DuckDB exposes primary key metadata
	tbl := ref.Table
	if s := strPtr(ref.DBSchema); s != "" {
		tbl = fmt.Sprintf("%s.%s", s, tbl)
	}

	const q = `SELECT name, pk FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk`

	db, err := r.conn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, q, tbl)
	if err != nil {
		return nil, r.wrapDBErr(err, q)
	}
	defer rows.Close()

	out := make([]models.Key, 0)
	catalog := strPtr(ref.Catalog)
	schema := strPtr(ref.DBSchema)

	for rows.Next() {
		var name string
		var seq int32
		if err := rows.Scan(&name, &seq); err != nil {
			return nil, err
		}
		out = append(out, models.Key{
			CatalogName: catalog,
			SchemaName:  schema,
			TableName:   ref.Table,
			ColumnName:  name,
			KeySequence: seq,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
func (r *metadataRepository) GetImportedKeys(ctx context.Context, ref models.TableRef) ([]models.ForeignKey, error) {
	// DuckDB exposes basic foreign key metadata via PRAGMA foreign_key_list.
	// This only returns table-local information and does not include catalog
	// or constraint names. We map the available columns to the ForeignKey
	// model and leave catalog/schema information blank if not provided.

	tbl := ref.Table
	if s := strPtr(ref.DBSchema); s != "" {
		tbl = fmt.Sprintf("%s.%s", s, tbl)
	}

	const q = "PRAGMA foreign_key_list(?)"

	db, err := r.conn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, q, tbl)
	if err != nil {
		return nil, r.wrapDBErr(err, q)
	}
	defer rows.Close()

	var fks []models.ForeignKey
	fkCatalog := strPtr(ref.Catalog)
	fkSchema := strPtr(ref.DBSchema)

	for rows.Next() {
		// DuckDB returns: id, seq, table, from, to, on_update, on_delete, match
		var (
			id       int32
			seq      int32
			pkTable  string
			fkColumn string
			pkColumn string
			onUpdate string
			onDelete string
			match    string
		)
		if err := rows.Scan(&id, &seq, &pkTable, &fkColumn, &pkColumn, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}

		fks = append(fks, models.ForeignKey{
			PKCatalogName: fkCatalog, // DuckDB currently lacks catalog info
			PKSchemaName:  fkSchema,
			PKTableName:   pkTable,
			PKColumnName:  pkColumn,
			FKCatalogName: fkCatalog,
			FKSchemaName:  fkSchema,
			FKTableName:   ref.Table,
			FKColumnName:  fkColumn,
			KeySequence:   seq + 1, // PRAGMA seq starts at 0
			PKKeyName:     "",
			FKKeyName:     "",
			UpdateRule:    toFKRule(onUpdate),
			DeleteRule:    toFKRule(onDelete),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// TODO: Resolve catalog and constraint names once DuckDB exposes them
	return fks, nil
}
func (r *metadataRepository) GetExportedKeys(ctx context.Context, ref models.TableRef) ([]models.ForeignKey, error) {
	// DuckDB does not expose a direct way to query foreign keys referencing
	// a table. As a basic implementation, scan all tables in the schema and
	// inspect their foreign keys. This may be inefficient for large
	// databases but provides basic functionality until DuckDB adds native
	// support.

	db, err := r.conn(ctx)
	if err != nil {
		return nil, err
	}

	var sb strings.Builder
	sb.WriteString(`
SELECT table_schema, table_name
FROM   information_schema.tables
WHERE  table_type = 'BASE TABLE'`)
	args := make([]interface{}, 0, 2)

	if s := strPtr(ref.DBSchema); s != "" {
		sb.WriteString(" AND table_schema = ?")
		args = append(args, s)
	}
	if c := strPtr(ref.Catalog); c != "" {
		sb.WriteString(" AND table_catalog = ?")
		args = append(args, c)
	}

	rows, err := db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, r.wrapDBErr(err, sb.String())
	}
	defer rows.Close()

	pkCatalog := strPtr(ref.Catalog)
	pkSchema := strPtr(ref.DBSchema)

	var fks []models.ForeignKey
	for rows.Next() {
		var schema string
		var table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, err
		}

		tbl := table
		if schema != "" {
			tbl = fmt.Sprintf("%s.%s", schema, table)
		}

		fkRows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list(?)", tbl)
		if err != nil {
			return nil, r.wrapDBErr(err, "PRAGMA foreign_key_list")
		}

		for fkRows.Next() {
			var (
				id       int32
				seq      int32
				pkTable  string
				fkColumn string
				pkColumn string
				onUpdate string
				onDelete string
				match    string
			)
			if err := fkRows.Scan(&id, &seq, &pkTable, &fkColumn, &pkColumn, &onUpdate, &onDelete, &match); err != nil {
				fkRows.Close()
				return nil, err
			}

			if pkTable != ref.Table {
				continue
			}

			fks = append(fks, models.ForeignKey{
				PKCatalogName: pkCatalog,
				PKSchemaName:  pkSchema,
				PKTableName:   ref.Table,
				PKColumnName:  pkColumn,
				FKCatalogName: pkCatalog,
				FKSchemaName:  schema,
				FKTableName:   table,
				FKColumnName:  fkColumn,
				KeySequence:   seq + 1,
				PKKeyName:     "",
				FKKeyName:     "",
				UpdateRule:    toFKRule(onUpdate),
				DeleteRule:    toFKRule(onDelete),
			})
		}
		if err := fkRows.Err(); err != nil {
			fkRows.Close()
			return nil, err
		}
		fkRows.Close()
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// TODO: Resolve catalog and constraint names once DuckDB exposes them
	return fks, nil
}
func (r *metadataRepository) GetCrossReference(ctx context.Context, ref models.CrossTableRef) ([]models.ForeignKey, error) {
	// DuckDB does not expose a direct cross-reference query, so inspect the
	// foreign table's PRAGMA foreign_key_list and filter by the referenced
	// primary table. This mirrors the behaviour of GetExportedKeys but for a
	// specific FK table.

	tbl := ref.FKRef.Table
	if s := strPtr(ref.FKRef.DBSchema); s != "" {
		tbl = fmt.Sprintf("%s.%s", s, tbl)
	}

	const q = "PRAGMA foreign_key_list(?)"

	db, err := r.conn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, q, tbl)
	if err != nil {
		return nil, r.wrapDBErr(err, q)
	}
	defer rows.Close()

	pkCatalog := strPtr(ref.PKRef.Catalog)
	pkSchema := strPtr(ref.PKRef.DBSchema)
	fkCatalog := strPtr(ref.FKRef.Catalog)
	fkSchema := strPtr(ref.FKRef.DBSchema)

	var fks []models.ForeignKey
	for rows.Next() {
		var (
			id       int32
			seq      int32
			pkTable  string
			fkColumn string
			pkColumn string
			onUpdate string
			onDelete string
			match    string
		)
		if err := rows.Scan(&id, &seq, &pkTable, &fkColumn, &pkColumn, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}

		if pkTable != ref.PKRef.Table {
			continue
		}

		fks = append(fks, models.ForeignKey{
			PKCatalogName: pkCatalog,
			PKSchemaName:  pkSchema,
			PKTableName:   ref.PKRef.Table,
			PKColumnName:  pkColumn,
			FKCatalogName: fkCatalog,
			FKSchemaName:  fkSchema,
			FKTableName:   ref.FKRef.Table,
			FKColumnName:  fkColumn,
			KeySequence:   seq + 1,
			PKKeyName:     "",
			FKKeyName:     "",
			UpdateRule:    toFKRule(onUpdate),
			DeleteRule:    toFKRule(onDelete),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// TODO: Resolve catalog and constraint names once DuckDB exposes them
	return fks, nil
}

func (r *metadataRepository) GetTypeInfo(ctx context.Context, dataType *int32) ([]models.XdbcTypeInfo, error) {
	tc := converter.New(r.log)

	// Define common DuckDB types
	duckdbTypes := []string{
		"tinyint", "smallint", "integer", "bigint",
		"real", "float", "double", "decimal", "numeric",
		"boolean", "varchar", "text", "blob",
		"date", "time", "timestamp",
	}

	// If dataType is nil, return all types
	if dataType == nil {
		types := make([]models.XdbcTypeInfo, 0)
		for _, duckdbType := range duckdbTypes {
			sqlType := tc.GetSQLType(duckdbType)
			arrowType, err := tc.DuckDBToArrowType(duckdbType)
			if err != nil {
				continue
			}

			types = append(types, models.XdbcTypeInfo{
				TypeName:          duckdbType,
				DataType:          sqlType,
				ColumnSize:        sql.NullInt32{Int32: getColumnSize(arrowType), Valid: true},
				LiteralPrefix:     sql.NullString{String: getLiteralPrefix(arrowType), Valid: true},
				LiteralSuffix:     sql.NullString{String: getLiteralSuffix(arrowType), Valid: true},
				CreateParams:      sql.NullString{String: getCreateParams(arrowType), Valid: true},
				Nullable:          1, // SQL_NULLABLE
				CaseSensitive:     getCaseSensitive(arrowType),
				Searchable:        3, // SQL_SEARCHABLE
				UnsignedAttribute: sql.NullBool{Bool: false, Valid: true},
				FixedPrecScale:    getFixedPrecScale(arrowType),
				AutoIncrement:     sql.NullBool{Bool: false, Valid: true},
				LocalTypeName:     sql.NullString{String: duckdbType, Valid: true},
				MinimumScale:      sql.NullInt32{Int32: getMinimumScale(arrowType), Valid: true},
				MaximumScale:      sql.NullInt32{Int32: getMaximumScale(arrowType), Valid: true},
				SQLDataType:       sqlType,
				DatetimeSubcode:   sql.NullInt32{Int32: getSQLDateTimeSub(arrowType), Valid: true},
				NumPrecRadix:      sql.NullInt32{Int32: getNumPrecRadix(arrowType), Valid: true},
				IntervalPrecision: sql.NullInt32{Int32: 0, Valid: true},
			})
		}
		return types, nil
	}

	// Find types matching the specified dataType
	types := make([]models.XdbcTypeInfo, 0)
	for _, duckdbType := range duckdbTypes {
		sqlType := tc.GetSQLType(duckdbType)
		if sqlType == *dataType {
			arrowType, err := tc.DuckDBToArrowType(duckdbType)
			if err != nil {
				continue
			}

			types = append(types, models.XdbcTypeInfo{
				TypeName:          duckdbType,
				DataType:          sqlType,
				ColumnSize:        sql.NullInt32{Int32: getColumnSize(arrowType), Valid: true},
				LiteralPrefix:     sql.NullString{String: getLiteralPrefix(arrowType), Valid: true},
				LiteralSuffix:     sql.NullString{String: getLiteralSuffix(arrowType), Valid: true},
				CreateParams:      sql.NullString{String: getCreateParams(arrowType), Valid: true},
				Nullable:          1, // SQL_NULLABLE
				CaseSensitive:     getCaseSensitive(arrowType),
				Searchable:        3, // SQL_SEARCHABLE
				UnsignedAttribute: sql.NullBool{Bool: false, Valid: true},
				FixedPrecScale:    getFixedPrecScale(arrowType),
				AutoIncrement:     sql.NullBool{Bool: false, Valid: true},
				LocalTypeName:     sql.NullString{String: duckdbType, Valid: true},
				MinimumScale:      sql.NullInt32{Int32: getMinimumScale(arrowType), Valid: true},
				MaximumScale:      sql.NullInt32{Int32: getMaximumScale(arrowType), Valid: true},
				SQLDataType:       sqlType,
				DatetimeSubcode:   sql.NullInt32{Int32: getSQLDateTimeSub(arrowType), Valid: true},
				NumPrecRadix:      sql.NullInt32{Int32: getNumPrecRadix(arrowType), Valid: true},
				IntervalPrecision: sql.NullInt32{Int32: 0, Valid: true},
			})
		}
	}
	return types, nil
}

// Helper functions for XDBC type info
func getColumnSize(arrowType arrow.DataType) int32 {
	switch arrowType.ID() {
	case arrow.INT8, arrow.UINT8:
		return 3
	case arrow.INT16, arrow.UINT16:
		return 5
	case arrow.INT32, arrow.UINT32:
		return 10
	case arrow.INT64, arrow.UINT64:
		return 19
	case arrow.FLOAT32:
		return 7
	case arrow.FLOAT64:
		return 15
	case arrow.STRING:
		return 0 // Variable length
	case arrow.DECIMAL:
		decimalType := arrowType.(arrow.DecimalType)
		return decimalType.GetPrecision()
	default:
		return 0
	}
}

func getLiteralPrefix(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.STRING:
		return "'"
	case arrow.DATE32, arrow.DATE64:
		return "'"
	case arrow.TIMESTAMP:
		return "'"
	default:
		return ""
	}
}

func getLiteralSuffix(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.STRING:
		return "'"
	case arrow.DATE32, arrow.DATE64:
		return "'"
	case arrow.TIMESTAMP:
		return "'"
	default:
		return ""
	}
}

func getCreateParams(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.DECIMAL:
		return "precision,scale"
	case arrow.STRING:
		return "length"
	default:
		return ""
	}
}

func getCaseSensitive(arrowType arrow.DataType) bool {
	return arrowType.ID() == arrow.STRING
}

func getFixedPrecScale(arrowType arrow.DataType) bool {
	switch arrowType.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

func getMinimumScale(arrowType arrow.DataType) int32 {
	switch arrowType.ID() {
	case arrow.DECIMAL:
		return 0
	default:
		return 0
	}
}

func getMaximumScale(arrowType arrow.DataType) int32 {
	switch arrowType.ID() {
	case arrow.DECIMAL:
		decimalType := arrowType.(arrow.DecimalType)
		return decimalType.GetScale()
	default:
		return 0
	}
}

func getSQLDateTimeSub(arrowType arrow.DataType) int32 {
	switch arrowType.ID() {
	case arrow.DATE32, arrow.DATE64:
		return 1 // SQL_CODE_DATE
	case arrow.TIMESTAMP:
		return 2 // SQL_CODE_TIMESTAMP
	default:
		return 0
	}
}

func getNumPrecRadix(arrowType arrow.DataType) int32 {
	switch arrowType.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return 10
	case arrow.FLOAT32, arrow.FLOAT64:
		return 2
	default:
		return 0
	}
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

func quoteIdentifier(name string) string {
	return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\""))
}

func toFKRule(rule string) models.FKRule {
	switch strings.ToUpper(rule) {
	case "CASCADE":
		return models.FKRuleCascade
	case "RESTRICT":
		return models.FKRuleRestrict
	case "SET NULL":
		return models.FKRuleSetNull
	case "SET DEFAULT":
		return models.FKRuleSetDefault
	default:
		return models.FKRuleNoAction
	}
}
