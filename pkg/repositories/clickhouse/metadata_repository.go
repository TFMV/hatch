// Package clickhouse provides ClickHouse‑specific repository implementations.
package clickhouse

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

// metadataRepository implements repositories.MetadataRepository for ClickHouse.
type metadataRepository struct {
	pool    pool.ConnectionPool
	sqlInfo *infrastructure.SQLInfoProvider
	log     zerolog.Logger
}

// NewMetadataRepository constructs a ClickHouse metadata repository.
func NewMetadataRepository(p pool.ConnectionPool, info *infrastructure.SQLInfoProvider, lg zerolog.Logger) repositories.MetadataRepository {
	return &metadataRepository{
		pool:    p,
		sqlInfo: info,
		log:     lg.With().Str("repo", "clickhouse-metadata").Logger(),
	}
}

//───────────────────────────────────
// public API
//───────────────────────────────────

func (r *metadataRepository) GetCatalogs(context.Context) ([]models.Catalog, error) {
	// ClickHouse has databases that act as catalogs
	return []models.Catalog{{
		Name:        "default",
		Description: "ClickHouse default database",
	}}, nil
}

func (r *metadataRepository) GetSchemas(ctx context.Context, catalog, pattern string) ([]models.Schema, error) {
	const q = `
SELECT DISTINCT name as schema_name
FROM   system.databases
WHERE  ($1 = '' OR name LIKE $1)
ORDER  BY name`

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
SELECT database as table_catalog, database as table_schema, name as table_name, 
       CASE 
         WHEN engine LIKE '%View%' THEN 'VIEW'
         WHEN engine = 'Memory' THEN 'LOCAL TEMPORARY'
         ELSE 'BASE TABLE'
       END as table_type
FROM   system.tables
WHERE  1=1`)
	args := make([]interface{}, 0, 4)

	if opt.SchemaFilterPattern != nil && !isWild(strPtr(opt.SchemaFilterPattern)) {
		sb.WriteString(" AND database LIKE ?")
		args = append(args, likeDeref(opt.SchemaFilterPattern))
	}
	if !isWild(strPtr(opt.TableNameFilterPattern)) {
		sb.WriteString(" AND name LIKE ?")
		args = append(args, likeDeref(opt.TableNameFilterPattern))
	}
	if len(opt.TableTypes) > 0 {
		sb.WriteString(" AND (")
		for i, t := range opt.TableTypes {
			if i > 0 {
				sb.WriteString(" OR ")
			}
			switch t {
			case "VIEW":
				sb.WriteString("engine LIKE '%View%'")
			case "LOCAL TEMPORARY":
				sb.WriteString("engine = 'Memory'")
			default: // BASE TABLE
				sb.WriteString("engine NOT LIKE '%View%' AND engine != 'Memory'")
			}
		}
		sb.WriteString(")")
	}
	sb.WriteString(" ORDER BY database, name")

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
SELECT database as table_catalog, database as table_schema, table as table_name,
       name as column_name, position as ordinal_position, default_expression as column_default,
       CASE WHEN is_in_primary_key = 1 THEN 'NO' ELSE 'YES' END as is_nullable, 
       type as data_type,
       NULL as character_maximum_length, NULL as numeric_precision,
       NULL as numeric_scale, NULL as datetime_precision
FROM   system.columns
WHERE  table = ?`)
	args := []interface{}{ref.Table}

	if s := strPtr(ref.DBSchema); s != "" {
		sb.WriteString(" AND database = ?")
		args = append(args, s)
	}
	sb.WriteString(" ORDER BY position")

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

func (r *metadataRepository) GetPrimaryKeys(ctx context.Context, ref models.TableRef) ([]models.Key, error) {
	const q = `
SELECT database, table, name, position
FROM   system.columns
WHERE  table = ? AND is_in_primary_key = 1
ORDER  BY position`

	args := []interface{}{ref.Table}
	if s := strPtr(ref.DBSchema); s != "" {
		// ClickHouse doesn't have a separate schema concept, database is the schema
	}

	db, err := r.conn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, r.wrapDBErr(err, q)
	}
	defer rows.Close()

	out := make([]models.Key, 0)
	catalog := strPtr(ref.Catalog)
	schema := strPtr(ref.DBSchema)

	for rows.Next() {
		var database, table, name string
		var position int32
		if err := rows.Scan(&database, &table, &name, &position); err != nil {
			return nil, err
		}
		out = append(out, models.Key{
			CatalogName: catalog,
			SchemaName:  schema,
			TableName:   ref.Table,
			ColumnName:  name,
			KeySequence: position,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// ClickHouse doesn't have traditional foreign keys, so these return empty results
func (r *metadataRepository) GetImportedKeys(ctx context.Context, ref models.TableRef) ([]models.ForeignKey, error) {
	return []models.ForeignKey{}, nil
}

func (r *metadataRepository) GetExportedKeys(ctx context.Context, ref models.TableRef) ([]models.ForeignKey, error) {
	return []models.ForeignKey{}, nil
}

func (r *metadataRepository) GetCrossReference(ctx context.Context, ref models.CrossTableRef) ([]models.ForeignKey, error) {
	return []models.ForeignKey{}, nil
}

func (r *metadataRepository) GetTypeInfo(ctx context.Context, dataType *int32) ([]models.XdbcTypeInfo, error) {
	tc := converter.New(r.log)

	// Define common ClickHouse types
	clickhouseTypes := []string{
		"Int8", "Int16", "Int32", "Int64",
		"UInt8", "UInt16", "UInt32", "UInt64",
		"Float32", "Float64", "Decimal", "Decimal32", "Decimal64", "Decimal128",
		"Bool", "String", "FixedString",
		"Date", "Date32", "DateTime", "DateTime64",
		"Array", "Tuple", "Map", "Nullable",
	}

	// If dataType is nil, return all types
	if dataType == nil {
		types := make([]models.XdbcTypeInfo, 0)
		for _, chType := range clickhouseTypes {
			sqlType := tc.GetSQLType(chType)
			arrowType, err := tc.DuckDBToArrowType(chType)
			if err != nil {
				continue
			}

			types = append(types, models.XdbcTypeInfo{
				TypeName:          chType,
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
				LocalTypeName:     sql.NullString{String: chType, Valid: true},
				MinimumScale:      sql.NullInt32{Int32: getMinimumScale(arrowType), Valid: true},
				MaximumScale:      sql.NullInt32{Int32: getMaximumScale(arrowType), Valid: true},
				SQLDataType:       sqlType,
				DatetimeSubcode:   sql.NullInt32{Int32: getSQLDateTimeSub(arrowType), Valid: true},
				NumPrecRadix:      sql.NullInt32{Int32: getNumPrecRadix(arrowType), Valid: true},
				IntervalPrecision: sql.NullInt32{Valid: false},
			})
		}
		return types, nil
	}

	// Filter by specific data type
	for _, chType := range clickhouseTypes {
		sqlType := tc.GetSQLType(chType)
		if sqlType == *dataType {
			arrowType, err := tc.DuckDBToArrowType(chType)
			if err != nil {
				continue
			}

			return []models.XdbcTypeInfo{{
				TypeName:          chType,
				DataType:          sqlType,
				ColumnSize:        sql.NullInt32{Int32: getColumnSize(arrowType), Valid: true},
				LiteralPrefix:     sql.NullString{String: getLiteralPrefix(arrowType), Valid: true},
				LiteralSuffix:     sql.NullString{String: getLiteralSuffix(arrowType), Valid: true},
				CreateParams:      sql.NullString{String: getCreateParams(arrowType), Valid: true},
				Nullable:          1,
				CaseSensitive:     getCaseSensitive(arrowType),
				Searchable:        3,
				UnsignedAttribute: sql.NullBool{Bool: false, Valid: true},
				FixedPrecScale:    getFixedPrecScale(arrowType),
				AutoIncrement:     sql.NullBool{Bool: false, Valid: true},
				LocalTypeName:     sql.NullString{String: chType, Valid: true},
				MinimumScale:      sql.NullInt32{Int32: getMinimumScale(arrowType), Valid: true},
				MaximumScale:      sql.NullInt32{Int32: getMaximumScale(arrowType), Valid: true},
				SQLDataType:       sqlType,
				DatetimeSubcode:   sql.NullInt32{Int32: getSQLDateTimeSub(arrowType), Valid: true},
				NumPrecRadix:      sql.NullInt32{Int32: getNumPrecRadix(arrowType), Valid: true},
				IntervalPrecision: sql.NullInt32{Valid: false},
			}}, nil
		}
	}

	return []models.XdbcTypeInfo{}, nil
}

func (r *metadataRepository) GetSQLInfo(ctx context.Context, ids []uint32) ([]models.SQLInfo, error) {
	if r.sqlInfo == nil {
		return nil, errors.New(errors.CodeInternal, "SQL info provider not configured")
	}
	// Return empty for now - SQLInfo implementation would need to be added
	return []models.SQLInfo{}, nil
}

//───────────────────────────────────
// internal helpers
//───────────────────────────────────

func (r *metadataRepository) conn(ctx context.Context) (*sql.Conn, error) {
	db, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "get conn")
	}
	return db.Conn(ctx)
}

func (r *metadataRepository) wrapDBErr(err error, sql string) error {
	return errors.Wrap(err, errors.CodeQueryFailed, fmt.Sprintf("query: %s", truncateSQL(sql, 100)))
}

// Helper functions from DuckDB implementation adapted for ClickHouse
func scanSchemas(rows *sql.Rows, catalog string) ([]models.Schema, error) {
	var schemas []models.Schema
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		schemas = append(schemas, models.Schema{
			CatalogName: catalog,
			Name:        name,
		})
	}
	return schemas, rows.Err()
}

func scanTables(rows *sql.Rows) ([]models.Table, error) {
	var tables []models.Table
	for rows.Next() {
		var catalog, schema, name, tableType string
		if err := rows.Scan(&catalog, &schema, &name, &tableType); err != nil {
			return nil, err
		}
		tables = append(tables, models.Table{
			CatalogName: catalog,
			SchemaName:  schema,
			Name:        name,
			Type:        tableType,
		})
	}
	return tables, rows.Err()
}

func scanColumns(rows *sql.Rows) ([]models.Column, error) {
	var columns []models.Column
	for rows.Next() {
		var catalog, schema, table, name sql.NullString
		var position sql.NullInt32
		var defaultVal, nullable, dataType sql.NullString
		var charMaxLen, numPrec, numScale, dtPrec sql.NullInt32

		if err := rows.Scan(&catalog, &schema, &table, &name, &position, &defaultVal, &nullable, &dataType, &charMaxLen, &numPrec, &numScale, &dtPrec); err != nil {
			return nil, err
		}

		columns = append(columns, models.Column{
			CatalogName:       catalog.String,
			SchemaName:        schema.String,
			TableName:         table.String,
			Name:              name.String,
			OrdinalPosition:   int(position.Int32),
			DefaultValue:      defaultVal,
			IsNullable:        nullable.String == "YES",
			DataType:          dataType.String,
			CharMaxLength:     sql.NullInt64{Int64: int64(charMaxLen.Int32), Valid: charMaxLen.Valid},
			NumericPrecision:  sql.NullInt64{Int64: int64(numPrec.Int32), Valid: numPrec.Valid},
			NumericScale:      sql.NullInt64{Int64: int64(numScale.Int32), Valid: numScale.Valid},
			DateTimePrecision: sql.NullInt64{Int64: int64(dtPrec.Int32), Valid: dtPrec.Valid},
		})
	}
	return columns, rows.Err()
}

// Utility functions
func like(s string) string {
	if s == "" {
		return "%"
	}
	return s
}

func likeDeref(ptr *string) string { return like(strPtr(ptr)) }
func isWild(s string) bool         { return s == "" || s == "%" }
func strPtr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func truncateSQL(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// Arrow type helper functions - simplified versions
func getColumnSize(arrowType arrow.DataType) int32 {
	switch arrowType.ID() {
	case arrow.STRING, arrow.BINARY:
		return 65536 // Default size for variable-length types
	case arrow.INT8:
		return 3
	case arrow.INT16:
		return 5
	case arrow.INT32:
		return 10
	case arrow.INT64:
		return 19
	case arrow.FLOAT32:
		return 7
	case arrow.FLOAT64:
		return 15
	default:
		return 0
	}
}

func getLiteralPrefix(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.STRING:
		return "'"
	default:
		return ""
	}
}

func getLiteralSuffix(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.STRING:
		return "'"
	default:
		return ""
	}
}

func getCreateParams(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.DECIMAL128, arrow.DECIMAL256:
		return "precision,scale"
	default:
		return ""
	}
}

func getCaseSensitive(arrowType arrow.DataType) bool {
	return arrowType.ID() == arrow.STRING
}

func getFixedPrecScale(arrowType arrow.DataType) bool {
	switch arrowType.ID() {
	case arrow.DECIMAL128, arrow.DECIMAL256:
		return true
	default:
		return false
	}
}

func getMinimumScale(arrowType arrow.DataType) int32 {
	switch arrowType.ID() {
	case arrow.DECIMAL128, arrow.DECIMAL256:
		return 0
	default:
		return 0
	}
}

func getMaximumScale(arrowType arrow.DataType) int32 {
	switch arrowType.ID() {
	case arrow.DECIMAL128, arrow.DECIMAL256:
		return 38
	default:
		return 0
	}
}

func getSQLDateTimeSub(arrowType arrow.DataType) int32 {
	switch arrowType.ID() {
	case arrow.DATE32, arrow.DATE64:
		return 1 // SQL_CODE_DATE
	case arrow.TIME32, arrow.TIME64:
		return 2 // SQL_CODE_TIME
	case arrow.TIMESTAMP:
		return 3 // SQL_CODE_TIMESTAMP
	default:
		return 0
	}
}

func getNumPrecRadix(arrowType arrow.DataType) int32 {
	switch arrowType.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64,
		arrow.DECIMAL128, arrow.DECIMAL256:
		return 10
	default:
		return 0
	}
}
