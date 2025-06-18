// Package infrastructure provides Flight SQL capability and XDBC metadata
// services for Hatch.
package infrastructure

import (
	"database/sql"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/TFMV/hatch/pkg/models"
)

//───────────────────────────────────
// SQL‑INFO PROVIDER
//───────────────────────────────────

// SQLInfoProvider exposes DuckDB/Hatch SQL capabilities.
type SQLInfoProvider struct {
	once     sync.Once
	alloc    memory.Allocator
	info     map[uint32]interface{}
	infoCopy []models.SQLInfo // cached slice for GetSQLInfo(nil)
}

// NewSQLInfoProvider returns a ready‑to‑use provider.
func NewSQLInfoProvider(alloc memory.Allocator) *SQLInfoProvider {
	return &SQLInfoProvider{alloc: alloc}
}

func (p *SQLInfoProvider) ensureInit() {
	p.once.Do(func() {
		p.info = make(map[uint32]interface{}, 32)

		add := func(k flightsql.SqlInfo, v interface{}) {
			p.info[uint32(k)] = v
		}

		// Flight SQL identification
		add(flightsql.SqlInfoFlightSqlServerName, "Hatch")
		add(flightsql.SqlInfoFlightSqlServerVersion, "2.0.0")
		add(flightsql.SqlInfoFlightSqlServerArrowVersion, "18.0.0")
		add(flightsql.SqlInfoFlightSqlServerReadOnly, false)
		add(flightsql.SqlInfoFlightSqlServerTransaction, int32(flightsql.SqlTransactionTransaction))

		// Language support
		add(flightsql.SqlInfoDDLCatalog, false)
		add(flightsql.SqlInfoDDLSchema, true)
		add(flightsql.SqlInfoDDLTable, true)

		// Identifier behaviour
		add(flightsql.SqlInfoIdentifierCase, int64(flightsql.SqlCaseSensitivityCaseInsensitive))
		add(flightsql.SqlInfoIdentifierQuoteChar, `"`)
		add(flightsql.SqlInfoQuotedIdentifierCase, int64(flightsql.SqlCaseSensitivityCaseInsensitive))

		// Sorting / NULL ordering
		add(flightsql.SqlInfoNullOrdering, int64(flightsql.SqlNullOrderingSortAtStart))

		add(flightsql.SqlInfoAllTablesAreSelectable, true)
		add(flightsql.SqlInfoTransactionsSupported, true)

		// Keywords / functions (pre‑allocated capacity improves append perf)
		add(flightsql.SqlInfoKeywords, duckdbKeywords)
		add(flightsql.SqlInfoNumericFunctions, duckdbNumericFns)
		add(flightsql.SqlInfoStringFunctions, duckdbStringFns)

		add(flightsql.SqlInfoSupportsConvert, map[int32][]int32{
			int32(flightsql.SqlConvertBigInt): {int32(flightsql.SqlConvertInteger)},
		})

		// Cache full copy
		p.infoCopy = make([]models.SQLInfo, 0, len(p.info))
		for k, v := range p.info {
			p.infoCopy = append(p.infoCopy, models.SQLInfo{InfoName: k, Value: v})
		}
	})
}

// GetSQLInfo implements the Flight SQL “GetSqlInfo” RPC semantics.
func (p *SQLInfoProvider) GetSQLInfo(ids []uint32) ([]models.SQLInfo, error) {
	p.ensureInit()

	// “0 ids” means “all”.
	if len(ids) == 0 {
		return append([]models.SQLInfo(nil), p.infoCopy...), nil // shallow copy
	}

	out := make([]models.SQLInfo, 0, len(ids))
	for _, id := range ids {
		if v, ok := p.info[id]; ok {
			out = append(out, models.SQLInfo{InfoName: id, Value: v})
		}
	}
	return out, nil
}

// GetSQLInfoResultMap returns the provider’s internal map in Flight format.
// Map is copied to avoid caller mutation.
func (p *SQLInfoProvider) GetSQLInfoResultMap() flightsql.SqlInfoResultMap {
	p.ensureInit()
	cp := make(map[uint32]interface{}, len(p.info))
	for k, v := range p.info {
		cp[k] = v
	}
	return flightsql.SqlInfoResultMap(cp)
}

//───────────────────────────────────
// XDBC TYPE‑INFO PROVIDER
//───────────────────────────────────

// Searchability describes how a type can appear in WHERE clauses.
type Searchability int32

const (
	SearchableNone Searchability = iota
	SearchableLike
	SearchableNoLike
	SearchableFull
)

// XdbcTypeInfoProvider serves XDBC type metadata.
type XdbcTypeInfoProvider struct {
	types []models.XdbcTypeInfo
}

// NewXdbcTypeInfoProvider constructs with built‑in Hatch/DuckDB types.
func NewXdbcTypeInfoProvider() *XdbcTypeInfoProvider {
	return &XdbcTypeInfoProvider{
		types: []models.XdbcTypeInfo{
			// BOOLEAN
			xdbcBool(),
			// TINYINT
			xdbcNumeric("TINYINT", java_sql_Types_TINYINT, 3),
			// add more as needed…
		},
	}
}

// GetTypeInfo filters by dataType when provided.
func (p *XdbcTypeInfoProvider) GetTypeInfo(dataType *int32) []models.XdbcTypeInfo {
	if dataType == nil {
		return p.types
	}
	out := make([]models.XdbcTypeInfo, 0, 4)
	for _, t := range p.types {
		if t.DataType == *dataType {
			out = append(out, t)
		}
	}
	return out
}

// GetTypeInfoResult builds an Arrow record for the XDBC response.
func (p *XdbcTypeInfoProvider) GetTypeInfoResult(alloc memory.Allocator, dt *int32) arrow.Record {
	rows := p.GetTypeInfo(dt)

	b := array.NewRecordBuilder(alloc, schema_ref.XdbcTypeInfo)
	defer b.Release()

	appendAny := func(i int, v sql.NullString) {
		if v.Valid {
			b.Field(i).(*array.StringBuilder).Append(v.String)
		} else {
			b.Field(i).AppendNull()
		}
	}
	appendInt := func(i int, v sql.NullInt32) {
		if v.Valid {
			b.Field(i).(*array.Int32Builder).Append(v.Int32)
		} else {
			b.Field(i).AppendNull()
		}
	}
	appendBool := func(i int, v sql.NullBool) {
		if v.Valid {
			b.Field(i).(*array.BooleanBuilder).Append(v.Bool)
		} else {
			b.Field(i).AppendNull()
		}
	}

	for _, t := range rows {
		b.Field(0).(*array.StringBuilder).Append(t.TypeName)
		b.Field(1).(*array.Int32Builder).Append(t.DataType)
		appendInt(2, t.ColumnSize)
		appendAny(3, t.LiteralPrefix)
		appendAny(4, t.LiteralSuffix)
		appendAny(5, t.CreateParams)
		b.Field(6).(*array.Int32Builder).Append(t.Nullable)
		b.Field(7).(*array.BooleanBuilder).Append(t.CaseSensitive)
		b.Field(8).(*array.Int32Builder).Append(t.Searchable)
		appendBool(9, t.UnsignedAttribute)
		b.Field(10).(*array.BooleanBuilder).Append(t.FixedPrecScale)
		appendBool(11, t.AutoIncrement)
		appendAny(12, t.LocalTypeName)
		appendInt(13, t.MinimumScale)
		appendInt(14, t.MaximumScale)
		b.Field(15).(*array.Int32Builder).Append(t.SQLDataType)
		appendInt(16, t.DatetimeSubcode)
		appendInt(17, t.NumPrecRadix)
		b.Field(18).AppendNull() // interval_precision
	}

	return b.NewRecord()
}

//───────────────────────────────────
// helpers for type info construction
//───────────────────────────────────

func xdbcBool() models.XdbcTypeInfo {
	return models.XdbcTypeInfo{
		TypeName:          "BOOLEAN",
		DataType:          java_sql_Types_BOOLEAN,
		ColumnSize:        sqlNullInt32(1),
		Nullable:          1,
		CaseSensitive:     false,
		Searchable:        int32(SearchableFull),
		UnsignedAttribute: sqlNullBool(false),
		FixedPrecScale:    true,
		AutoIncrement:     sqlNullBool(false),
		LocalTypeName:     sqlNullString("BOOLEAN"),
		SQLDataType:       java_sql_Types_BOOLEAN,
		NumPrecRadix:      sqlNullInt32(10),
	}
}

func xdbcNumeric(name string, sqlType int32, prec int32) models.XdbcTypeInfo {
	return models.XdbcTypeInfo{
		TypeName:          name,
		DataType:          sqlType,
		ColumnSize:        sqlNullInt32(prec),
		Nullable:          1,
		CaseSensitive:     false,
		Searchable:        int32(SearchableFull),
		UnsignedAttribute: sqlNullBool(false),
		FixedPrecScale:    true,
		AutoIncrement:     sqlNullBool(false),
		LocalTypeName:     sqlNullString(name),
		SQLDataType:       sqlType,
		NumPrecRadix:      sqlNullInt32(10),
	}
}

func sqlNullString(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
func sqlNullInt32(i int32) sql.NullInt32    { return sql.NullInt32{Int32: i, Valid: true} }
func sqlNullBool(b bool) sql.NullBool       { return sql.NullBool{Bool: b, Valid: true} }

//───────────────────────────────────
// constants
//───────────────────────────────────

// DuckDB keyword / function sets
var (
	duckdbKeywords = []string{
		"ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "AND", "ANY",
		"AS", "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN",
		"BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN",
		"COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE",
		"CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE",
		"DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DROP", "EACH",
		"ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE", "EXISTS", "EXPLAIN",
		"FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM",
		"FULL", "GROUP", "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN",
		"INDEX", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT",
		"INTO", "IS", "ISNULL", "JOIN", "KEY", "LAST", "LEFT", "LIKE",
		"LIMIT", "MATCH", "NATURAL", "NO", "NOT", "NOTNULL", "NULL",
		"OF", "OFFSET", "ON", "OR", "ORDER", "OUTER", "OVER", "PARTITION",
		"PLAN", "PRAGMA", "PRIMARY", "RANGE", "RECURSIVE", "REFERENCES",
		"REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RETURNING",
		"RIGHT", "ROLLBACK", "ROW", "ROWS", "SAVEPOINT", "SELECT", "SET",
		"TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION",
		"TRIGGER", "UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING",
		"VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE", "WINDOW",
		"WITH", "WITHOUT",
	}

	duckdbNumericFns = []string{
		"ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CEIL", "CEILING", "COS",
		"COT", "DEGREES", "DIV", "EXP", "FLOOR", "LN", "LOG", "MOD",
		"PI", "POW", "POWER", "RADIANS", "RAND", "RANDOM", "ROUND", "SIGN",
		"SIN", "SQRT", "TAN", "TRUNC", "WIDTH_BUCKET",
	}

	duckdbStringFns = []string{
		"ASCII", "CHAR", "CHAR_LENGTH", "CHR", "CONCAT", "CONTAINS",
		"ENDS_WITH", "INITCAP", "LEFT", "LENGTH", "LIKE", "LOWER",
		"LPAD", "LTRIM", "POSITION", "REGEXP_MATCHES", "REGEXP_REPLACE",
		"REGEXP_EXTRACT", "REPEAT", "REPLACE", "REVERSE", "RIGHT", "RPAD",
		"RTRIM", "SPLIT_PART", "STARTS_WITH", "STRPOS", "SUBSTRING", "TRIM",
		"UPPER",
	}
)

// Java SQL Types (JDBC) – extend as needed
const (
	java_sql_Types_BIT           = -7
	java_sql_Types_TINYINT       = -6
	java_sql_Types_SMALLINT      = 5
	java_sql_Types_INTEGER       = 4
	java_sql_Types_BIGINT        = -5
	java_sql_Types_FLOAT         = 6
	java_sql_Types_REAL          = 7
	java_sql_Types_DOUBLE        = 8
	java_sql_Types_NUMERIC       = 2
	java_sql_Types_DECIMAL       = 3
	java_sql_Types_CHAR          = 1
	java_sql_Types_VARCHAR       = 12
	java_sql_Types_LONGVARCHAR   = -1
	java_sql_Types_DATE          = 91
	java_sql_Types_TIME          = 92
	java_sql_Types_TIMESTAMP     = 93
	java_sql_Types_BINARY        = -2
	java_sql_Types_VARBINARY     = -3
	java_sql_Types_LONGVARBINARY = -4
	java_sql_Types_BOOLEAN       = 16
	java_sql_Types_NULL          = 0
	java_sql_Types_OTHER         = 1111
	java_sql_Types_BLOB          = 2004
	java_sql_Types_CLOB          = 2005
	java_sql_Types_ARRAY         = 2003
	java_sql_Types_STRUCT        = 2002
	java_sql_Types_REF           = 2006
	java_sql_Types_DATALINK      = 70
	java_sql_Types_ROWID         = -8
	java_sql_Types_SQLXML        = 2009
	java_sql_Types_NCHAR         = -15
	java_sql_Types_NVARCHAR      = -9
	java_sql_Types_LONGNVARCHAR  = -16
	java_sql_Types_NCLOB         = 2011
	java_sql_Types_REF_CURSOR    = 2012
	java_sql_Types_TIME_WITH_TZ  = 2013
	java_sql_Types_TIMESTAMP_TZ  = 2014
)
