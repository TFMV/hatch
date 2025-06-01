// Package infrastructure provides core infrastructure components.
package infrastructure

import (
	"database/sql"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/TFMV/flight/pkg/models"
)

// SQLInfoProvider provides SQL feature information for DuckDB.
type SQLInfoProvider struct {
	allocator memory.Allocator
	infoMap   map[uint32]interface{}
}

// NewSQLInfoProvider creates a new SQL info provider.
func NewSQLInfoProvider(allocator memory.Allocator) *SQLInfoProvider {
	provider := &SQLInfoProvider{
		allocator: allocator,
		infoMap:   make(map[uint32]interface{}),
	}

	provider.initializeSQLInfo()
	return provider
}

// initializeSQLInfo initializes the SQL info map with DuckDB capabilities.
func (p *SQLInfoProvider) initializeSQLInfo() {
	// Flight SQL identification
	p.infoMap[uint32(flightsql.SqlInfoFlightSqlServerName)] = "DuckDB Flight SQL Server"
	p.infoMap[uint32(flightsql.SqlInfoFlightSqlServerVersion)] = "2.0.0"
	p.infoMap[uint32(flightsql.SqlInfoFlightSqlServerArrowVersion)] = "18.0.0"
	p.infoMap[uint32(flightsql.SqlInfoFlightSqlServerReadOnly)] = false
	p.infoMap[uint32(flightsql.SqlInfoFlightSqlServerTransaction)] = int32(flightsql.SqlTransactionTransaction)

	// SQL language support
	p.infoMap[uint32(flightsql.SqlInfoDDLCatalog)] = false // DuckDB doesn't support catalog DDL
	p.infoMap[uint32(flightsql.SqlInfoDDLSchema)] = true
	p.infoMap[uint32(flightsql.SqlInfoDDLTable)] = true

	// Identifier behavior
	p.infoMap[uint32(flightsql.SqlInfoIdentifierCase)] = int64(flightsql.SqlCaseSensitivityCaseInsensitive)
	p.infoMap[uint32(flightsql.SqlInfoIdentifierQuoteChar)] = "\""
	p.infoMap[uint32(flightsql.SqlInfoQuotedIdentifierCase)] = int64(flightsql.SqlCaseSensitivityCaseInsensitive)

	// Null ordering
	p.infoMap[uint32(flightsql.SqlInfoNullOrdering)] = int64(flightsql.SqlNullOrderingSortAtStart)

	// All tables are selectable
	p.infoMap[uint32(flightsql.SqlInfoAllTablesAreASelectable)] = true

	// Transaction support
	p.infoMap[uint32(flightsql.SqlInfoTransactionsSupported)] = true

	// Keywords
	p.infoMap[uint32(flightsql.SqlInfoKeywords)] = []string{
		"ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC",
		"ATTACH", "AUTHORIZATION", "AUTOINCREMENT",
		"BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE",
		"COLUMN", "COMMIT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME",
		"CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC",
		"DETACH", "DISTINCT", "DROP", "ELSE", "END", "EXCEPT", "EXCLUDE", "EXISTS", "EXPLAIN",
		"FAIL", "FOR", "FOREIGN", "FROM", "FULL", "GLOB", "GROUP", "HAVING", "IF", "IGNORE",
		"IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD",
		"INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LEFT", "LIKE", "LIMIT", "MATCH",
		"NATURAL", "NO", "NOT", "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON", "OR",
		"ORDER", "OUTER", "OVER", "PRIMARY", "REFERENCES", "REGEXP", "REINDEX", "RELEASE",
		"RENAME", "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK", "ROW", "SAVEPOINT", "SELECT",
		"SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TO", "TRANSACTION", "TRIGGER", "UNION",
		"UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE",
		"WITH", "WITHOUT",
	}

	// Numeric functions
	p.infoMap[uint32(flightsql.SqlInfoNumericFunctions)] = []string{
		"ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CEIL", "CEILING", "COS", "COSH",
		"EXP", "FLOOR", "LN", "LOG", "LOG10", "MOD", "PI", "POW", "POWER", "ROUND",
		"SIGN", "SQRT", "TRUNC",
	}

	// String functions
	p.infoMap[uint32(flightsql.SqlInfoStringFunctions)] = []string{
		"ASCII", "CHAR", "CHAR_LENGTH", "CHARACTER_LENGTH", "CONCAT", "INSTR", "LCASE",
		"LEFT", "LENGTH", "LOCATE", "LTRIM", "POSITION", "REPEAT", "REPLACE", "RIGHT",
		"RTRIM", "SUBSTR", "SUBSTRING", "UCASE",
	}

	// SupportsConvert: example conversions
	p.infoMap[uint32(flightsql.SqlInfoSupportsConvert)] = map[int32][]int32{
		int32(flightsql.SqlConvertBigInt): {int32(flightsql.SqlConvertInteger)},
	}
}

// GetSQLInfo returns SQL info for the requested info types.
func (p *SQLInfoProvider) GetSQLInfo(infoTypes []uint32) ([]models.SQLInfo, error) {
	if len(infoTypes) == 0 {
		// Return all info
		result := make([]models.SQLInfo, 0, len(p.infoMap))
		for k, v := range p.infoMap {
			result = append(result, models.SQLInfo{
				InfoName: k,
				Value:    v,
			})
		}
		return result, nil
	}

	// Return only requested info
	result := make([]models.SQLInfo, 0, len(infoTypes))
	for _, infoType := range infoTypes {
		if v, ok := p.infoMap[infoType]; ok {
			result = append(result, models.SQLInfo{
				InfoName: infoType,
				Value:    v,
			})
		}
	}

	return result, nil
}

// GetSQLInfoResultMap returns the SQL info as a flightsql.SqlInfoResultMap.
func (p *SQLInfoProvider) GetSQLInfoResultMap() flightsql.SqlInfoResultMap {
	return flightsql.SqlInfoResultMap(p.infoMap)
}

// XdbcTypeInfoProvider provides XDBC type information.
type XdbcTypeInfoProvider struct {
	types []models.XdbcTypeInfo
}

// NewXdbcTypeInfoProvider creates a new XDBC type info provider.
func NewXdbcTypeInfoProvider() *XdbcTypeInfoProvider {
	provider := &XdbcTypeInfoProvider{}
	provider.initializeTypes()
	return provider
}

// Searchable constants
const (
	SearchableNone          = 0 // No support
	SearchableSearchable    = 1 // Only searchable with WHERE ... LIKE
	SearchableAllExceptLike = 2 // Searchable except with WHERE ... LIKE
	SearchableFull          = 3 // Full searchable support
)

// initializeTypes initializes the XDBC type information.
func (p *XdbcTypeInfoProvider) initializeTypes() {
	p.types = []models.XdbcTypeInfo{
		// Boolean
		{
			TypeName:          "BOOLEAN",
			DataType:          java_sql_Types_BOOLEAN,
			ColumnSize:        sql.NullInt32{Int32: 1, Valid: true},
			LiteralPrefix:     sql.NullString{},
			LiteralSuffix:     sql.NullString{},
			CreateParams:      sql.NullString{},
			Nullable:          1, // NULL allowed
			CaseSensitive:     false,
			Searchable:        int32(SearchableFull),
			UnsignedAttribute: sql.NullBool{Bool: false, Valid: true},
			FixedPrecScale:    true,
			AutoIncrement:     sql.NullBool{Bool: false, Valid: true},
			LocalTypeName:     sql.NullString{String: "BOOLEAN", Valid: true},
			MinimumScale:      sql.NullInt32{Int32: 0, Valid: true},
			MaximumScale:      sql.NullInt32{Int32: 0, Valid: true},
			SQLDataType:       java_sql_Types_BOOLEAN,
			DatetimeSubcode:   sql.NullInt32{},
			NumPrecRadix:      sql.NullInt32{Int32: 10, Valid: true},
		},
		// TINYINT
		{
			TypeName:          "TINYINT",
			DataType:          java_sql_Types_TINYINT,
			ColumnSize:        sql.NullInt32{Int32: 3, Valid: true},
			LiteralPrefix:     sql.NullString{},
			LiteralSuffix:     sql.NullString{},
			CreateParams:      sql.NullString{},
			Nullable:          1,
			CaseSensitive:     false,
			Searchable:        int32(SearchableFull),
			UnsignedAttribute: sql.NullBool{Bool: false, Valid: true},
			FixedPrecScale:    true,
			AutoIncrement:     sql.NullBool{Bool: false, Valid: true},
			LocalTypeName:     sql.NullString{String: "TINYINT", Valid: true},
			MinimumScale:      sql.NullInt32{Int32: 0, Valid: true},
			MaximumScale:      sql.NullInt32{Int32: 0, Valid: true},
			SQLDataType:       java_sql_Types_TINYINT,
			NumPrecRadix:      sql.NullInt32{Int32: 10, Valid: true},
		},
		// Additional types would be added here...
	}
}

// GetTypeInfo returns type information, optionally filtered by data type.
func (p *XdbcTypeInfoProvider) GetTypeInfo(dataType *int32) []models.XdbcTypeInfo {
	if dataType == nil {
		return p.types
	}

	var result []models.XdbcTypeInfo
	for _, t := range p.types {
		if t.DataType == *dataType {
			result = append(result, t)
		}
	}
	return result
}

// GetTypeInfoResult returns an Arrow record with type info.
func (p *XdbcTypeInfoProvider) GetTypeInfoResult(allocator memory.Allocator, dataType *int32) arrow.Record {
	types := p.GetTypeInfo(dataType)

	// Build arrays for each field
	b := array.NewRecordBuilder(allocator, schema_ref.XdbcTypeInfo)
	defer b.Release()

	for _, t := range types {
		// type_name
		b.Field(0).(*array.StringBuilder).Append(t.TypeName)
		// data_type
		b.Field(1).(*array.Int32Builder).Append(t.DataType)
		// column_size
		if t.ColumnSize.Valid {
			b.Field(2).(*array.Int32Builder).Append(t.ColumnSize.Int32)
		} else {
			b.Field(2).AppendNull()
		}
		// literal_prefix
		if t.LiteralPrefix.Valid {
			b.Field(3).(*array.StringBuilder).Append(t.LiteralPrefix.String)
		} else {
			b.Field(3).AppendNull()
		}
		// literal_suffix
		if t.LiteralSuffix.Valid {
			b.Field(4).(*array.StringBuilder).Append(t.LiteralSuffix.String)
		} else {
			b.Field(4).AppendNull()
		}
		// create_params
		if t.CreateParams.Valid {
			b.Field(5).(*array.StringBuilder).Append(t.CreateParams.String)
		} else {
			b.Field(5).AppendNull()
		}
		// nullable
		b.Field(6).(*array.Int32Builder).Append(t.Nullable)
		// case_sensitive
		b.Field(7).(*array.BooleanBuilder).Append(t.CaseSensitive)
		// searchable
		b.Field(8).(*array.Int32Builder).Append(t.Searchable)
		// unsigned_attribute
		if t.UnsignedAttribute.Valid {
			b.Field(9).(*array.BooleanBuilder).Append(t.UnsignedAttribute.Bool)
		} else {
			b.Field(9).AppendNull()
		}
		// fixed_prec_scale
		b.Field(10).(*array.BooleanBuilder).Append(t.FixedPrecScale)
		// auto_increment
		if t.AutoIncrement.Valid {
			b.Field(11).(*array.BooleanBuilder).Append(t.AutoIncrement.Bool)
		} else {
			b.Field(11).AppendNull()
		}
		// local_type_name
		if t.LocalTypeName.Valid {
			b.Field(12).(*array.StringBuilder).Append(t.LocalTypeName.String)
		} else {
			b.Field(12).AppendNull()
		}
		// minimum_scale
		if t.MinimumScale.Valid {
			b.Field(13).(*array.Int32Builder).Append(t.MinimumScale.Int32)
		} else {
			b.Field(13).AppendNull()
		}
		// maximum_scale
		if t.MaximumScale.Valid {
			b.Field(14).(*array.Int32Builder).Append(t.MaximumScale.Int32)
		} else {
			b.Field(14).AppendNull()
		}
		// sql_data_type
		b.Field(15).(*array.Int32Builder).Append(t.SQLDataType)
		// datetime_subcode
		if t.DatetimeSubcode.Valid {
			b.Field(16).(*array.Int32Builder).Append(t.DatetimeSubcode.Int32)
		} else {
			b.Field(16).AppendNull()
		}
		// num_prec_radix
		if t.NumPrecRadix.Valid {
			b.Field(17).(*array.Int32Builder).Append(t.NumPrecRadix.Int32)
		} else {
			b.Field(17).AppendNull()
		}
		// interval_precision - always null for now
		b.Field(18).AppendNull()
	}

	return b.NewRecord()
}

// Java SQL Types constants (from JDBC spec)
const (
	java_sql_Types_BIT         = -7
	java_sql_Types_TINYINT     = -6
	java_sql_Types_SMALLINT    = 5
	java_sql_Types_INTEGER     = 4
	java_sql_Types_BIGINT      = -5
	java_sql_Types_FLOAT       = 6
	java_sql_Types_REAL        = 7
	java_sql_Types_DOUBLE      = 8
	java_sql_Types_NUMERIC     = 2
	java_sql_Types_DECIMAL     = 3
	java_sql_Types_CHAR        = 1
	java_sql_Types_VARCHAR     = 12
	java_sql_Types_LONGVARCHAR = -1
	java_sql_Types_DATE        = 91
	java_sql_Types_TIME        = 92
	java_sql_Types_TIMESTAMP   = 93
	java_sql_Types_BINARY      = -2
	java_sql_Types_VARBINARY   = -3
	java_sql_Types_BOOLEAN     = 16
	java_sql_Types_BLOB        = 2004
)
