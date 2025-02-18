package flight

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
)

func SqlInfoResultMap() flightsql.SqlInfoResultMap {
	return flightsql.SqlInfoResultMap{
		// Report the server name as DuckDB and update the version as needed.
		uint32(flightsql.SqlInfoFlightSqlServerName):         "DuckDB",
		uint32(flightsql.SqlInfoFlightSqlServerVersion):      "DuckDB 0.9.0", // adjust version if needed
		uint32(flightsql.SqlInfoFlightSqlServerArrowVersion): arrow.PkgVersion,
		uint32(flightsql.SqlInfoFlightSqlServerReadOnly):     false,
		// DuckDB does not support catalog DDL but does support schema DDL.
		uint32(flightsql.SqlInfoDDLCatalog): false,
		uint32(flightsql.SqlInfoDDLSchema):  true,
		uint32(flightsql.SqlInfoDDLTable):   true,
		// Identifiers are case insensitive; quoted identifiers use double quotes.
		uint32(flightsql.SqlInfoIdentifierCase):       int64(flightsql.SqlCaseSensitivityCaseInsensitive),
		uint32(flightsql.SqlInfoIdentifierQuoteChar):  `"`,
		uint32(flightsql.SqlInfoQuotedIdentifierCase): int64(flightsql.SqlCaseSensitivityCaseInsensitive),
		// All tables are selectable.
		uint32(flightsql.SqlInfoAllTablesAreASelectable): true,
		// Null ordering: for DuckDB, nulls sort at the start.
		uint32(flightsql.SqlInfoNullOrdering): int64(flightsql.SqlNullOrderingSortAtStart),
		// DuckDB supports transactions.
		uint32(flightsql.SqlInfoFlightSqlServerTransaction): int32(flightsql.SqlTransactionTransaction),
		uint32(flightsql.SqlInfoTransactionsSupported):      true,
		// Keywords: This is a sample list. Adjust as needed based on DuckDB’s documentation.
		uint32(flightsql.SqlInfoKeywords): []string{
			"ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC",
			"ATTACH", "AUTHORIZATION", "AUTOINCREMENT", // Note: DuckDB uses IDENTITY columns.
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
		},
		// Numeric functions available in DuckDB.
		uint32(flightsql.SqlInfoNumericFunctions): []string{
			"ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CEIL", "CEILING", "COS", "COSH",
			"EXP", "FLOOR", "LN", "LOG", "LOG10", "MOD", "PI", "POW", "POWER", "ROUND",
			"SIGN", "SQRT", "TRUNC",
		},
		// String functions available in DuckDB.
		uint32(flightsql.SqlInfoStringFunctions): []string{
			"ASCII", "CHAR", "CHAR_LENGTH", "CHARACTER_LENGTH", "CONCAT", "INSTR", "LCASE",
			"LEFT", "LENGTH", "LOCATE", "LTRIM", "POSITION", "REPEAT", "REPLACE", "RIGHT",
			"RTRIM", "SUBSTR", "SUBSTRING", "UCASE",
		},
		// SupportsConvert: as an example, converting BIGINT to INTEGER.
		uint32(flightsql.SqlInfoSupportsConvert): map[int32][]int32{
			int32(flightsql.SqlConvertBigInt): {int32(flightsql.SqlConvertInteger)},
		},
	}
}
