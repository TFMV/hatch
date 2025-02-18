package flight

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// DuckDB SQL type code constants.
const (
	DUCKDB_SQL_NULL      = 0
	DUCKDB_SQL_BOOLEAN   = -7
	DUCKDB_SQL_TINYINT   = -6
	DUCKDB_SQL_SMALLINT  = 5
	DUCKDB_SQL_INTEGER   = 4
	DUCKDB_SQL_BIGINT    = -5
	DUCKDB_SQL_FLOAT     = 6
	DUCKDB_SQL_DOUBLE    = 8
	DUCKDB_SQL_DECIMAL   = 2
	DUCKDB_SQL_DATE      = 91
	DUCKDB_SQL_TIME      = 92
	DUCKDB_SQL_TIMESTAMP = 93
	DUCKDB_SQL_VARCHAR   = 12
	DUCKDB_SQL_VARBINARY = -3
)

// DuckDBTablesSchemaBatchReader implements a batch reader that retrieves
// table column metadata from DuckDB.
type DuckDBTablesSchemaBatchReader struct {
	refCount   int64
	mem        memory.Allocator
	ctx        context.Context
	rdr        array.RecordReader
	stmt       *sql.Stmt
	schemaBldr *array.BinaryBuilder
	record     arrow.Record
	err        error
}

// NewDuckDBTablesSchemaBatchReader creates a new reader. The query below uses
// DuckDB's information_schema to return (table_name, column_name, data_type, notnull)
// for the given table.
func NewDuckDBTablesSchemaBatchReader(ctx context.Context, mem memory.Allocator, rdr array.RecordReader, db *sql.DB, mainQuery string) (*DuckDBTablesSchemaBatchReader, error) {
	schemaQuery := `
SELECT table_name, column_name AS name, data_type AS type,
       CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull
FROM information_schema.columns
WHERE table_name = ?`

	stmt, err := db.PrepareContext(ctx, schemaQuery)
	if err != nil {
		rdr.Release()
		return nil, err
	}

	return &DuckDBTablesSchemaBatchReader{
		refCount:   1,
		ctx:        ctx,
		rdr:        rdr,
		stmt:       stmt,
		mem:        mem,
		schemaBldr: array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary),
	}, nil
}

func (s *DuckDBTablesSchemaBatchReader) Err() error { return s.err }

func (s *DuckDBTablesSchemaBatchReader) Retain() { atomic.AddInt64(&s.refCount, 1) }

func (s *DuckDBTablesSchemaBatchReader) Release() {
	if atomic.AddInt64(&s.refCount, -1) == 0 {
		s.rdr.Release()
		s.stmt.Close()
		s.schemaBldr.Release()
		if s.record != nil {
			s.record.Release()
			s.record = nil
		}
	}
}

func (s *DuckDBTablesSchemaBatchReader) Schema() *arrow.Schema {
	fields := append(s.rdr.Schema().Fields(),
		arrow.Field{Name: "table_schema", Type: arrow.BinaryTypes.Binary})
	return arrow.NewSchema(fields, nil)
}

func (s *DuckDBTablesSchemaBatchReader) Record() arrow.Record { return s.record }

// getSqlTypeFromTypeName maps a DuckDB type name (as returned in the catalog)
// to a DuckDB SQL type code.
func getSqlTypeFromTypeName(sqltype string) int {
	if sqltype == "" {
		return DUCKDB_SQL_NULL
	}

	sqltype = strings.ToLower(sqltype)
	if strings.Contains(sqltype, "boolean") {
		return DUCKDB_SQL_BOOLEAN
	}
	if strings.HasPrefix(sqltype, "tinyint") {
		return DUCKDB_SQL_TINYINT
	}
	if strings.HasPrefix(sqltype, "smallint") {
		return DUCKDB_SQL_SMALLINT
	}
	if strings.HasPrefix(sqltype, "bigint") {
		return DUCKDB_SQL_BIGINT
	}
	// "int" and "integer" are mapped here.
	if strings.HasPrefix(sqltype, "int") {
		return DUCKDB_SQL_INTEGER
	}
	if strings.Contains(sqltype, "double") {
		return DUCKDB_SQL_DOUBLE
	}
	if strings.Contains(sqltype, "float") {
		return DUCKDB_SQL_FLOAT
	}
	if strings.Contains(sqltype, "decimal") {
		return DUCKDB_SQL_DECIMAL
	}
	if sqltype == "date" {
		return DUCKDB_SQL_DATE
	}
	if sqltype == "time" {
		return DUCKDB_SQL_TIME
	}
	if sqltype == "timestamp" {
		return DUCKDB_SQL_TIMESTAMP
	}
	if strings.HasPrefix(sqltype, "varchar") || strings.HasPrefix(sqltype, "char") {
		return DUCKDB_SQL_VARCHAR
	}
	if strings.Contains(sqltype, "blob") || strings.Contains(sqltype, "binary") {
		return DUCKDB_SQL_VARBINARY
	}
	return DUCKDB_SQL_NULL
}

// getPrecisionFromCol returns a default precision for certain types.
func getPrecisionFromCol(sqltype int) int {
	switch sqltype {
	case DUCKDB_SQL_INTEGER, DUCKDB_SQL_BIGINT, DUCKDB_SQL_SMALLINT, DUCKDB_SQL_TINYINT:
		return 10
	case DUCKDB_SQL_FLOAT, DUCKDB_SQL_DOUBLE:
		return 15
	default:
		return 0
	}
}

// getColumnMetadata uses the flightsql.ColumnMetadataBuilder to add metadata
// (such as precision and scale) for a given column.
func getColumnMetadata(bldr *flightsql.ColumnMetadataBuilder, sqltype int, table string) arrow.Metadata {
	defer bldr.Clear()

	bldr.Scale(15).IsReadOnly(false).IsAutoIncrement(false)
	if table != "" {
		bldr.TableName(table)
	}
	// For non-text/binary types, set a default precision.
	switch sqltype {
	case DUCKDB_SQL_VARCHAR, DUCKDB_SQL_VARBINARY:
		// No precision metadata needed.
	default:
		bldr.Precision(int32(getPrecisionFromCol(sqltype)))
	}

	return bldr.Metadata()
}

// Next fetches the next batch of table schema metadata.
func (s *DuckDBTablesSchemaBatchReader) Next() bool {
	if s.record != nil {
		s.record.Release()
		s.record = nil
	}

	if !s.rdr.Next() {
		return false
	}

	rec := s.rdr.Record()
	tableNameArr := rec.Column(rec.Schema().FieldIndices("table_name")[0]).(*array.String)

	bldr := flightsql.NewColumnMetadataBuilder()
	columnFields := make([]arrow.Field, 0)
	for i := 0; i < tableNameArr.Len(); i++ {
		table := tableNameArr.Value(i)
		rows, err := s.stmt.QueryContext(s.ctx, table)
		if err != nil {
			s.err = err
			return false
		}

		var tableName, name, typ string
		var nn int
		for rows.Next() {
			if err := rows.Scan(&tableName, &name, &typ, &nn); err != nil {
				rows.Close()
				s.err = err
				return false
			}

			columnFields = append(columnFields, arrow.Field{
				Name:     name,
				Type:     getArrowTypeFromString(typ), // Assume this helper exists.
				Nullable: nn == 0,
				Metadata: getColumnMetadata(bldr, getSqlTypeFromTypeName(typ), tableName),
			})
		}

		rows.Close()
		if rows.Err() != nil {
			s.err = rows.Err()
			return false
		}
		val := flight.SerializeSchema(arrow.NewSchema(columnFields, nil), s.mem)
		s.schemaBldr.Append(val)

		columnFields = columnFields[:0]
	}

	schemaCol := s.schemaBldr.NewArray()
	defer schemaCol.Release()

	s.record = array.NewRecord(s.Schema(), append(rec.Columns(), schemaCol), rec.NumRows())
	return true
}
