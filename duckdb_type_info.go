package flight

import (
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// GetTypeInfoResult returns an Arrow Record containing type-info metadata
// for DuckDB. We define 13 types with metadata corresponding to their SQL
// type codes and properties.
func GetTypeInfoResult(mem memory.Allocator) arrow.Record {
	// DuckDB type names
	typeNames, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`[
		"BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
		"FLOAT", "DOUBLE", "DECIMAL", "DATE", "TIME", "TIMESTAMP",
		"VARCHAR", "VARBINARY"
	]`))
	if err != nil {
		panic(err)
	}
	defer typeNames.Release()

	// SQL type codes (using standard ODBC / JDBC codes)
	dataType, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[
		-7, -6, 5, 4, -5, 6, 8, 2, 91, 92, 93, 12, -3
	]`))
	if err != nil {
		panic(err)
	}
	defer dataType.Release()

	// Maximum column size (length, precision etc.)
	columnSize, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[
		1, 3, 5, 10, 19, 7, 15, 38, 10, 8, 26, 255, 255
	]`))
	if err != nil {
		panic(err)
	}
	defer columnSize.Release()

	// Literal prefixes – only the date/time and string types get quotes
	literalPrefix, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`[
		null, null, null, null, null, null, null, null, "'", "'", "'", "'", null
	]`))
	if err != nil {
		panic(err)
	}
	defer literalPrefix.Release()

	literalSuffix, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`[
		null, null, null, null, null, null, null, null, "'", "'", "'", "'", null
	]`))
	if err != nil {
		panic(err)
	}
	defer literalSuffix.Release()

	// Parameters needed to create the type:
	// For DECIMAL we allow "precision" and "scale", for VARCHAR and VARBINARY a "length".
	createParams, _, err := array.FromJSON(mem, arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.BinaryTypes.String, Nullable: false}),
		strings.NewReader(`[
			[], [], [], [], [], [], [],
			["precision", "scale"],
			[],
			[],
			["length"],
			["length"]
		]`))
	if err != nil {
		panic(err)
	}
	defer createParams.Release()

	// All types are nullable (1 means nullable)
	nullable, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
	]`))
	if err != nil {
		panic(err)
	}
	defer nullable.Release()

	// For case sensitivity: only VARCHAR is marked case sensitive here.
	caseSensitive, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(`[
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0
	]`))
	if err != nil {
		panic(err)
	}
	defer caseSensitive.Release()

	// Searchability: we mark every type with 3 (fully searchable)
	searchable, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[
		3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3
	]`))
	if err != nil {
		panic(err)
	}
	defer searchable.Release()

	// Unsigned attribute: DuckDB does not support unsigned types so we set these to false.
	unsignedAttribute, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(`[
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	]`))
	if err != nil {
		panic(err)
	}
	defer unsignedAttribute.Release()

	// For fixed precision scale, only DECIMAL is marked as having a fixed scale.
	fixedPrecScale, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(`[
		0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0
	]`))
	if err != nil {
		panic(err)
	}
	defer fixedPrecScale.Release()

	// Auto unique value is false for all types.
	autoUniqueVal, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(`[
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	]`))
	if err != nil {
		panic(err)
	}
	defer autoUniqueVal.Release()

	// Local type names – we simply use the same names.
	localTypeName, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`[
		"BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
		"FLOAT", "DOUBLE", "DECIMAL", "DATE", "TIME", "TIMESTAMP",
		"VARCHAR", "VARBINARY"
	]`))
	if err != nil {
		panic(err)
	}
	defer localTypeName.Release()

	// Minimal scale – here all set to 0.
	minimalScale, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	]`))
	if err != nil {
		panic(err)
	}
	defer minimalScale.Release()

	// Maximum scale – all set to 0.
	maximumScale, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	]`))
	if err != nil {
		panic(err)
	}
	defer maximumScale.Release()

	// SQL data type – same as dataType.
	sqlDataType, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[
		-7, -6, 5, 4, -5, 6, 8, 2, 91, 92, 93, 12, -3
	]`))
	if err != nil {
		panic(err)
	}
	defer sqlDataType.Release()

	// SQL DateTime Sub types – all set to 0.
	sqlDateTimeSub, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	]`))
	if err != nil {
		panic(err)
	}
	defer sqlDateTimeSub.Release()

	// NUM_PREC_RADIX – for numeric types we could provide 10 (or 2 for floats),
	// but here we mix in a bit of detail:
	//   * BOOLEAN, DATE, TIME, TIMESTAMP, VARCHAR and VARBINARY get 0,
	//   * Integer types get 10 and float types get 2.
	numPrecRadix, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[
		0, 10, 10, 10, 10, 2, 2, 10, 0, 0, 0, 0, 0
	]`))
	if err != nil {
		panic(err)
	}
	defer numPrecRadix.Release()

	// Interval precision – not used, so all 0.
	intervalPrecision, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	]`))
	if err != nil {
		panic(err)
	}
	defer intervalPrecision.Release()

	// The final record is constructed with the DuckDB type-info schema.
	// (Note: the last parameter "13" is the number of rows.)
	return array.NewRecord(schema_ref.XdbcTypeInfo, []arrow.Array{
		typeNames, dataType, columnSize, literalPrefix, literalSuffix,
		createParams, nullable, caseSensitive, searchable, unsignedAttribute,
		fixedPrecScale, autoUniqueVal, localTypeName, minimalScale, maximumScale,
		sqlDataType, sqlDateTimeSub, numPrecRadix, intervalPrecision,
	}, 13)
}

// GetFilteredTypeInfoResult returns a slice of the full type info record,
// filtering based on the SQL type code (filter).
func GetFilteredTypeInfoResult(mem memory.Allocator, filter int32) arrow.Record {
	batch := GetTypeInfoResult(mem)
	defer batch.Release()

	// Use the new DuckDB data type codes.
	dataTypeVector := []int32{-7, -6, 5, 4, -5, 6, 8, 2, 91, 92, 93, 12, -3}
	start, end := -1, -1
	for i, v := range dataTypeVector {
		if filter == v {
			if start == -1 {
				start = i
			}
		} else if start != -1 && end == -1 {
			end = i
			break
		}
	}
	if start == -1 {
		// Not found: return an empty slice.
		return batch.NewSlice(0, 0)
	}
	if end == -1 {
		end = len(dataTypeVector)
	}
	return batch.NewSlice(int64(start), int64(end))
}
