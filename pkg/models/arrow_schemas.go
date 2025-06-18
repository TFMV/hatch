// Package models provides data structures used throughout the Flight SQL server.
package models

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// GetCatalogsSchema returns the Arrow schema for catalog results.
func GetCatalogsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

// GetDBSchemasSchema returns the Arrow schema for database schema results.
func GetDBSchemasSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

// GetTablesSchema returns the Arrow schema for table results.
func GetTablesSchema(includeSchema bool) *arrow.Schema {
	fields := []arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "table_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "table_type", Type: arrow.BinaryTypes.String, Nullable: false},
	}

	if includeSchema {
		fields = append(fields, arrow.Field{
			Name:     "table_schema",
			Type:     arrow.BinaryTypes.Binary,
			Nullable: true,
		})
	}

	return arrow.NewSchema(fields, nil)
}

// GetTableTypesSchema returns the Arrow schema for table type results.
func GetTableTypesSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "table_type", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

// GetColumnsSchema returns the Arrow schema for column metadata results.
func GetColumnsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "table_cat", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "table_schem", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "table_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "column_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "data_type", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "type_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "column_size", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "decimal_digits", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "num_prec_radix", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "nullable", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "remarks", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "column_def", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "sql_data_type", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "char_octet_length", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "ordinal_position", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "is_nullable", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "is_autoincrement", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "is_generatedcolumn", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)
}

// GetPrimaryKeysSchema returns the Arrow schema for primary key results.
func GetPrimaryKeysSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "table_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "column_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "key_sequence", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "key_name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

// GetImportedKeysSchema returns the Arrow schema for imported foreign key results.
func GetImportedKeysSchema() *arrow.Schema {
	return GetForeignKeysSchema()
}

// GetExportedKeysSchema returns the Arrow schema for exported foreign key results.
func GetExportedKeysSchema() *arrow.Schema {
	return GetForeignKeysSchema()
}

// GetForeignKeysSchema returns the Arrow schema for foreign key results.
func GetForeignKeysSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "pk_catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "pk_db_schema_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pk_table_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pk_column_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "fk_catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "fk_db_schema_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "fk_table_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "fk_column_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "key_sequence", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fk_key_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "pk_key_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "update_rule", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "delete_rule", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
	}, nil)
}

// GetXdbcTypeInfoSchema returns the Arrow schema for XDBC type info results.
func GetXdbcTypeInfoSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "type_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "data_type", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "column_size", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "literal_prefix", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "literal_suffix", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "create_params", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "nullable", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "case_sensitive", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "searchable", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "unsigned_attribute", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "fixed_prec_scale", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "auto_increment", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "local_type_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "minimum_scale", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "maximum_scale", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "sql_data_type", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "datetime_subcode", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "num_prec_radix", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "interval_precision", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
}

// GetSqlInfoSchema returns the Arrow schema for SQL info results.
func GetSqlInfoSchema() *arrow.Schema {
	// SQL info uses a dense union type for the value
	fields := []arrow.Field{
		{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: false},                                                                     // 0: string value
		{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},                                                                  // 1: bool value
		{Name: "bigint_value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},                                                                   // 2: bigint value
		{Name: "int32_bitmask", Type: arrow.PrimitiveTypes.Int32, Nullable: false},                                                                  // 3: int32 bitmask
		{Name: "string_list", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},                                                        // 4: string list
		{Name: "int32_to_int32_list_map", Type: arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.ListOf(arrow.PrimitiveTypes.Int32)), Nullable: false}, // 5: int32 to int32 list map
	}

	codes := []arrow.UnionTypeCode{0, 1, 2, 3, 4, 5}
	valueType := arrow.DenseUnionOf(fields, codes)

	return arrow.NewSchema([]arrow.Field{
		{Name: "info_name", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
		{Name: "value", Type: valueType, Nullable: false},
	}, nil)
}

// XdbcTypeInfoResult holds XDBC type information for conversion to Arrow.
type XdbcTypeInfoResult struct {
	Types []XdbcTypeInfo
}

// ToArrowRecord converts XdbcTypeInfo results to an Arrow record.
func (x *XdbcTypeInfoResult) ToArrowRecord(allocator memory.Allocator) arrow.Record {
	schema := GetXdbcTypeInfoSchema()
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	for _, typeInfo := range x.Types {
		// type_name
		builder.Field(0).(*array.StringBuilder).Append(typeInfo.TypeName)

		// data_type
		builder.Field(1).(*array.Int32Builder).Append(typeInfo.DataType)

		// column_size
		if typeInfo.ColumnSize.Valid {
			builder.Field(2).(*array.Int32Builder).Append(typeInfo.ColumnSize.Int32)
		} else {
			builder.Field(2).AppendNull()
		}

		// literal_prefix
		if typeInfo.LiteralPrefix.Valid {
			builder.Field(3).(*array.StringBuilder).Append(typeInfo.LiteralPrefix.String)
		} else {
			builder.Field(3).AppendNull()
		}

		// literal_suffix
		if typeInfo.LiteralSuffix.Valid {
			builder.Field(4).(*array.StringBuilder).Append(typeInfo.LiteralSuffix.String)
		} else {
			builder.Field(4).AppendNull()
		}

		// create_params
		if typeInfo.CreateParams.Valid {
			builder.Field(5).(*array.StringBuilder).Append(typeInfo.CreateParams.String)
		} else {
			builder.Field(5).AppendNull()
		}

		// nullable
		builder.Field(6).(*array.Int32Builder).Append(typeInfo.Nullable)

		// case_sensitive
		builder.Field(7).(*array.BooleanBuilder).Append(typeInfo.CaseSensitive)

		// searchable
		builder.Field(8).(*array.Int32Builder).Append(typeInfo.Searchable)

		// unsigned_attribute
		if typeInfo.UnsignedAttribute.Valid {
			builder.Field(9).(*array.BooleanBuilder).Append(typeInfo.UnsignedAttribute.Bool)
		} else {
			builder.Field(9).AppendNull()
		}

		// fixed_prec_scale
		builder.Field(10).(*array.BooleanBuilder).Append(typeInfo.FixedPrecScale)

		// auto_increment
		if typeInfo.AutoIncrement.Valid {
			builder.Field(11).(*array.BooleanBuilder).Append(typeInfo.AutoIncrement.Bool)
		} else {
			builder.Field(11).AppendNull()
		}

		// local_type_name
		if typeInfo.LocalTypeName.Valid {
			builder.Field(12).(*array.StringBuilder).Append(typeInfo.LocalTypeName.String)
		} else {
			builder.Field(12).AppendNull()
		}

		// minimum_scale
		if typeInfo.MinimumScale.Valid {
			builder.Field(13).(*array.Int32Builder).Append(typeInfo.MinimumScale.Int32)
		} else {
			builder.Field(13).AppendNull()
		}

		// maximum_scale
		if typeInfo.MaximumScale.Valid {
			builder.Field(14).(*array.Int32Builder).Append(typeInfo.MaximumScale.Int32)
		} else {
			builder.Field(14).AppendNull()
		}

		// sql_data_type
		builder.Field(15).(*array.Int32Builder).Append(typeInfo.SQLDataType)

		// datetime_subcode
		if typeInfo.DatetimeSubcode.Valid {
			builder.Field(16).(*array.Int32Builder).Append(typeInfo.DatetimeSubcode.Int32)
		} else {
			builder.Field(16).AppendNull()
		}

		// num_prec_radix
		if typeInfo.NumPrecRadix.Valid {
			builder.Field(17).(*array.Int32Builder).Append(typeInfo.NumPrecRadix.Int32)
		} else {
			builder.Field(17).AppendNull()
		}

		// interval_precision
		if typeInfo.IntervalPrecision.Valid {
			builder.Field(18).(*array.Int32Builder).Append(typeInfo.IntervalPrecision.Int32)
		} else {
			builder.Field(18).AppendNull()
		}
	}

	return builder.NewRecord()
}

// SqlInfoResult holds SQL info for conversion to Arrow.
type SqlInfoResult struct {
	Info []SQLInfo
}

// ToArrowRecord converts SqlInfo results to an Arrow record.
func (s *SqlInfoResult) ToArrowRecord(allocator memory.Allocator) arrow.Record {
	schema := GetSqlInfoSchema()
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	for _, info := range s.Info {
		// info_name
		builder.Field(0).(*array.Uint32Builder).Append(info.InfoName)

		// value (dense union)
		unionBuilder := builder.Field(1).(*array.DenseUnionBuilder)

		switch v := info.Value.(type) {
		case string:
			unionBuilder.Append(0)
			unionBuilder.Child(0).(*array.StringBuilder).Append(v)
		case bool:
			unionBuilder.Append(1)
			unionBuilder.Child(1).(*array.BooleanBuilder).Append(v)
		case int64:
			unionBuilder.Append(2)
			unionBuilder.Child(2).(*array.Int64Builder).Append(v)
		case int32:
			unionBuilder.Append(3)
			unionBuilder.Child(3).(*array.Int32Builder).Append(v)
		case []string:
			unionBuilder.Append(4)
			listBuilder := unionBuilder.Child(4).(*array.ListBuilder)
			listBuilder.Append(true)
			valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
			for _, s := range v {
				valueBuilder.Append(s)
			}
		case map[int32][]int32:
			unionBuilder.Append(5)
			mapBuilder := unionBuilder.Child(5).(*array.MapBuilder)
			mapBuilder.Append(true)
			keyBuilder := mapBuilder.KeyBuilder().(*array.Int32Builder)
			itemBuilder := mapBuilder.ItemBuilder().(*array.ListBuilder)
			for k, vals := range v {
				keyBuilder.Append(k)
				itemBuilder.Append(true)
				valueBuilder := itemBuilder.ValueBuilder().(*array.Int32Builder)
				for _, val := range vals {
					valueBuilder.Append(val)
				}
			}
		default:
			// Default to string representation
			unionBuilder.Append(0)
			unionBuilder.Child(0).(*array.StringBuilder).Append("")
		}
	}

	return builder.NewRecord()
}
