package models

import (
	"database/sql"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryStructures(t *testing.T) {
	t.Run("QueryRequest", func(t *testing.T) {
		req := QueryRequest{
			Query:      "SELECT * FROM test",
			Parameters: []interface{}{1, "test"},
		}
		assert.Equal(t, "SELECT * FROM test", req.Query)
		assert.Len(t, req.Parameters, 2)
	})

	t.Run("QueryResult", func(t *testing.T) {
		allocator := memory.NewGoAllocator()
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		builder := array.NewRecordBuilder(allocator, schema)
		defer builder.Release()

		result := QueryResult{
			Schema: schema,
		}
		assert.NotNil(t, result.Schema)
		assert.Equal(t, "id", result.Schema.Field(0).Name)
	})

	t.Run("TransactionOptions", func(t *testing.T) {
		opts := TransactionOptions{
			ReadOnly: true,
			Timeout:  5 * time.Second,
		}
		assert.True(t, opts.ReadOnly)
		assert.Equal(t, 5*time.Second, opts.Timeout)
	})
}

func TestMetadataStructures(t *testing.T) {
	t.Run("Catalog", func(t *testing.T) {
		catalog := Catalog{
			Name:        "test_catalog",
			Description: "Test catalog",
			Properties: map[string]interface{}{
				"key": "value",
			},
		}
		assert.Equal(t, "test_catalog", catalog.Name)
		assert.Equal(t, "Test catalog", catalog.Description)
		assert.Equal(t, "value", catalog.Properties["key"])
	})

	t.Run("Schema", func(t *testing.T) {
		schema := Schema{
			CatalogName: "test_catalog",
			Name:        "test_schema",
			Owner:       "test_user",
		}
		assert.Equal(t, "test_catalog", schema.CatalogName)
		assert.Equal(t, "test_schema", schema.Name)
		assert.Equal(t, "test_user", schema.Owner)
	})

	t.Run("Table", func(t *testing.T) {
		now := time.Now()
		rowCount := int64(100)
		table := Table{
			CatalogName: "test_catalog",
			SchemaName:  "test_schema",
			Name:        "test_table",
			Type:        "TABLE",
			CreatedAt:   &now,
			RowCount:    &rowCount,
		}
		assert.Equal(t, "test_catalog", table.CatalogName)
		assert.Equal(t, "test_schema", table.SchemaName)
		assert.Equal(t, "test_table", table.Name)
		assert.Equal(t, "TABLE", table.Type)
		assert.Equal(t, now, *table.CreatedAt)
		assert.Equal(t, rowCount, *table.RowCount)
	})

	t.Run("Column", func(t *testing.T) {
		column := Column{
			CatalogName:     "test_catalog",
			SchemaName:      "test_schema",
			TableName:       "test_table",
			Name:            "test_column",
			OrdinalPosition: 1,
			DataType:        "VARCHAR",
			IsNullable:      true,
		}
		assert.Equal(t, "test_catalog", column.CatalogName)
		assert.Equal(t, "test_schema", column.SchemaName)
		assert.Equal(t, "test_table", column.TableName)
		assert.Equal(t, "test_column", column.Name)
		assert.Equal(t, 1, column.OrdinalPosition)
		assert.Equal(t, "VARCHAR", column.DataType)
		assert.True(t, column.IsNullable)
	})
}

func TestArrowSchemas(t *testing.T) {
	t.Run("GetCatalogsSchema", func(t *testing.T) {
		schema := GetCatalogsSchema()
		assert.Equal(t, 1, schema.NumFields())
		assert.Equal(t, "catalog_name", schema.Field(0).Name)
		assert.Equal(t, arrow.BinaryTypes.String, schema.Field(0).Type)
	})

	t.Run("GetDBSchemasSchema", func(t *testing.T) {
		schema := GetDBSchemasSchema()
		assert.Equal(t, 2, schema.NumFields())
		assert.Equal(t, "catalog_name", schema.Field(0).Name)
		assert.Equal(t, "db_schema_name", schema.Field(1).Name)
	})

	t.Run("GetTablesSchema", func(t *testing.T) {
		schema := GetTablesSchema(true)
		assert.Equal(t, 5, schema.NumFields())
		assert.Equal(t, "catalog_name", schema.Field(0).Name)
		assert.Equal(t, "db_schema_name", schema.Field(1).Name)
		assert.Equal(t, "table_name", schema.Field(2).Name)
		assert.Equal(t, "table_type", schema.Field(3).Name)
		assert.Equal(t, "table_schema", schema.Field(4).Name)
	})

	t.Run("GetPrimaryKeysSchema", func(t *testing.T) {
		schema := GetPrimaryKeysSchema()
		assert.Equal(t, 6, schema.NumFields())
		assert.Equal(t, "catalog_name", schema.Field(0).Name)
		assert.Equal(t, "db_schema_name", schema.Field(1).Name)
		assert.Equal(t, "table_name", schema.Field(2).Name)
		assert.Equal(t, "column_name", schema.Field(3).Name)
		assert.Equal(t, "key_sequence", schema.Field(4).Name)
		assert.Equal(t, "key_name", schema.Field(5).Name)
	})
}

func TestXdbcTypeInfoConversion(t *testing.T) {
	allocator := memory.NewGoAllocator()
	typeInfo := XdbcTypeInfo{
		TypeName:          "VARCHAR",
		DataType:          12,
		ColumnSize:        sql.NullInt32{Int32: 255, Valid: true},
		Nullable:          1,
		CaseSensitive:     true,
		Searchable:        3,
		FixedPrecScale:    false,
		SQLDataType:       12,
		UnsignedAttribute: sql.NullBool{Bool: false, Valid: true},
	}

	result := &XdbcTypeInfoResult{
		Types: []XdbcTypeInfo{typeInfo},
	}

	record := result.ToArrowRecord(allocator)
	require.NotNil(t, record)
	assert.Equal(t, int64(1), record.NumRows())
	assert.Equal(t, int64(19), record.NumCols())

	// Verify some key fields
	assert.Equal(t, "VARCHAR", record.Column(0).(*array.String).Value(0))
	assert.Equal(t, int32(12), record.Column(1).(*array.Int32).Value(0))
	assert.Equal(t, int32(255), record.Column(2).(*array.Int32).Value(0))
	assert.Equal(t, int32(1), record.Column(6).(*array.Int32).Value(0))
	assert.Equal(t, true, record.Column(7).(*array.Boolean).Value(0))
}

func TestForeignKeyRules(t *testing.T) {
	t.Run("FKRule Constants", func(t *testing.T) {
		assert.Equal(t, FKRule(0), FKRuleCascade)
		assert.Equal(t, FKRule(1), FKRuleRestrict)
		assert.Equal(t, FKRule(2), FKRuleSetNull)
		assert.Equal(t, FKRule(3), FKRuleNoAction)
		assert.Equal(t, FKRule(4), FKRuleSetDefault)
	})

	t.Run("ForeignKey Structure", func(t *testing.T) {
		fk := ForeignKey{
			PKCatalogName: "pk_catalog",
			PKSchemaName:  "pk_schema",
			PKTableName:   "pk_table",
			PKColumnName:  "pk_column",
			FKCatalogName: "fk_catalog",
			FKSchemaName:  "fk_schema",
			FKTableName:   "fk_table",
			FKColumnName:  "fk_column",
			KeySequence:   1,
			UpdateRule:    FKRuleCascade,
			DeleteRule:    FKRuleRestrict,
		}

		assert.Equal(t, "pk_catalog", fk.PKCatalogName)
		assert.Equal(t, "fk_catalog", fk.FKCatalogName)
		assert.Equal(t, FKRuleCascade, fk.UpdateRule)
		assert.Equal(t, FKRuleRestrict, fk.DeleteRule)
	})
}
