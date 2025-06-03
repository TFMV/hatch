package services

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TFMV/hatch/pkg/models"
)

// mockMetadataRepo implements repositories.MetadataRepository
type mockMetadataRepo struct {
	getCatalogsFunc       func(ctx context.Context) ([]models.Catalog, error)
	getSchemasFunc        func(ctx context.Context, catalog string, pattern string) ([]models.Schema, error)
	getTablesFunc         func(ctx context.Context, opts models.GetTablesOptions) ([]models.Table, error)
	getTableTypesFunc     func(ctx context.Context) ([]string, error)
	getColumnsFunc        func(ctx context.Context, table models.TableRef) ([]models.Column, error)
	getPrimaryKeysFunc    func(ctx context.Context, table models.TableRef) ([]models.Key, error)
	getImportedKeysFunc   func(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error)
	getExportedKeysFunc   func(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error)
	getCrossReferenceFunc func(ctx context.Context, ref models.CrossTableRef) ([]models.ForeignKey, error)
	getTypeInfoFunc       func(ctx context.Context, dataType *int32) ([]models.XdbcTypeInfo, error)
	getSQLInfoFunc        func(ctx context.Context, infoTypes []uint32) ([]models.SQLInfo, error)
}

func (m *mockMetadataRepo) GetCatalogs(ctx context.Context) ([]models.Catalog, error) {
	return m.getCatalogsFunc(ctx)
}

func (m *mockMetadataRepo) GetSchemas(ctx context.Context, catalog string, pattern string) ([]models.Schema, error) {
	return m.getSchemasFunc(ctx, catalog, pattern)
}

func (m *mockMetadataRepo) GetTables(ctx context.Context, opts models.GetTablesOptions) ([]models.Table, error) {
	return m.getTablesFunc(ctx, opts)
}

func (m *mockMetadataRepo) GetTableTypes(ctx context.Context) ([]string, error) {
	return m.getTableTypesFunc(ctx)
}

func (m *mockMetadataRepo) GetColumns(ctx context.Context, table models.TableRef) ([]models.Column, error) {
	return m.getColumnsFunc(ctx, table)
}

func (m *mockMetadataRepo) GetPrimaryKeys(ctx context.Context, table models.TableRef) ([]models.Key, error) {
	return m.getPrimaryKeysFunc(ctx, table)
}

func (m *mockMetadataRepo) GetImportedKeys(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error) {
	return m.getImportedKeysFunc(ctx, table)
}

func (m *mockMetadataRepo) GetExportedKeys(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error) {
	return m.getExportedKeysFunc(ctx, table)
}

func (m *mockMetadataRepo) GetCrossReference(ctx context.Context, ref models.CrossTableRef) ([]models.ForeignKey, error) {
	return m.getCrossReferenceFunc(ctx, ref)
}

func (m *mockMetadataRepo) GetTypeInfo(ctx context.Context, dataType *int32) ([]models.XdbcTypeInfo, error) {
	return m.getTypeInfoFunc(ctx, dataType)
}

func (m *mockMetadataRepo) GetSQLInfo(ctx context.Context, infoTypes []uint32) ([]models.SQLInfo, error) {
	return m.getSQLInfoFunc(ctx, infoTypes)
}

func setupTestMetadataService() (MetadataService, *mockMetadataRepo, *mockLogger, *mockMetricsCollector) {
	repo := &mockMetadataRepo{}
	logger := &mockLogger{}
	metrics := &mockMetricsCollector{}
	service := NewMetadataService(repo, logger, metrics)
	return service, repo, logger, metrics
}

func TestMetadataService_GetCatalogs(t *testing.T) {
	service, repo, _, _ := setupTestMetadataService()

	t.Run("successful get catalogs", func(t *testing.T) {
		// Setup mock response
		expectedCatalogs := []models.Catalog{
			{Name: "catalog1"},
			{Name: "catalog2"},
		}
		repo.getCatalogsFunc = func(ctx context.Context) ([]models.Catalog, error) {
			return expectedCatalogs, nil
		}

		// Test
		catalogs, err := service.GetCatalogs(context.Background())
		require.NoError(t, err)
		assert.Equal(t, expectedCatalogs, catalogs)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getCatalogsFunc = func(ctx context.Context) ([]models.Catalog, error) {
			return nil, assert.AnError
		}

		// Test
		catalogs, err := service.GetCatalogs(context.Background())
		assert.Error(t, err)
		assert.Nil(t, catalogs)
	})
}

func TestMetadataService_GetSchemas(t *testing.T) {
	service, repo, _, _ := setupTestMetadataService()

	t.Run("successful get schemas", func(t *testing.T) {
		// Setup mock response
		expectedSchemas := []models.Schema{
			{CatalogName: "catalog1", Name: "schema1"},
			{CatalogName: "catalog1", Name: "schema2"},
		}
		repo.getSchemasFunc = func(ctx context.Context, catalog string, pattern string) ([]models.Schema, error) {
			return expectedSchemas, nil
		}

		// Test
		schemas, err := service.GetSchemas(context.Background(), "catalog1", "schema%")
		require.NoError(t, err)
		assert.Equal(t, expectedSchemas, schemas)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getSchemasFunc = func(ctx context.Context, catalog string, pattern string) ([]models.Schema, error) {
			return nil, assert.AnError
		}

		// Test
		schemas, err := service.GetSchemas(context.Background(), "catalog1", "schema%")
		assert.Error(t, err)
		assert.Nil(t, schemas)
	})
}

func TestMetadataService_GetTables(t *testing.T) {
	service, repo, _, _ := setupTestMetadataService()

	t.Run("successful get tables", func(t *testing.T) {
		// Setup mock response
		expectedTables := []models.Table{
			{CatalogName: "catalog1", SchemaName: "schema1", Name: "table1", Type: "TABLE"},
			{CatalogName: "catalog1", SchemaName: "schema1", Name: "table2", Type: "TABLE"},
		}
		repo.getTablesFunc = func(ctx context.Context, opts models.GetTablesOptions) ([]models.Table, error) {
			return expectedTables, nil
		}

		// Test
		catalog := "catalog1"
		schemaPattern := "schema1"
		tablePattern := "table%"
		opts := models.GetTablesOptions{
			Catalog:                &catalog,
			SchemaFilterPattern:    &schemaPattern,
			TableNameFilterPattern: &tablePattern,
			TableTypes:             []string{"TABLE"},
			IncludeSchema:          true,
		}
		tables, err := service.GetTables(context.Background(), opts)
		require.NoError(t, err)
		assert.Equal(t, expectedTables, tables)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getTablesFunc = func(ctx context.Context, opts models.GetTablesOptions) ([]models.Table, error) {
			return nil, assert.AnError
		}

		// Test
		catalog := "catalog1"
		schemaPattern := "schema1"
		tablePattern := "table%"
		opts := models.GetTablesOptions{
			Catalog:                &catalog,
			SchemaFilterPattern:    &schemaPattern,
			TableNameFilterPattern: &tablePattern,
			TableTypes:             []string{"TABLE"},
			IncludeSchema:          true,
		}
		tables, err := service.GetTables(context.Background(), opts)
		assert.Error(t, err)
		assert.Nil(t, tables)
	})
}

func TestMetadataService_GetTableTypes(t *testing.T) {
	service, repo, _, _ := setupTestMetadataService()

	t.Run("successful get table types", func(t *testing.T) {
		// Setup mock response
		expectedTypes := []string{"TABLE", "VIEW"}
		repo.getTableTypesFunc = func(ctx context.Context) ([]string, error) {
			return expectedTypes, nil
		}

		// Test
		types, err := service.GetTableTypes(context.Background())
		require.NoError(t, err)
		assert.Equal(t, expectedTypes, types)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getTableTypesFunc = func(ctx context.Context) ([]string, error) {
			return nil, assert.AnError
		}

		// Test
		types, err := service.GetTableTypes(context.Background())
		assert.Error(t, err)
		assert.Nil(t, types)
	})
}

func TestMetadataService_GetColumns(t *testing.T) {
	service, repo, _, _ := setupTestMetadataService()

	t.Run("successful get columns", func(t *testing.T) {
		// Setup mock response
		expectedColumns := []models.Column{
			{Name: "col1", DataType: "INTEGER"},
			{Name: "col2", DataType: "VARCHAR"},
		}
		repo.getColumnsFunc = func(ctx context.Context, table models.TableRef) ([]models.Column, error) {
			return expectedColumns, nil
		}

		// Test
		catalog := "catalog1"
		schema := "schema1"
		table := models.TableRef{
			Catalog:  &catalog,
			DBSchema: &schema,
			Table:    "table1",
		}
		columns, err := service.GetColumns(context.Background(), table)
		require.NoError(t, err)
		assert.Equal(t, expectedColumns, columns)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getColumnsFunc = func(ctx context.Context, table models.TableRef) ([]models.Column, error) {
			return nil, assert.AnError
		}

		// Test
		catalog := "catalog1"
		schema := "schema1"
		table := models.TableRef{
			Catalog:  &catalog,
			DBSchema: &schema,
			Table:    "table1",
		}
		columns, err := service.GetColumns(context.Background(), table)
		assert.Error(t, err)
		assert.Nil(t, columns)
	})
}

func TestMetadataService_GetPrimaryKeys(t *testing.T) {
	service, repo, _, _ := setupTestMetadataService()

	t.Run("successful get primary keys", func(t *testing.T) {
		// Setup mock response
		expectedKeys := []models.Key{
			{
				CatalogName: "test_catalog",
				SchemaName:  "test_schema",
				TableName:   "test_table",
				ColumnName:  "id",
				KeySequence: 1,
				KeyName:     "pk_test_table",
			},
		}
		repo.getPrimaryKeysFunc = func(ctx context.Context, table models.TableRef) ([]models.Key, error) {
			return expectedKeys, nil
		}

		// Test
		catalog := "catalog1"
		schema := "schema1"
		table := models.TableRef{
			Catalog:  &catalog,
			DBSchema: &schema,
			Table:    "table1",
		}
		keys, err := service.GetPrimaryKeys(context.Background(), table)
		require.NoError(t, err)
		assert.Equal(t, expectedKeys, keys)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getPrimaryKeysFunc = func(ctx context.Context, table models.TableRef) ([]models.Key, error) {
			return nil, assert.AnError
		}

		// Test
		catalog := "catalog1"
		schema := "schema1"
		table := models.TableRef{
			Catalog:  &catalog,
			DBSchema: &schema,
			Table:    "table1",
		}
		keys, err := service.GetPrimaryKeys(context.Background(), table)
		assert.Error(t, err)
		assert.Nil(t, keys)
	})
}

func TestMetadataService_GetImportedKeys(t *testing.T) {
	service, repo, _, _ := setupTestMetadataService()

	t.Run("successful get imported keys", func(t *testing.T) {
		// Setup mock response
		expectedKeys := []models.ForeignKey{
			{
				PKCatalogName: "test_catalog",
				PKSchemaName:  "test_schema",
				PKTableName:   "parent_table",
				PKColumnName:  "id",
				FKCatalogName: "test_catalog",
				FKSchemaName:  "test_schema",
				FKTableName:   "test_table",
				FKColumnName:  "parent_id",
				KeySequence:   1,
				PKKeyName:     "pk_parent_table",
				FKKeyName:     "fk_test_table_parent",
				UpdateRule:    models.FKRuleCascade,
				DeleteRule:    models.FKRuleCascade,
			},
		}
		repo.getImportedKeysFunc = func(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error) {
			return expectedKeys, nil
		}

		// Test
		catalog := "catalog1"
		schema := "schema1"
		table := models.TableRef{
			Catalog:  &catalog,
			DBSchema: &schema,
			Table:    "table1",
		}
		keys, err := service.GetImportedKeys(context.Background(), table)
		require.NoError(t, err)
		assert.Equal(t, expectedKeys, keys)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getImportedKeysFunc = func(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error) {
			return nil, assert.AnError
		}

		// Test
		catalog := "catalog1"
		schema := "schema1"
		table := models.TableRef{
			Catalog:  &catalog,
			DBSchema: &schema,
			Table:    "table1",
		}
		keys, err := service.GetImportedKeys(context.Background(), table)
		assert.Error(t, err)
		assert.Nil(t, keys)
	})
}

func TestMetadataService_GetExportedKeys(t *testing.T) {
	service, repo, _, _ := setupTestMetadataService()

	t.Run("successful get exported keys", func(t *testing.T) {
		// Setup mock response
		expectedKeys := []models.ForeignKey{
			{
				PKCatalogName: "test_catalog",
				PKSchemaName:  "test_schema",
				PKTableName:   "test_table",
				PKColumnName:  "id",
				FKCatalogName: "test_catalog",
				FKSchemaName:  "test_schema",
				FKTableName:   "child_table",
				FKColumnName:  "test_id",
				KeySequence:   1,
				PKKeyName:     "pk_test_table",
				FKKeyName:     "fk_child_table_test",
				UpdateRule:    models.FKRuleCascade,
				DeleteRule:    models.FKRuleCascade,
			},
		}
		repo.getExportedKeysFunc = func(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error) {
			return expectedKeys, nil
		}

		// Test
		catalog := "catalog1"
		schema := "schema1"
		table := models.TableRef{
			Catalog:  &catalog,
			DBSchema: &schema,
			Table:    "table1",
		}
		keys, err := service.GetExportedKeys(context.Background(), table)
		require.NoError(t, err)
		assert.Equal(t, expectedKeys, keys)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getExportedKeysFunc = func(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error) {
			return nil, assert.AnError
		}

		// Test
		catalog := "catalog1"
		schema := "schema1"
		table := models.TableRef{
			Catalog:  &catalog,
			DBSchema: &schema,
			Table:    "table1",
		}
		keys, err := service.GetExportedKeys(context.Background(), table)
		assert.Error(t, err)
		assert.Nil(t, keys)
	})
}

func TestMetadataService_GetCrossReference(t *testing.T) {
	service, repo, _, _ := setupTestMetadataService()

	t.Run("successful get cross reference", func(t *testing.T) {
		// Setup mock response
		expectedKeys := []models.ForeignKey{
			{
				PKCatalogName: "test_catalog",
				PKSchemaName:  "test_schema",
				PKTableName:   "parent_table",
				PKColumnName:  "id",
				FKCatalogName: "test_catalog",
				FKSchemaName:  "test_schema",
				FKTableName:   "child_table",
				FKColumnName:  "parent_id",
				KeySequence:   1,
				PKKeyName:     "pk_parent_table",
				FKKeyName:     "fk_child_table_parent",
				UpdateRule:    models.FKRuleCascade,
				DeleteRule:    models.FKRuleCascade,
			},
		}
		repo.getCrossReferenceFunc = func(ctx context.Context, ref models.CrossTableRef) ([]models.ForeignKey, error) {
			return expectedKeys, nil
		}

		// Test
		catalog := "catalog1"
		schema := "schema1"
		ref := models.CrossTableRef{
			PKRef: models.TableRef{
				Catalog:  &catalog,
				DBSchema: &schema,
				Table:    "table1",
			},
			FKRef: models.TableRef{
				Catalog:  &catalog,
				DBSchema: &schema,
				Table:    "table2",
			},
		}
		keys, err := service.GetCrossReference(context.Background(), ref)
		require.NoError(t, err)
		assert.Equal(t, expectedKeys, keys)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getCrossReferenceFunc = func(ctx context.Context, ref models.CrossTableRef) ([]models.ForeignKey, error) {
			return nil, assert.AnError
		}

		// Test
		catalog := "catalog1"
		schema := "schema1"
		ref := models.CrossTableRef{
			PKRef: models.TableRef{
				Catalog:  &catalog,
				DBSchema: &schema,
				Table:    "table1",
			},
			FKRef: models.TableRef{
				Catalog:  &catalog,
				DBSchema: &schema,
				Table:    "table2",
			},
		}
		keys, err := service.GetCrossReference(context.Background(), ref)
		assert.Error(t, err)
		assert.Nil(t, keys)
	})
}

func TestMetadataService_GetTypeInfo(t *testing.T) {
	service, repo, _, _ := setupTestMetadataService()

	t.Run("successful get type info", func(t *testing.T) {
		// Setup mock response
		expectedTypes := []models.XdbcTypeInfo{
			{
				TypeName:          "INTEGER",
				DataType:          4,
				ColumnSize:        sql.NullInt32{Int32: 10, Valid: true},
				LiteralPrefix:     sql.NullString{Valid: false},
				LiteralSuffix:     sql.NullString{Valid: false},
				CreateParams:      sql.NullString{Valid: false},
				Nullable:          1,
				CaseSensitive:     false,
				Searchable:        3,
				UnsignedAttribute: sql.NullBool{Valid: false},
				FixedPrecScale:    true,
				AutoIncrement:     sql.NullBool{Valid: false},
				LocalTypeName:     sql.NullString{Valid: false},
				MinimumScale:      sql.NullInt32{Valid: false},
				MaximumScale:      sql.NullInt32{Valid: false},
				SQLDataType:       4,
				DatetimeSubcode:   sql.NullInt32{Valid: false},
				NumPrecRadix:      sql.NullInt32{Valid: false},
				IntervalPrecision: sql.NullInt32{Valid: false},
			},
		}
		repo.getTypeInfoFunc = func(ctx context.Context, dataType *int32) ([]models.XdbcTypeInfo, error) {
			return expectedTypes, nil
		}

		// Test
		var dataType int32 = 4
		types, err := service.GetTypeInfo(context.Background(), &dataType)
		require.NoError(t, err)
		assert.Equal(t, expectedTypes, types)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getTypeInfoFunc = func(ctx context.Context, dataType *int32) ([]models.XdbcTypeInfo, error) {
			return nil, assert.AnError
		}

		// Test
		var dataType int32 = 4
		types, err := service.GetTypeInfo(context.Background(), &dataType)
		assert.Error(t, err)
		assert.Nil(t, types)
	})
}

func TestMetadataService_GetSQLInfo(t *testing.T) {
	service, repo, _, _ := setupTestMetadataService()

	t.Run("successful get SQL info", func(t *testing.T) {
		// Setup mock response
		expectedInfo := []models.SQLInfo{
			{
				InfoName: 1,
				Value:    "test_value",
			},
		}
		repo.getSQLInfoFunc = func(ctx context.Context, infoTypes []uint32) ([]models.SQLInfo, error) {
			return expectedInfo, nil
		}

		// Test
		info, err := service.GetSQLInfo(context.Background(), []uint32{1})
		require.NoError(t, err)
		assert.Equal(t, expectedInfo, info)
	})

	t.Run("error handling", func(t *testing.T) {
		// Setup mock response
		repo.getSQLInfoFunc = func(ctx context.Context, infoTypes []uint32) ([]models.SQLInfo, error) {
			return nil, assert.AnError
		}

		// Test
		info, err := service.GetSQLInfo(context.Background(), []uint32{1})
		assert.Error(t, err)
		assert.Nil(t, info)
	})
}
