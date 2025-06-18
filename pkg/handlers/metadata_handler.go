// Package handlers contains Flight SQL protocol handlers.
package handlers

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/TFMV/hatch/pkg/models"
	"github.com/TFMV/hatch/pkg/services"
)

// MetadataHandler handles metadata-related operations.
type metadataHandler struct {
	metadataService services.MetadataService
	allocator       memory.Allocator
	logger          Logger
	metrics         MetricsCollector
}

// NewMetadataHandler creates a new metadata handler.
func NewMetadataHandler(
	metadataService services.MetadataService,
	allocator memory.Allocator,
	logger Logger,
	metrics MetricsCollector,
) MetadataHandler {
	return &metadataHandler{
		metadataService: metadataService,
		allocator:       allocator,
		logger:          logger,
		metrics:         metrics,
	}
}

// GetCatalogs returns available catalogs.
func (h *metadataHandler) GetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_get_catalogs")
	defer timer.Stop()

	h.logger.Debug("Getting catalogs")

	catalogs, err := h.metadataService.GetCatalogs(ctx)
	if err != nil {
		h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_catalogs")
		return nil, nil, fmt.Errorf("failed to get catalogs: %w", err)
	}

	// Create Arrow schema for catalogs
	schema := models.GetCatalogsSchema()

	// Create stream for results
	chunks := make(chan flight.StreamChunk, 1)

	go func() {
		defer close(chunks)

		builder := array.NewRecordBuilder(h.allocator, schema)
		defer builder.Release()

		// Build records from catalogs
		for _, catalog := range catalogs {
			builder.Field(0).(*array.StringBuilder).Append(catalog.Name)
		}

		record := builder.NewRecord()
		chunks <- flight.StreamChunk{Data: record}

		h.logger.Info("Catalogs retrieved", "count", len(catalogs))
		h.metrics.RecordHistogram("handler_catalogs_count", float64(len(catalogs)))
	}()

	return schema, chunks, nil
}

// GetSchemas returns schemas matching the filter.
func (h *metadataHandler) GetSchemas(ctx context.Context, catalog *string, schemaPattern *string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_get_schemas")
	defer timer.Stop()

	h.logger.Debug("Getting schemas", "catalog", catalog, "pattern", schemaPattern)

	// Convert pointers to values for service call
	var catalogValue, patternValue string
	if catalog != nil {
		catalogValue = *catalog
	}
	if schemaPattern != nil {
		patternValue = *schemaPattern
	}

	schemas, err := h.metadataService.GetSchemas(ctx, catalogValue, patternValue)
	if err != nil {
		h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_schemas")
		return nil, nil, fmt.Errorf("failed to get schemas: %w", err)
	}

	// Create Arrow schema for schemas
	schema := models.GetDBSchemasSchema()

	// Create stream for results
	chunks := make(chan flight.StreamChunk, 1)

	go func() {
		defer close(chunks)

		builder := array.NewRecordBuilder(h.allocator, schema)
		defer builder.Release()

		// Build records from schemas
		for _, s := range schemas {
			// catalog_name
			if s.CatalogName != "" {
				builder.Field(0).(*array.StringBuilder).Append(s.CatalogName)
			} else {
				builder.Field(0).AppendNull()
			}
			// db_schema_name
			builder.Field(1).(*array.StringBuilder).Append(s.Name)
		}

		record := builder.NewRecord()
		chunks <- flight.StreamChunk{Data: record}

		h.logger.Info("Schemas retrieved", "count", len(schemas))
		h.metrics.RecordHistogram("handler_schemas_count", float64(len(schemas)))
	}()

	return schema, chunks, nil
}

// GetTables returns tables matching the filter.
func (h *metadataHandler) GetTables(ctx context.Context, catalog *string, schemaPattern *string, tablePattern *string, tableTypes []string, includeSchema bool) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_get_tables")
	defer timer.Stop()

	h.logger.Debug("Getting tables",
		"catalog", catalog,
		"schema_pattern", schemaPattern,
		"table_pattern", tablePattern,
		"table_types", tableTypes,
		"include_schema", includeSchema)

	opts := models.GetTablesOptions{
		Catalog:                catalog,
		SchemaFilterPattern:    schemaPattern,
		TableNameFilterPattern: tablePattern,
		TableTypes:             tableTypes,
		IncludeSchema:          includeSchema,
	}

	tables, err := h.metadataService.GetTables(ctx, opts)
	if err != nil {
		h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_tables")
		return nil, nil, fmt.Errorf("failed to get tables: %w", err)
	}

	// Create Arrow schema for tables
	schema := models.GetTablesSchema(includeSchema)

	// Create stream for results
	chunks := make(chan flight.StreamChunk, 1)

	go func() {
		defer close(chunks)

		builder := array.NewRecordBuilder(h.allocator, schema)
		defer builder.Release()

		// Build records from tables
		for _, table := range tables {
			fieldIdx := 0

			// catalog_name
			if table.CatalogName != "" {
				builder.Field(fieldIdx).(*array.StringBuilder).Append(table.CatalogName)
			} else {
				builder.Field(fieldIdx).AppendNull()
			}
			fieldIdx++

			// db_schema_name
			builder.Field(fieldIdx).(*array.StringBuilder).Append(table.SchemaName)
			fieldIdx++

			// table_name
			builder.Field(fieldIdx).(*array.StringBuilder).Append(table.Name)
			fieldIdx++

			// table_type
			builder.Field(fieldIdx).(*array.StringBuilder).Append(table.Type)
			fieldIdx++

			// table_schema (if requested)
			if includeSchema {
				ref := models.TableRef{
					Catalog:  &table.CatalogName,
					DBSchema: &table.SchemaName,
					Table:    table.Name,
				}
				tblSchema, err := h.metadataService.GetTableSchema(ctx, ref)
				if err != nil || tblSchema == nil {
					builder.Field(fieldIdx).(*array.BinaryBuilder).AppendNull()
				} else {
					serialized := flight.SerializeSchema(tblSchema, h.allocator)
					builder.Field(fieldIdx).(*array.BinaryBuilder).Append(serialized)
				}
			}
		}

		record := builder.NewRecord()
		chunks <- flight.StreamChunk{Data: record}

		h.logger.Info("Tables retrieved", "count", len(tables))
		h.metrics.RecordHistogram("handler_tables_count", float64(len(tables)))
	}()

	return schema, chunks, nil
}

// GetColumns returns columns matching the provided patterns.
func (h *metadataHandler) GetColumns(ctx context.Context, catalog *string, schemaPattern *string, tablePattern *string, columnPattern *string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_get_columns")
	defer timer.Stop()

	h.logger.Debug("Getting columns",
		"catalog", catalog,
		"schema_pattern", schemaPattern,
		"table_pattern", tablePattern,
		"column_pattern", columnPattern,
	)

	// ── resolve tables ─────────────────────────────────────────────
	var tables []models.TableRef
	if tablePattern == nil || *tablePattern == "" || strings.ContainsAny(*tablePattern, "%_") {
		opts := models.GetTablesOptions{
			Catalog:                catalog,
			SchemaFilterPattern:    schemaPattern,
			TableNameFilterPattern: tablePattern,
			IncludeSchema:          false,
		}
		tbls, err := h.metadataService.GetTables(ctx, opts)
		if err != nil {
			h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_columns")
			return nil, nil, fmt.Errorf("failed to resolve tables: %w", err)
		}
		for _, t := range tbls {
			c := t.CatalogName
			s := t.SchemaName
			ref := models.TableRef{Catalog: &c, DBSchema: &s, Table: t.Name}
			tables = append(tables, ref)
		}
	} else {
		tables = append(tables, models.TableRef{Catalog: catalog, DBSchema: schemaPattern, Table: *tablePattern})
	}

	// ── gather columns ─────────────────────────────────────────────
	var cols []models.Column
	for _, ref := range tables {
		c, err := h.metadataService.GetColumns(ctx, ref)
		if err != nil {
			h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_columns")
			return nil, nil, fmt.Errorf("failed to get columns: %w", err)
		}
		cols = append(cols, c...)
	}

	// ── filter column names ───────────────────────────────────────
	if columnPattern != nil && *columnPattern != "" && *columnPattern != "%" {
		pat := regexp.QuoteMeta(*columnPattern)
		pat = strings.ReplaceAll(pat, "\\%", ".*")
		pat = strings.ReplaceAll(pat, "\\_", ".")
		re := regexp.MustCompile("^" + pat + "$")
		filtered := cols[:0]
		for _, c := range cols {
			if re.MatchString(c.Name) {
				filtered = append(filtered, c)
			}
		}
		cols = filtered
	}

	schema := models.GetColumnsSchema()
	ch := make(chan flight.StreamChunk, 1)

	go func() {
		defer close(ch)

		b := array.NewRecordBuilder(h.allocator, schema)
		defer b.Release()

		for _, col := range cols {
			if col.CatalogName != "" {
				b.Field(0).(*array.StringBuilder).Append(col.CatalogName)
			} else {
				b.Field(0).AppendNull()
			}
			b.Field(1).(*array.StringBuilder).Append(col.SchemaName)
			b.Field(2).(*array.StringBuilder).Append(col.TableName)
			b.Field(3).(*array.StringBuilder).Append(col.Name)
			b.Field(4).(*array.Int32Builder).Append(int32(col.OrdinalPosition))
			if col.DefaultValue.Valid {
				b.Field(5).(*array.StringBuilder).Append(col.DefaultValue.String)
			} else {
				b.Field(5).AppendNull()
			}
			b.Field(6).(*array.BooleanBuilder).Append(col.IsNullable)
			b.Field(7).(*array.StringBuilder).Append(col.DataType)
			if col.CharMaxLength.Valid {
				b.Field(8).(*array.Int64Builder).Append(col.CharMaxLength.Int64)
			} else {
				b.Field(8).AppendNull()
			}
			if col.NumericPrecision.Valid {
				b.Field(9).(*array.Int64Builder).Append(col.NumericPrecision.Int64)
			} else {
				b.Field(9).AppendNull()
			}
			if col.NumericScale.Valid {
				b.Field(10).(*array.Int64Builder).Append(col.NumericScale.Int64)
			} else {
				b.Field(10).AppendNull()
			}
			if col.DateTimePrecision.Valid {
				b.Field(11).(*array.Int64Builder).Append(col.DateTimePrecision.Int64)
			} else {
				b.Field(11).AppendNull()
			}
		}

		rec := b.NewRecord()
		ch <- flight.StreamChunk{Data: rec}

		h.logger.Info("Columns retrieved", "count", len(cols))
		h.metrics.RecordHistogram("handler_columns_count", float64(len(cols)))
	}()

	return schema, ch, nil
}

// GetTableTypes returns available table types.
func (h *metadataHandler) GetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_get_table_types")
	defer timer.Stop()

	h.logger.Debug("Getting table types")

	tableTypes, err := h.metadataService.GetTableTypes(ctx)
	if err != nil {
		h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_table_types")
		return nil, nil, fmt.Errorf("failed to get table types: %w", err)
	}

	// Create Arrow schema for table types
	schema := models.GetTableTypesSchema()

	// Create stream for results
	chunks := make(chan flight.StreamChunk, 1)

	go func() {
		defer close(chunks)

		builder := array.NewRecordBuilder(h.allocator, schema)
		defer builder.Release()

		// Build records from table types
		for _, tableType := range tableTypes {
			builder.Field(0).(*array.StringBuilder).Append(tableType)
		}

		record := builder.NewRecord()
		chunks <- flight.StreamChunk{Data: record}

		h.logger.Info("Table types retrieved", "count", len(tableTypes))
		h.metrics.RecordHistogram("handler_table_types_count", float64(len(tableTypes)))
	}()

	return schema, chunks, nil
}

// GetPrimaryKeys returns primary keys for a table.
func (h *metadataHandler) GetPrimaryKeys(ctx context.Context, catalog *string, schema *string, table string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_get_primary_keys")
	defer timer.Stop()

	h.logger.Debug("Getting primary keys",
		"catalog", catalog,
		"schema", schema,
		"table", table)

	tableRef := models.TableRef{
		Catalog:  catalog,
		DBSchema: schema,
		Table:    table,
	}

	keys, err := h.metadataService.GetPrimaryKeys(ctx, tableRef)
	if err != nil {
		h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_primary_keys")
		return nil, nil, fmt.Errorf("failed to get primary keys: %w", err)
	}

	// Create Arrow schema for primary keys
	arrowSchema := models.GetPrimaryKeysSchema()

	// Create stream for results
	chunks := make(chan flight.StreamChunk, 1)

	go func() {
		defer close(chunks)

		builder := array.NewRecordBuilder(h.allocator, arrowSchema)
		defer builder.Release()

		// Build records from keys
		for _, key := range keys {
			// catalog_name
			if key.CatalogName != "" {
				builder.Field(0).(*array.StringBuilder).Append(key.CatalogName)
			} else {
				builder.Field(0).AppendNull()
			}

			// db_schema_name
			builder.Field(1).(*array.StringBuilder).Append(key.SchemaName)

			// table_name
			builder.Field(2).(*array.StringBuilder).Append(key.TableName)

			// column_name
			builder.Field(3).(*array.StringBuilder).Append(key.ColumnName)

			// key_sequence
			builder.Field(4).(*array.Int32Builder).Append(key.KeySequence)

			// key_name
			if key.KeyName != "" {
				builder.Field(5).(*array.StringBuilder).Append(key.KeyName)
			} else {
				builder.Field(5).AppendNull()
			}
		}

		record := builder.NewRecord()
		chunks <- flight.StreamChunk{Data: record}

		h.logger.Info("Primary keys retrieved", "count", len(keys))
		h.metrics.RecordHistogram("handler_primary_keys_count", float64(len(keys)))
	}()

	return arrowSchema, chunks, nil
}

// GetImportedKeys returns imported foreign keys for a table.
func (h *metadataHandler) GetImportedKeys(ctx context.Context, catalog *string, schema *string, table string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_get_imported_keys")
	defer timer.Stop()

	h.logger.Debug("Getting imported keys",
		"catalog", catalog,
		"schema", schema,
		"table", table)

	tableRef := models.TableRef{
		Catalog:  catalog,
		DBSchema: schema,
		Table:    table,
	}

	keys, err := h.metadataService.GetImportedKeys(ctx, tableRef)
	if err != nil {
		h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_imported_keys")
		return nil, nil, fmt.Errorf("failed to get imported keys: %w", err)
	}

	// Create Arrow schema for foreign keys
	arrowSchema := models.GetImportedKeysSchema()

	// Create stream for results
	chunks := make(chan flight.StreamChunk, 1)

	go func() {
		defer close(chunks)

		record := h.createForeignKeyRecord(arrowSchema, keys)
		chunks <- flight.StreamChunk{Data: record}

		h.logger.Info("Imported keys retrieved", "count", len(keys))
		h.metrics.RecordHistogram("handler_imported_keys_count", float64(len(keys)))
	}()

	return arrowSchema, chunks, nil
}

// GetExportedKeys returns exported foreign keys for a table.
func (h *metadataHandler) GetExportedKeys(ctx context.Context, catalog *string, schema *string, table string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_get_exported_keys")
	defer timer.Stop()

	h.logger.Debug("Getting exported keys",
		"catalog", catalog,
		"schema", schema,
		"table", table)

	tableRef := models.TableRef{
		Catalog:  catalog,
		DBSchema: schema,
		Table:    table,
	}

	keys, err := h.metadataService.GetExportedKeys(ctx, tableRef)
	if err != nil {
		h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_exported_keys")
		return nil, nil, fmt.Errorf("failed to get exported keys: %w", err)
	}

	// Create Arrow schema for foreign keys
	arrowSchema := models.GetExportedKeysSchema()

	// Create stream for results
	chunks := make(chan flight.StreamChunk, 1)

	go func() {
		defer close(chunks)

		record := h.createForeignKeyRecord(arrowSchema, keys)
		chunks <- flight.StreamChunk{Data: record}

		h.logger.Info("Exported keys retrieved", "count", len(keys))
		h.metrics.RecordHistogram("handler_exported_keys_count", float64(len(keys)))
	}()

	return arrowSchema, chunks, nil
}

// GetCrossReference returns foreign key relationships between two tables.
func (h *metadataHandler) GetCrossReference(
	ctx context.Context,
	pkCatalog *string, pkSchema *string, pkTable string,
	fkCatalog *string, fkSchema *string, fkTable string,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_get_cross_reference")
	defer timer.Stop()

	h.logger.Debug("Getting cross reference",
		"pk_catalog", pkCatalog,
		"pk_schema", pkSchema,
		"pk_table", pkTable,
		"fk_catalog", fkCatalog,
		"fk_schema", fkSchema,
		"fk_table", fkTable,
	)

	ref := models.CrossTableRef{
		PKRef: models.TableRef{Catalog: pkCatalog, DBSchema: pkSchema, Table: pkTable},
		FKRef: models.TableRef{Catalog: fkCatalog, DBSchema: fkSchema, Table: fkTable},
	}

	keys, err := h.metadataService.GetCrossReference(ctx, ref)
	if err != nil {
		h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_cross_reference")
		return nil, nil, fmt.Errorf("failed to get cross reference: %w", err)
	}

	schema := models.GetForeignKeysSchema()
	ch := make(chan flight.StreamChunk, 1)

	go func() {
		defer close(ch)
		rec := h.createForeignKeyRecord(schema, keys)
		ch <- flight.StreamChunk{Data: rec}

		h.logger.Info("Cross reference retrieved", "count", len(keys))
		h.metrics.RecordHistogram("handler_cross_reference_count", float64(len(keys)))
	}()

	return schema, ch, nil
}

// GetXdbcTypeInfo returns XDBC type information.
func (h *metadataHandler) GetXdbcTypeInfo(ctx context.Context, dataType *int32) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_get_xdbc_type_info")
	defer timer.Stop()

	h.logger.Debug("Getting XDBC type info", "data_type", dataType)

	typeInfoList, err := h.metadataService.GetTypeInfo(ctx, dataType)
	if err != nil {
		h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_xdbc_type_info")
		return nil, nil, fmt.Errorf("failed to get XDBC type info: %w", err)
	}

	// Create Arrow schema for type info
	schema := models.GetXdbcTypeInfoSchema()

	// Create stream for results
	chunks := make(chan flight.StreamChunk, 1)

	go func() {
		defer close(chunks)

		typeInfoResult := &models.XdbcTypeInfoResult{Types: typeInfoList}
		record := typeInfoResult.ToArrowRecord(h.allocator)
		chunks <- flight.StreamChunk{Data: record}

		h.logger.Info("XDBC type info retrieved")
		h.metrics.IncrementCounter("handler_xdbc_type_info_retrieved")
	}()

	return schema, chunks, nil
}

// GetSqlInfo returns SQL information.
func (h *metadataHandler) GetSqlInfo(ctx context.Context, info []uint32) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := h.metrics.StartTimer("handler_get_sql_info")
	defer timer.Stop()

	h.logger.Debug("Getting SQL info", "info_count", len(info))

	values, err := h.metadataService.GetSQLInfo(ctx, info)
	if err != nil {
		h.metrics.IncrementCounter("handler_metadata_errors", "operation", "get_sql_info")
		return nil, nil, fmt.Errorf("failed to get SQL info: %w", err)
	}

	schema := models.GetSqlInfoSchema()
	ch := make(chan flight.StreamChunk, 1)

	go func() {
		defer close(ch)

		result := &models.SqlInfoResult{Info: values}
		rec := result.ToArrowRecord(h.allocator)
		ch <- flight.StreamChunk{Data: rec}

		h.logger.Info("SQL info retrieved", "count", len(values))
		h.metrics.IncrementCounter("handler_sql_info_retrieved")
	}()

	return schema, ch, nil
}

// createForeignKeyRecord creates an Arrow record from foreign keys.
func (h *metadataHandler) createForeignKeyRecord(schema *arrow.Schema, keys []models.ForeignKey) arrow.Record {
	builder := array.NewRecordBuilder(h.allocator, schema)
	defer builder.Release()

	for _, key := range keys {
		// pk_catalog_name
		if key.PKCatalogName != "" {
			builder.Field(0).(*array.StringBuilder).Append(key.PKCatalogName)
		} else {
			builder.Field(0).AppendNull()
		}

		// pk_db_schema_name
		builder.Field(1).(*array.StringBuilder).Append(key.PKSchemaName)

		// pk_table_name
		builder.Field(2).(*array.StringBuilder).Append(key.PKTableName)

		// pk_column_name
		builder.Field(3).(*array.StringBuilder).Append(key.PKColumnName)

		// fk_catalog_name
		if key.FKCatalogName != "" {
			builder.Field(4).(*array.StringBuilder).Append(key.FKCatalogName)
		} else {
			builder.Field(4).AppendNull()
		}

		// fk_db_schema_name
		builder.Field(5).(*array.StringBuilder).Append(key.FKSchemaName)

		// fk_table_name
		builder.Field(6).(*array.StringBuilder).Append(key.FKTableName)

		// fk_column_name
		builder.Field(7).(*array.StringBuilder).Append(key.FKColumnName)

		// key_sequence
		builder.Field(8).(*array.Int32Builder).Append(key.KeySequence)

		// fk_key_name
		if key.FKKeyName != "" {
			builder.Field(9).(*array.StringBuilder).Append(key.FKKeyName)
		} else {
			builder.Field(9).AppendNull()
		}

		// pk_key_name
		if key.PKKeyName != "" {
			builder.Field(10).(*array.StringBuilder).Append(key.PKKeyName)
		} else {
			builder.Field(10).AppendNull()
		}

		// update_rule
		builder.Field(11).(*array.Uint8Builder).Append(uint8(key.UpdateRule))

		// delete_rule
		builder.Field(12).(*array.Uint8Builder).Append(uint8(key.DeleteRule))
	}

	return builder.NewRecord()
}
