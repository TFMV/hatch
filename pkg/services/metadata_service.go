// Package services contains business logic implementations.
package services

import (
	"context"

	"github.com/TFMV/hatch/pkg/errors"
	"github.com/TFMV/hatch/pkg/models"
	"github.com/TFMV/hatch/pkg/repositories"
)

// metadataService implements MetadataService interface.
type metadataService struct {
	repo    repositories.MetadataRepository
	logger  Logger
	metrics MetricsCollector
}

// NewMetadataService creates a new metadata service.
func NewMetadataService(
	repo repositories.MetadataRepository,
	logger Logger,
	metrics MetricsCollector,
) MetadataService {
	return &metadataService{
		repo:    repo,
		logger:  logger,
		metrics: metrics,
	}
}

// GetCatalogs returns all available catalogs.
func (s *metadataService) GetCatalogs(ctx context.Context) ([]models.Catalog, error) {
	timer := s.metrics.StartTimer("metadata_get_catalogs")
	defer timer.Stop()

	s.logger.Debug("Getting catalogs")

	catalogs, err := s.repo.GetCatalogs(ctx)
	if err != nil {
		s.metrics.IncrementCounter("metadata_errors", "operation", "get_catalogs")
		s.logger.Error("Failed to get catalogs", "error", err)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get catalogs")
	}

	s.metrics.RecordGauge("catalog_count", float64(len(catalogs)))
	s.logger.Info("Retrieved catalogs", "count", len(catalogs))

	return catalogs, nil
}

// GetSchemas returns schemas matching the filter.
func (s *metadataService) GetSchemas(ctx context.Context, catalog string, pattern string) ([]models.Schema, error) {
	timer := s.metrics.StartTimer("metadata_get_schemas")
	defer timer.Stop()

	s.logger.Debug("Getting schemas", "catalog", catalog, "pattern", pattern)

	schemas, err := s.repo.GetSchemas(ctx, catalog, pattern)
	if err != nil {
		s.metrics.IncrementCounter("metadata_errors", "operation", "get_schemas")
		s.logger.Error("Failed to get schemas", "error", err, "catalog", catalog, "pattern", pattern)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get schemas")
	}

	s.metrics.RecordGauge("schema_count", float64(len(schemas)))
	s.logger.Info("Retrieved schemas", "count", len(schemas), "catalog", catalog)

	return schemas, nil
}

// GetTables returns tables matching the options.
func (s *metadataService) GetTables(ctx context.Context, opts models.GetTablesOptions) ([]models.Table, error) {
	timer := s.metrics.StartTimer("metadata_get_tables")
	defer timer.Stop()

	s.logger.Debug("Getting tables",
		"catalog", opts.Catalog,
		"schema_pattern", opts.SchemaFilterPattern,
		"table_pattern", opts.TableNameFilterPattern,
		"table_types", opts.TableTypes,
		"include_schema", opts.IncludeSchema)

	// Validate options
	if err := s.validateGetTablesOptions(opts); err != nil {
		s.metrics.IncrementCounter("metadata_validation_errors", "operation", "get_tables")
		return nil, err
	}

	tables, err := s.repo.GetTables(ctx, opts)
	if err != nil {
		s.metrics.IncrementCounter("metadata_errors", "operation", "get_tables")
		s.logger.Error("Failed to get tables", "error", err, "options", opts)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get tables")
	}

	s.metrics.RecordGauge("table_count", float64(len(tables)))
	s.logger.Info("Retrieved tables", "count", len(tables))

	return tables, nil
}

// GetTableTypes returns all available table types.
func (s *metadataService) GetTableTypes(ctx context.Context) ([]string, error) {
	timer := s.metrics.StartTimer("metadata_get_table_types")
	defer timer.Stop()

	s.logger.Debug("Getting table types")

	types, err := s.repo.GetTableTypes(ctx)
	if err != nil {
		s.metrics.IncrementCounter("metadata_errors", "operation", "get_table_types")
		s.logger.Error("Failed to get table types", "error", err)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get table types")
	}

	s.logger.Info("Retrieved table types", "count", len(types), "types", types)

	return types, nil
}

// GetColumns returns columns for a specific table.
func (s *metadataService) GetColumns(ctx context.Context, table models.TableRef) ([]models.Column, error) {
	timer := s.metrics.StartTimer("metadata_get_columns")
	defer timer.Stop()

	s.logger.Debug("Getting columns",
		"catalog", table.Catalog,
		"schema", table.DBSchema,
		"table", table.Table)

	// Validate table reference
	if err := s.validateTableRef(table); err != nil {
		s.metrics.IncrementCounter("metadata_validation_errors", "operation", "get_columns")
		return nil, err
	}

	columns, err := s.repo.GetColumns(ctx, table)
	if err != nil {
		s.metrics.IncrementCounter("metadata_errors", "operation", "get_columns")
		s.logger.Error("Failed to get columns", "error", err, "table", table)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get columns")
	}

	s.metrics.RecordGauge("column_count", float64(len(columns)))
	s.logger.Info("Retrieved columns", "count", len(columns), "table", table.Table)

	return columns, nil
}

// GetPrimaryKeys returns primary keys for a table.
func (s *metadataService) GetPrimaryKeys(ctx context.Context, table models.TableRef) ([]models.Key, error) {
	timer := s.metrics.StartTimer("metadata_get_primary_keys")
	defer timer.Stop()

	s.logger.Debug("Getting primary keys",
		"catalog", table.Catalog,
		"schema", table.DBSchema,
		"table", table.Table)

	// Validate table reference
	if err := s.validateTableRef(table); err != nil {
		s.metrics.IncrementCounter("metadata_validation_errors", "operation", "get_primary_keys")
		return nil, err
	}

	keys, err := s.repo.GetPrimaryKeys(ctx, table)
	if err != nil {
		s.metrics.IncrementCounter("metadata_errors", "operation", "get_primary_keys")
		s.logger.Error("Failed to get primary keys", "error", err, "table", table)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get primary keys")
	}

	s.logger.Info("Retrieved primary keys", "count", len(keys), "table", table.Table)

	return keys, nil
}

// GetImportedKeys returns foreign keys that reference a table.
func (s *metadataService) GetImportedKeys(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error) {
	timer := s.metrics.StartTimer("metadata_get_imported_keys")
	defer timer.Stop()

	s.logger.Debug("Getting imported keys",
		"catalog", table.Catalog,
		"schema", table.DBSchema,
		"table", table.Table)

	// Validate table reference
	if err := s.validateTableRef(table); err != nil {
		s.metrics.IncrementCounter("metadata_validation_errors", "operation", "get_imported_keys")
		return nil, err
	}

	keys, err := s.repo.GetImportedKeys(ctx, table)
	if err != nil {
		s.metrics.IncrementCounter("metadata_errors", "operation", "get_imported_keys")
		s.logger.Error("Failed to get imported keys", "error", err, "table", table)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get imported keys")
	}

	s.logger.Info("Retrieved imported keys", "count", len(keys), "table", table.Table)

	return keys, nil
}

// GetExportedKeys returns foreign keys from a table.
func (s *metadataService) GetExportedKeys(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error) {
	timer := s.metrics.StartTimer("metadata_get_exported_keys")
	defer timer.Stop()

	s.logger.Debug("Getting exported keys",
		"catalog", table.Catalog,
		"schema", table.DBSchema,
		"table", table.Table)

	// Validate table reference
	if err := s.validateTableRef(table); err != nil {
		s.metrics.IncrementCounter("metadata_validation_errors", "operation", "get_exported_keys")
		return nil, err
	}

	keys, err := s.repo.GetExportedKeys(ctx, table)
	if err != nil {
		s.metrics.IncrementCounter("metadata_errors", "operation", "get_exported_keys")
		s.logger.Error("Failed to get exported keys", "error", err, "table", table)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get exported keys")
	}

	s.logger.Info("Retrieved exported keys", "count", len(keys), "table", table.Table)

	return keys, nil
}

// GetCrossReference returns foreign key relationships between two tables.
func (s *metadataService) GetCrossReference(ctx context.Context, ref models.CrossTableRef) ([]models.ForeignKey, error) {
	timer := s.metrics.StartTimer("metadata_get_cross_reference")
	defer timer.Stop()

	s.logger.Debug("Getting cross reference",
		"pk_table", ref.PKRef.Table,
		"fk_table", ref.FKRef.Table)

	// Validate references
	if err := s.validateTableRef(ref.PKRef); err != nil {
		s.metrics.IncrementCounter("metadata_validation_errors", "operation", "get_cross_reference")
		return nil, errors.Wrap(err, errors.CodeInvalidRequest, "invalid primary key table reference")
	}

	if err := s.validateTableRef(ref.FKRef); err != nil {
		s.metrics.IncrementCounter("metadata_validation_errors", "operation", "get_cross_reference")
		return nil, errors.Wrap(err, errors.CodeInvalidRequest, "invalid foreign key table reference")
	}

	keys, err := s.repo.GetCrossReference(ctx, ref)
	if err != nil {
		s.metrics.IncrementCounter("metadata_errors", "operation", "get_cross_reference")
		s.logger.Error("Failed to get cross reference", "error", err, "ref", ref)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get cross reference")
	}

	s.logger.Info("Retrieved cross reference", "count", len(keys))

	return keys, nil
}

// GetTypeInfo returns database type information.
func (s *metadataService) GetTypeInfo(ctx context.Context, dataType *int32) ([]models.XdbcTypeInfo, error) {
	timer := s.metrics.StartTimer("metadata_get_type_info")
	defer timer.Stop()

	if dataType != nil {
		s.logger.Debug("Getting type info", "data_type", *dataType)
	} else {
		s.logger.Debug("Getting all type info")
	}

	types, err := s.repo.GetTypeInfo(ctx, dataType)
	if err != nil {
		s.metrics.IncrementCounter("metadata_errors", "operation", "get_type_info")
		s.logger.Error("Failed to get type info", "error", err)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get type info")
	}

	s.logger.Info("Retrieved type info", "count", len(types))

	return types, nil
}

// GetSQLInfo returns SQL feature information.
func (s *metadataService) GetSQLInfo(ctx context.Context, infoTypes []uint32) ([]models.SQLInfo, error) {
	timer := s.metrics.StartTimer("metadata_get_sql_info")
	defer timer.Stop()

	if len(infoTypes) > 0 {
		s.logger.Debug("Getting SQL info", "info_types", infoTypes)
	} else {
		s.logger.Debug("Getting all SQL info")
	}

	info, err := s.repo.GetSQLInfo(ctx, infoTypes)
	if err != nil {
		s.metrics.IncrementCounter("metadata_errors", "operation", "get_sql_info")
		s.logger.Error("Failed to get SQL info", "error", err)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get SQL info")
	}

	s.logger.Info("Retrieved SQL info", "count", len(info))

	return info, nil
}

// validateGetTablesOptions validates GetTables options.
func (s *metadataService) validateGetTablesOptions(opts models.GetTablesOptions) error {
	// Patterns can be empty (meaning no filter)
	// Table types can be empty (meaning all types)
	// No specific validation needed for now
	return nil
}

// validateTableRef validates a table reference.
func (s *metadataService) validateTableRef(ref models.TableRef) error {
	// Table name is required
	if ref.Table == "" {
		return errors.New(errors.CodeInvalidRequest, "table name is required")
	}

	// Catalog and schema can be empty (will use defaults)
	return nil
}
