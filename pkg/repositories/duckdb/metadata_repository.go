// Package duckdb provides DuckDB-specific repository implementations.
package duckdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"

	"github.com/TFMV/flight/pkg/errors"
	"github.com/TFMV/flight/pkg/infrastructure"
	"github.com/TFMV/flight/pkg/infrastructure/pool"
	"github.com/TFMV/flight/pkg/models"
	"github.com/TFMV/flight/pkg/repositories"
)

// metadataRepository implements repositories.MetadataRepository for DuckDB.
type metadataRepository struct {
	pool    pool.ConnectionPool
	sqlInfo *infrastructure.SQLInfoProvider
	logger  zerolog.Logger
}

// NewMetadataRepository creates a new DuckDB metadata repository.
func NewMetadataRepository(pool pool.ConnectionPool, sqlInfo *infrastructure.SQLInfoProvider, logger zerolog.Logger) repositories.MetadataRepository {
	return &metadataRepository{
		pool:    pool,
		sqlInfo: sqlInfo,
		logger:  logger,
	}
}

// GetCatalogs returns all available catalogs.
func (r *metadataRepository) GetCatalogs(ctx context.Context) ([]models.Catalog, error) {
	r.logger.Debug().Msg("Getting catalogs")

	_, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get database connection")
	}

	// DuckDB doesn't have multiple catalogs in the traditional sense
	// Return the default catalog
	catalogs := []models.Catalog{
		{
			Name:        "memory",
			Description: "DuckDB in-memory database",
		},
	}

	return catalogs, nil
}

// GetSchemas returns schemas matching the filter.
func (r *metadataRepository) GetSchemas(ctx context.Context, catalog string, pattern string) ([]models.Schema, error) {
	r.logger.Debug().
		Str("catalog", catalog).
		Str("pattern", pattern).
		Msg("Getting schemas")

	db, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get database connection")
	}

	query := `
		SELECT DISTINCT schema_name 
		FROM information_schema.schemata
		WHERE 1=1
	`

	args := []interface{}{}

	if pattern != "" && pattern != "%" {
		query += " AND schema_name LIKE ?"
		args = append(args, pattern)
	}

	query += " ORDER BY schema_name"

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		r.logger.Error().Err(err).Msg("Failed to query schemas")
		return nil, errors.Wrap(err, errors.CodeQueryFailed, "failed to query schemas")
	}
	defer rows.Close()

	var schemas []models.Schema
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return nil, errors.Wrap(err, errors.CodeInternal, "failed to scan schema row")
		}

		schemas = append(schemas, models.Schema{
			CatalogName: catalog,
			Name:        schemaName,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, errors.CodeInternal, "error iterating schema rows")
	}

	return schemas, nil
}

// GetTables returns tables matching the options.
func (r *metadataRepository) GetTables(ctx context.Context, opts models.GetTablesOptions) ([]models.Table, error) {
	r.logger.Debug().
		Interface("options", opts).
		Msg("Getting tables")

	db, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get database connection")
	}

	query := `
		SELECT 
			table_catalog,
			table_schema,
			table_name,
			table_type
		FROM information_schema.tables
		WHERE 1=1
	`

	args := []interface{}{}

	// Apply filters
	if opts.SchemaFilterPattern != nil && *opts.SchemaFilterPattern != "" && *opts.SchemaFilterPattern != "%" {
		query += " AND table_schema LIKE ?"
		args = append(args, *opts.SchemaFilterPattern)
	}

	if opts.TableNameFilterPattern != nil && *opts.TableNameFilterPattern != "" && *opts.TableNameFilterPattern != "%" {
		query += " AND table_name LIKE ?"
		args = append(args, *opts.TableNameFilterPattern)
	}

	if len(opts.TableTypes) > 0 {
		placeholders := make([]string, len(opts.TableTypes))
		for i, tableType := range opts.TableTypes {
			placeholders[i] = "?"
			args = append(args, tableType)
		}
		query += fmt.Sprintf(" AND table_type IN (%s)", strings.Join(placeholders, ","))
	}

	query += " ORDER BY table_schema, table_name"

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		r.logger.Error().Err(err).Msg("Failed to query tables")
		return nil, errors.Wrap(err, errors.CodeQueryFailed, "failed to query tables")
	}
	defer rows.Close()

	var tables []models.Table
	for rows.Next() {
		var table models.Table
		if err := rows.Scan(
			&table.CatalogName,
			&table.SchemaName,
			&table.Name,
			&table.Type,
		); err != nil {
			return nil, errors.Wrap(err, errors.CodeInternal, "failed to scan table row")
		}

		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, errors.CodeInternal, "error iterating table rows")
	}

	return tables, nil
}

// GetTableTypes returns all available table types.
func (r *metadataRepository) GetTableTypes(ctx context.Context) ([]string, error) {
	r.logger.Debug().Msg("Getting table types")

	// DuckDB supports these table types
	return []string{
		"BASE TABLE",
		"VIEW",
		"LOCAL TEMPORARY",
	}, nil
}

// GetColumns returns columns for a specific table.
func (r *metadataRepository) GetColumns(ctx context.Context, table models.TableRef) ([]models.Column, error) {
	r.logger.Debug().
		Interface("table", table).
		Msg("Getting columns")

	db, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get database connection")
	}

	query := `
		SELECT 
			table_catalog,
			table_schema,
			table_name,
			column_name,
			ordinal_position,
			column_default,
			is_nullable,
			data_type,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			datetime_precision
		FROM information_schema.columns
		WHERE table_name = ?
	`

	args := []interface{}{table.Table}

	if table.DBSchema != nil && *table.DBSchema != "" {
		query += " AND table_schema = ?"
		args = append(args, *table.DBSchema)
	}

	if table.Catalog != nil && *table.Catalog != "" {
		query += " AND table_catalog = ?"
		args = append(args, *table.Catalog)
	}

	query += " ORDER BY ordinal_position"

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		r.logger.Error().Err(err).Msg("Failed to query columns")
		return nil, errors.Wrap(err, errors.CodeQueryFailed, "failed to query columns")
	}
	defer rows.Close()

	var columns []models.Column
	for rows.Next() {
		var col models.Column
		var isNullableStr string

		if err := rows.Scan(
			&col.CatalogName,
			&col.SchemaName,
			&col.TableName,
			&col.Name,
			&col.OrdinalPosition,
			&col.DefaultValue,
			&isNullableStr,
			&col.DataType,
			&col.CharMaxLength,
			&col.NumericPrecision,
			&col.NumericScale,
			&col.DateTimePrecision,
		); err != nil {
			return nil, errors.Wrap(err, errors.CodeInternal, "failed to scan column row")
		}

		col.IsNullable = (isNullableStr == "YES")
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, errors.CodeInternal, "error iterating column rows")
	}

	return columns, nil
}

// GetPrimaryKeys returns primary keys for a table.
func (r *metadataRepository) GetPrimaryKeys(ctx context.Context, table models.TableRef) ([]models.Key, error) {
	r.logger.Debug().
		Interface("table", table).
		Msg("Getting primary keys")

	_, err := r.pool.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeConnectionFailed, "failed to get database connection")
	}

	// DuckDB doesn't expose primary keys through information_schema yet
	// This would need to be implemented using DuckDB's internal tables
	// For now, return empty result
	return []models.Key{}, nil
}

// GetImportedKeys returns foreign keys that reference a table.
func (r *metadataRepository) GetImportedKeys(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error) {
	r.logger.Debug().
		Interface("table", table).
		Msg("Getting imported keys")

	// DuckDB doesn't expose foreign keys through information_schema yet
	// This would need to be implemented using DuckDB's internal tables
	// For now, return empty result
	return []models.ForeignKey{}, nil
}

// GetExportedKeys returns foreign keys from a table.
func (r *metadataRepository) GetExportedKeys(ctx context.Context, table models.TableRef) ([]models.ForeignKey, error) {
	r.logger.Debug().
		Interface("table", table).
		Msg("Getting exported keys")

	// DuckDB doesn't expose foreign keys through information_schema yet
	// This would need to be implemented using DuckDB's internal tables
	// For now, return empty result
	return []models.ForeignKey{}, nil
}

// GetCrossReference returns foreign key relationships between two tables.
func (r *metadataRepository) GetCrossReference(ctx context.Context, ref models.CrossTableRef) ([]models.ForeignKey, error) {
	r.logger.Debug().
		Interface("ref", ref).
		Msg("Getting cross reference")

	// DuckDB doesn't expose foreign keys through information_schema yet
	// This would need to be implemented using DuckDB's internal tables
	// For now, return empty result
	return []models.ForeignKey{}, nil
}

// GetTypeInfo returns database type information.
func (r *metadataRepository) GetTypeInfo(ctx context.Context, dataType *int32) ([]models.XdbcTypeInfo, error) {
	r.logger.Debug().Msg("Getting type info")

	// For now, return empty result as we need to implement XdbcTypeInfoProvider
	// TODO: Implement proper type info provider
	return []models.XdbcTypeInfo{}, nil
}

// GetSQLInfo returns SQL feature information.
func (r *metadataRepository) GetSQLInfo(ctx context.Context, infoTypes []uint32) ([]models.SQLInfo, error) {
	r.logger.Debug().
		Interface("info_types", infoTypes).
		Msg("Getting SQL info")

	// Get SQL info from the provider
	sqlInfo, err := r.sqlInfo.GetSQLInfo(infoTypes)
	if err != nil {
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get SQL info")
	}

	return sqlInfo, nil
}
