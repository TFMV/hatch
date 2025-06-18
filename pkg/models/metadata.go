// Package models provides data structures used throughout the Flight SQL server.
package models

import (
	"database/sql"
	"time"
)

// Catalog represents a database catalog.
type Catalog struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Properties  map[string]interface{} `json:"properties,omitempty"`
}

// Schema represents a database schema.
type Schema struct {
	CatalogName string                 `json:"catalog_name"`
	Name        string                 `json:"name"`
	Owner       string                 `json:"owner,omitempty"`
	Description string                 `json:"description,omitempty"`
	Properties  map[string]interface{} `json:"properties,omitempty"`
}

// Table represents a database table.
type Table struct {
	CatalogName string                 `json:"catalog_name"`
	SchemaName  string                 `json:"schema_name"`
	Name        string                 `json:"name"`
	Type        string                 `json:"type"`
	Description string                 `json:"description,omitempty"`
	Owner       string                 `json:"owner,omitempty"`
	CreatedAt   *time.Time             `json:"created_at,omitempty"`
	UpdatedAt   *time.Time             `json:"updated_at,omitempty"`
	RowCount    *int64                 `json:"row_count,omitempty"`
	Properties  map[string]interface{} `json:"properties,omitempty"`
}

// TableRef represents a reference to a table.
type TableRef struct {
	Catalog  *string `json:"catalog,omitempty"`
	DBSchema *string `json:"db_schema,omitempty"`
	Table    string  `json:"table"`
}

// GetTablesOptions represents options for getting tables.
type GetTablesOptions struct {
	Catalog                *string  `json:"catalog,omitempty"`
	SchemaFilterPattern    *string  `json:"schema_filter_pattern,omitempty"`
	TableNameFilterPattern *string  `json:"table_name_filter_pattern,omitempty"`
	TableTypes             []string `json:"table_types,omitempty"`
	IncludeSchema          bool     `json:"include_schema,omitempty"`
}

// Column represents a database column.
type Column struct {
	CatalogName       string         `json:"catalog_name"`
	SchemaName        string         `json:"schema_name"`
	TableName         string         `json:"table_name"`
	Name              string         `json:"name"`
	OrdinalPosition   int            `json:"ordinal_position"`
	DataType          string         `json:"data_type"`
	IsNullable        bool           `json:"is_nullable"`
	DefaultValue      sql.NullString `json:"default_value,omitempty"`
	CharMaxLength     sql.NullInt64  `json:"char_max_length,omitempty"`
	NumericPrecision  sql.NullInt64  `json:"numeric_precision,omitempty"`
	NumericScale      sql.NullInt64  `json:"numeric_scale,omitempty"`
	DateTimePrecision sql.NullInt64  `json:"datetime_precision,omitempty"`
	CharSet           sql.NullString `json:"char_set,omitempty"`
	Collation         sql.NullString `json:"collation,omitempty"`
	Comment           sql.NullString `json:"comment,omitempty"`
}

// Key represents a primary key.
type Key struct {
	CatalogName string `json:"catalog_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	ColumnName  string `json:"column_name"`
	KeySequence int32  `json:"key_sequence"`
	KeyName     string `json:"key_name,omitempty"`
}

// ForeignKey represents a foreign key relationship.
type ForeignKey struct {
	PKCatalogName string `json:"pk_catalog_name"`
	PKSchemaName  string `json:"pk_schema_name"`
	PKTableName   string `json:"pk_table_name"`
	PKColumnName  string `json:"pk_column_name"`
	FKCatalogName string `json:"fk_catalog_name"`
	FKSchemaName  string `json:"fk_schema_name"`
	FKTableName   string `json:"fk_table_name"`
	FKColumnName  string `json:"fk_column_name"`
	KeySequence   int32  `json:"key_sequence"`
	PKKeyName     string `json:"pk_key_name,omitempty"`
	FKKeyName     string `json:"fk_key_name,omitempty"`
	UpdateRule    FKRule `json:"update_rule"`
	DeleteRule    FKRule `json:"delete_rule"`
}

// FKRule represents a foreign key update/delete rule.
type FKRule int32

const (
	// FKRuleCascade indicates CASCADE rule.
	FKRuleCascade FKRule = 0
	// FKRuleRestrict indicates RESTRICT rule.
	FKRuleRestrict FKRule = 1
	// FKRuleSetNull indicates SET NULL rule.
	FKRuleSetNull FKRule = 2
	// FKRuleNoAction indicates NO ACTION rule.
	FKRuleNoAction FKRule = 3
	// FKRuleSetDefault indicates SET DEFAULT rule.
	FKRuleSetDefault FKRule = 4
)

// CrossTableRef represents a cross-table reference for foreign key lookups.
type CrossTableRef struct {
	PKRef TableRef `json:"pk_ref"`
	FKRef TableRef `json:"fk_ref"`
}

// SQLInfo represents SQL feature information.
type SQLInfo struct {
	InfoName uint32      `json:"info_name"`
	Value    interface{} `json:"value"`
}

// XdbcTypeInfo represents XDBC type information.
type XdbcTypeInfo struct {
	TypeName          string         `json:"type_name"`
	DataType          int32          `json:"data_type"`
	ColumnSize        sql.NullInt32  `json:"column_size"`
	LiteralPrefix     sql.NullString `json:"literal_prefix"`
	LiteralSuffix     sql.NullString `json:"literal_suffix"`
	CreateParams      sql.NullString `json:"create_params"`
	Nullable          int32          `json:"nullable"`
	CaseSensitive     bool           `json:"case_sensitive"`
	Searchable        int32          `json:"searchable"`
	UnsignedAttribute sql.NullBool   `json:"unsigned_attribute"`
	FixedPrecScale    bool           `json:"fixed_prec_scale"`
	AutoIncrement     sql.NullBool   `json:"auto_increment"`
	LocalTypeName     sql.NullString `json:"local_type_name"`
	MinimumScale      sql.NullInt32  `json:"minimum_scale"`
	MaximumScale      sql.NullInt32  `json:"maximum_scale"`
	SQLDataType       int32          `json:"sql_data_type"`
	DatetimeSubcode   sql.NullInt32  `json:"datetime_subcode"`
	NumPrecRadix      sql.NullInt32  `json:"num_prec_radix"`
	IntervalPrecision sql.NullInt32  `json:"interval_precision"`
}

// TODO: add statistics model when index metadata is supported
