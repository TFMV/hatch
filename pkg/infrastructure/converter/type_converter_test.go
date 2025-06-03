package converter

import (
	"database/sql"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypeConverter(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	tc := New(logger)

	t.Run("DuckDBToArrowType", func(t *testing.T) {
		tests := []struct {
			name     string
			duckType string
			want     arrow.DataType
			wantErr  bool
		}{
			{
				name:     "tinyint",
				duckType: "tinyint",
				want:     arrow.PrimitiveTypes.Int8,
			},
			{
				name:     "smallint",
				duckType: "smallint",
				want:     arrow.PrimitiveTypes.Int16,
			},
			{
				name:     "integer",
				duckType: "integer",
				want:     arrow.PrimitiveTypes.Int32,
			},
			{
				name:     "bigint",
				duckType: "bigint",
				want:     arrow.PrimitiveTypes.Int64,
			},
			{
				name:     "real",
				duckType: "real",
				want:     arrow.PrimitiveTypes.Float32,
			},
			{
				name:     "double",
				duckType: "double",
				want:     arrow.PrimitiveTypes.Float64,
			},
			{
				name:     "boolean",
				duckType: "boolean",
				want:     arrow.FixedWidthTypes.Boolean,
			},
			{
				name:     "varchar",
				duckType: "varchar",
				want:     arrow.BinaryTypes.String,
			},
			{
				name:     "decimal",
				duckType: "decimal(18,2)",
				want:     &arrow.Decimal128Type{Precision: 18, Scale: 2},
			},
			{
				name:     "invalid type",
				duckType: "invalid_type",
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := tc.DuckDBToArrowType(tt.duckType)
				if tt.wantErr {
					assert.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("ArrowToDuckDBType", func(t *testing.T) {
		tests := []struct {
			name      string
			arrowType arrow.DataType
			want      string
			wantErr   bool
		}{
			{
				name:      "int8",
				arrowType: arrow.PrimitiveTypes.Int8,
				want:      "TINYINT",
			},
			{
				name:      "int16",
				arrowType: arrow.PrimitiveTypes.Int16,
				want:      "SMALLINT",
			},
			{
				name:      "int32",
				arrowType: arrow.PrimitiveTypes.Int32,
				want:      "INTEGER",
			},
			{
				name:      "int64",
				arrowType: arrow.PrimitiveTypes.Int64,
				want:      "BIGINT",
			},
			{
				name:      "float32",
				arrowType: arrow.PrimitiveTypes.Float32,
				want:      "FLOAT",
			},
			{
				name:      "float64",
				arrowType: arrow.PrimitiveTypes.Float64,
				want:      "DOUBLE",
			},
			{
				name:      "boolean",
				arrowType: arrow.FixedWidthTypes.Boolean,
				want:      "BOOLEAN",
			},
			{
				name:      "string",
				arrowType: arrow.BinaryTypes.String,
				want:      "VARCHAR",
			},
			{
				name:      "decimal",
				arrowType: &arrow.Decimal128Type{Precision: 18, Scale: 2},
				want:      "DECIMAL(18,2)",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := tc.ArrowToDuckDBType(tt.arrowType)
				if tt.wantErr {
					assert.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("GetSQLType", func(t *testing.T) {
		tests := []struct {
			name     string
			duckType string
			want     int32
		}{
			{
				name:     "tinyint",
				duckType: "tinyint",
				want:     int32(java_sql_Types_TINYINT),
			},
			{
				name:     "smallint",
				duckType: "smallint",
				want:     int32(java_sql_Types_SMALLINT),
			},
			{
				name:     "integer",
				duckType: "integer",
				want:     int32(java_sql_Types_INTEGER),
			},
			{
				name:     "bigint",
				duckType: "bigint",
				want:     int32(java_sql_Types_BIGINT),
			},
			{
				name:     "real",
				duckType: "real",
				want:     int32(java_sql_Types_REAL),
			},
			{
				name:     "float",
				duckType: "float",
				want:     int32(java_sql_Types_FLOAT),
			},
			{
				name:     "double",
				duckType: "double",
				want:     int32(java_sql_Types_DOUBLE),
			},
			{
				name:     "decimal",
				duckType: "decimal",
				want:     int32(java_sql_Types_DECIMAL),
			},
			{
				name:     "numeric",
				duckType: "numeric",
				want:     int32(java_sql_Types_NUMERIC),
			},
			{
				name:     "boolean",
				duckType: "boolean",
				want:     int32(java_sql_Types_BOOLEAN),
			},
			{
				name:     "varchar",
				duckType: "varchar",
				want:     int32(java_sql_Types_VARCHAR),
			},
			{
				name:     "text",
				duckType: "text",
				want:     int32(java_sql_Types_VARCHAR),
			},
			{
				name:     "blob",
				duckType: "blob",
				want:     int32(java_sql_Types_BLOB),
			},
			{
				name:     "date",
				duckType: "date",
				want:     int32(java_sql_Types_DATE),
			},
			{
				name:     "time",
				duckType: "time",
				want:     int32(java_sql_Types_TIME),
			},
			{
				name:     "timestamp",
				duckType: "timestamp",
				want:     int32(java_sql_Types_TIMESTAMP),
			},
			{
				name:     "unknown type",
				duckType: "unknown",
				want:     int32(java_sql_Types_VARCHAR), // Default to VARCHAR
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := tc.GetSQLType(tt.duckType)
				assert.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("DuckDBToArrowValue", func(t *testing.T) {
		tests := []struct {
			name      string
			value     interface{}
			arrowType arrow.DataType
			want      interface{}
			wantErr   bool
		}{
			{
				name:      "null value",
				value:     nil,
				arrowType: arrow.PrimitiveTypes.Int32,
				want:      nil,
			},
			{
				name:      "sql.NullInt32 valid",
				value:     sql.NullInt32{Int32: 42, Valid: true},
				arrowType: arrow.PrimitiveTypes.Int32,
				want:      int32(42),
			},
			{
				name:      "sql.NullInt32 invalid",
				value:     sql.NullInt32{Valid: false},
				arrowType: arrow.PrimitiveTypes.Int32,
				want:      nil,
			},
			{
				name:      "sql.NullString valid",
				value:     sql.NullString{String: "test", Valid: true},
				arrowType: arrow.BinaryTypes.String,
				want:      "test",
			},
			{
				name:      "sql.NullString invalid",
				value:     sql.NullString{Valid: false},
				arrowType: arrow.BinaryTypes.String,
				want:      nil,
			},
			{
				name:      "direct int32",
				value:     int32(42),
				arrowType: arrow.PrimitiveTypes.Int32,
				want:      int32(42),
			},
			{
				name:      "direct string",
				value:     "test",
				arrowType: arrow.BinaryTypes.String,
				want:      "test",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := tc.DuckDBToArrowValue(tt.value, tt.arrowType)
				if tt.wantErr {
					assert.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("ArrowToDuckDBValue", func(t *testing.T) {
		tests := []struct {
			name     string
			value    interface{}
			duckType string
			want     interface{}
			wantErr  bool
		}{
			{
				name:     "null value",
				value:    nil,
				duckType: "integer",
				want:     nil,
			},
			{
				name:     "int32",
				value:    int32(42),
				duckType: "integer",
				want:     int32(42),
			},
			{
				name:     "string",
				value:    "test",
				duckType: "varchar",
				want:     "test",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := tc.ArrowToDuckDBValue(tt.value, tt.duckType)
				if tt.wantErr {
					assert.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			})
		}
	})
}
