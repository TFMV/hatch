package handlers

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockMetadataHandler is a mock implementation of MetadataHandler
type MockMetadataHandler struct {
	mock.Mock
}

func (m *MockMetadataHandler) GetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func (m *MockMetadataHandler) GetSchemas(ctx context.Context, catalog *string, schemaPattern *string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx, catalog, schemaPattern)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func (m *MockMetadataHandler) GetTables(ctx context.Context, catalog *string, schemaPattern *string, tablePattern *string, tableTypes []string, includeSchema bool) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx, catalog, schemaPattern, tablePattern, tableTypes, includeSchema)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func (m *MockMetadataHandler) GetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func (m *MockMetadataHandler) GetPrimaryKeys(ctx context.Context, catalog *string, schema *string, table string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx, catalog, schema, table)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func (m *MockMetadataHandler) GetImportedKeys(ctx context.Context, catalog *string, schema *string, table string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx, catalog, schema, table)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func (m *MockMetadataHandler) GetExportedKeys(ctx context.Context, catalog *string, schema *string, table string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx, catalog, schema, table)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func (m *MockMetadataHandler) GetXdbcTypeInfo(ctx context.Context, dataType *int32) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx, dataType)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func (m *MockMetadataHandler) GetSqlInfo(ctx context.Context, info []uint32) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx, info)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func TestMetadataHandler_GetCatalogs(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*MockMetadataHandler)
		expectedErr bool
	}{
		{
			name: "successful get catalogs",
			setupMock: func(m *MockMetadataHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "catalog_name", Type: arrow.BinaryTypes.String},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("GetCatalogs", mock.Anything).
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name: "failed get catalogs",
			setupMock: func(m *MockMetadataHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("GetCatalogs", mock.Anything).
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockMetadataHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.GetCatalogs(context.Background())

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, schema)
				assert.Nil(t, ch)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
				assert.NotNil(t, ch)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestMetadataHandler_GetSchemas(t *testing.T) {
	tests := []struct {
		name          string
		catalog       *string
		schemaPattern *string
		setupMock     func(*MockMetadataHandler)
		expectedErr   bool
	}{
		{
			name:          "successful get schemas",
			catalog:       stringPtr("test_catalog"),
			schemaPattern: stringPtr("test_%"),
			setupMock: func(m *MockMetadataHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "schema_name", Type: arrow.BinaryTypes.String},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("GetSchemas", mock.Anything, stringPtr("test_catalog"), stringPtr("test_%")).
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name:          "failed get schemas",
			catalog:       stringPtr("test_catalog"),
			schemaPattern: stringPtr("test_%"),
			setupMock: func(m *MockMetadataHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("GetSchemas", mock.Anything, stringPtr("test_catalog"), stringPtr("test_%")).
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockMetadataHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.GetSchemas(context.Background(), tt.catalog, tt.schemaPattern)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, schema)
				assert.Nil(t, ch)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
				assert.NotNil(t, ch)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestMetadataHandler_GetTables(t *testing.T) {
	tests := []struct {
		name          string
		catalog       *string
		schemaPattern *string
		tablePattern  *string
		tableTypes    []string
		includeSchema bool
		setupMock     func(*MockMetadataHandler)
		expectedErr   bool
	}{
		{
			name:          "successful get tables",
			catalog:       stringPtr("test_catalog"),
			schemaPattern: stringPtr("test_schema"),
			tablePattern:  stringPtr("test_%"),
			tableTypes:    []string{"TABLE"},
			includeSchema: true,
			setupMock: func(m *MockMetadataHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "table_name", Type: arrow.BinaryTypes.String},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("GetTables", mock.Anything, stringPtr("test_catalog"), stringPtr("test_schema"), stringPtr("test_%"), []string{"TABLE"}, true).
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name:          "failed get tables",
			catalog:       stringPtr("test_catalog"),
			schemaPattern: stringPtr("test_schema"),
			tablePattern:  stringPtr("test_%"),
			tableTypes:    []string{"TABLE"},
			includeSchema: true,
			setupMock: func(m *MockMetadataHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("GetTables", mock.Anything, stringPtr("test_catalog"), stringPtr("test_schema"), stringPtr("test_%"), []string{"TABLE"}, true).
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockMetadataHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.GetTables(context.Background(), tt.catalog, tt.schemaPattern, tt.tablePattern, tt.tableTypes, tt.includeSchema)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, schema)
				assert.Nil(t, ch)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
				assert.NotNil(t, ch)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestMetadataHandler_GetTableTypes(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*MockMetadataHandler)
		expectedErr bool
	}{
		{
			name: "successful get table types",
			setupMock: func(m *MockMetadataHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "table_type", Type: arrow.BinaryTypes.String},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("GetTableTypes", mock.Anything).
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name: "failed get table types",
			setupMock: func(m *MockMetadataHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("GetTableTypes", mock.Anything).
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockMetadataHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.GetTableTypes(context.Background())

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, schema)
				assert.Nil(t, ch)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
				assert.NotNil(t, ch)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestMetadataHandler_GetPrimaryKeys(t *testing.T) {
	tests := []struct {
		name        string
		catalog     *string
		schema      *string
		table       string
		setupMock   func(*MockMetadataHandler)
		expectedErr bool
	}{
		{
			name:    "successful get primary keys",
			catalog: stringPtr("test_catalog"),
			schema:  stringPtr("test_schema"),
			table:   "test_table",
			setupMock: func(m *MockMetadataHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "column_name", Type: arrow.BinaryTypes.String},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("GetPrimaryKeys", mock.Anything, stringPtr("test_catalog"), stringPtr("test_schema"), "test_table").
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name:    "failed get primary keys",
			catalog: stringPtr("test_catalog"),
			schema:  stringPtr("test_schema"),
			table:   "test_table",
			setupMock: func(m *MockMetadataHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("GetPrimaryKeys", mock.Anything, stringPtr("test_catalog"), stringPtr("test_schema"), "test_table").
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockMetadataHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.GetPrimaryKeys(context.Background(), tt.catalog, tt.schema, tt.table)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, schema)
				assert.Nil(t, ch)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
				assert.NotNil(t, ch)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestMetadataHandler_GetImportedKeys(t *testing.T) {
	tests := []struct {
		name        string
		catalog     *string
		schema      *string
		table       string
		setupMock   func(*MockMetadataHandler)
		expectedErr bool
	}{
		{
			name:    "successful get imported keys",
			catalog: stringPtr("test_catalog"),
			schema:  stringPtr("test_schema"),
			table:   "test_table",
			setupMock: func(m *MockMetadataHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "column_name", Type: arrow.BinaryTypes.String},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("GetImportedKeys", mock.Anything, stringPtr("test_catalog"), stringPtr("test_schema"), "test_table").
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name:    "failed get imported keys",
			catalog: stringPtr("test_catalog"),
			schema:  stringPtr("test_schema"),
			table:   "test_table",
			setupMock: func(m *MockMetadataHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("GetImportedKeys", mock.Anything, stringPtr("test_catalog"), stringPtr("test_schema"), "test_table").
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockMetadataHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.GetImportedKeys(context.Background(), tt.catalog, tt.schema, tt.table)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, schema)
				assert.Nil(t, ch)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
				assert.NotNil(t, ch)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestMetadataHandler_GetExportedKeys(t *testing.T) {
	tests := []struct {
		name        string
		catalog     *string
		schema      *string
		table       string
		setupMock   func(*MockMetadataHandler)
		expectedErr bool
	}{
		{
			name:    "successful get exported keys",
			catalog: stringPtr("test_catalog"),
			schema:  stringPtr("test_schema"),
			table:   "test_table",
			setupMock: func(m *MockMetadataHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "column_name", Type: arrow.BinaryTypes.String},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("GetExportedKeys", mock.Anything, stringPtr("test_catalog"), stringPtr("test_schema"), "test_table").
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name:    "failed get exported keys",
			catalog: stringPtr("test_catalog"),
			schema:  stringPtr("test_schema"),
			table:   "test_table",
			setupMock: func(m *MockMetadataHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("GetExportedKeys", mock.Anything, stringPtr("test_catalog"), stringPtr("test_schema"), "test_table").
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockMetadataHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.GetExportedKeys(context.Background(), tt.catalog, tt.schema, tt.table)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, schema)
				assert.Nil(t, ch)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
				assert.NotNil(t, ch)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestMetadataHandler_GetXdbcTypeInfo(t *testing.T) {
	tests := []struct {
		name        string
		dataType    *int32
		setupMock   func(*MockMetadataHandler)
		expectedErr bool
	}{
		{
			name:     "successful get xdbc type info",
			dataType: int32Ptr(1),
			setupMock: func(m *MockMetadataHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "type_name", Type: arrow.BinaryTypes.String},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("GetXdbcTypeInfo", mock.Anything, int32Ptr(1)).
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name:     "failed get xdbc type info",
			dataType: int32Ptr(1),
			setupMock: func(m *MockMetadataHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("GetXdbcTypeInfo", mock.Anything, int32Ptr(1)).
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockMetadataHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.GetXdbcTypeInfo(context.Background(), tt.dataType)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, schema)
				assert.Nil(t, ch)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
				assert.NotNil(t, ch)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestMetadataHandler_GetSqlInfo(t *testing.T) {
	tests := []struct {
		name        string
		info        []uint32
		setupMock   func(*MockMetadataHandler)
		expectedErr bool
	}{
		{
			name: "successful get sql info",
			info: []uint32{1, 2, 3},
			setupMock: func(m *MockMetadataHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "info_name", Type: arrow.BinaryTypes.String},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("GetSqlInfo", mock.Anything, []uint32{1, 2, 3}).
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name: "failed get sql info",
			info: []uint32{1, 2, 3},
			setupMock: func(m *MockMetadataHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("GetSqlInfo", mock.Anything, []uint32{1, 2, 3}).
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockMetadataHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.GetSqlInfo(context.Background(), tt.info)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, schema)
				assert.Nil(t, ch)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
				assert.NotNil(t, ch)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

// Helper functions for creating pointers
func stringPtr(s string) *string {
	return &s
}

func int32Ptr(i int32) *int32 {
	return &i
}
