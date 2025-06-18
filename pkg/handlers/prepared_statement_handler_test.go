package handlers

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/TFMV/hatch/pkg/infrastructure/metrics"
	"github.com/TFMV/hatch/pkg/models"
	"github.com/TFMV/hatch/pkg/services"
)

// MockPreparedStatementHandler is a mock implementation of PreparedStatementHandler
type MockPreparedStatementHandler struct {
	mock.Mock
}

func (m *MockPreparedStatementHandler) Create(ctx context.Context, query string, transactionID string) (string, *arrow.Schema, error) {
	args := m.Called(ctx, query, transactionID)
	if args.Get(1) == nil {
		return args.String(0), nil, args.Error(2)
	}
	return args.String(0), args.Get(1).(*arrow.Schema), args.Error(2)
}

func (m *MockPreparedStatementHandler) Close(ctx context.Context, handle string) error {
	args := m.Called(ctx, handle)
	return args.Error(0)
}

func (m *MockPreparedStatementHandler) ExecuteQuery(ctx context.Context, handle string, params arrow.Record) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx, handle, params)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func (m *MockPreparedStatementHandler) ExecuteUpdate(ctx context.Context, handle string, params arrow.Record) (int64, error) {
	args := m.Called(ctx, handle, params)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockPreparedStatementHandler) GetSchema(ctx context.Context, handle string) (*arrow.Schema, error) {
	args := m.Called(ctx, handle)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*arrow.Schema), args.Error(1)
}

func (m *MockPreparedStatementHandler) GetParameterSchema(ctx context.Context, handle string) (*arrow.Schema, error) {
	args := m.Called(ctx, handle)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*arrow.Schema), args.Error(1)
}

func (m *MockPreparedStatementHandler) SetParameters(ctx context.Context, handle string, params arrow.Record) error {
	args := m.Called(ctx, handle, params)
	return args.Error(0)
}

func TestPreparedStatementHandler_Create(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		transactionID string
		setupMock     func(*MockPreparedStatementHandler)
		expectedID    string
		expectedErr   bool
	}{
		{
			name:          "successful create",
			query:         "SELECT * FROM test WHERE id = ?",
			transactionID: "tx123",
			setupMock: func(m *MockPreparedStatementHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				}, nil)
				m.On("Create", mock.Anything, "SELECT * FROM test WHERE id = ?", "tx123").
					Return("stmt123", schema, nil)
			},
			expectedID:  "stmt123",
			expectedErr: false,
		},
		{
			name:          "failed create",
			query:         "SELECT * FROM test WHERE id = ?",
			transactionID: "tx123",
			setupMock: func(m *MockPreparedStatementHandler) {
				m.On("Create", mock.Anything, "SELECT * FROM test WHERE id = ?", "tx123").
					Return("", nil, assert.AnError)
			},
			expectedID:  "",
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockPreparedStatementHandler)
			tt.setupMock(mockHandler)

			id, schema, err := mockHandler.Create(context.Background(), tt.query, tt.transactionID)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Empty(t, id)
				assert.Nil(t, schema)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedID, id)
				assert.NotNil(t, schema)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestPreparedStatementHandler_Close(t *testing.T) {
	tests := []struct {
		name        string
		handle      string
		setupMock   func(*MockPreparedStatementHandler)
		expectedErr bool
	}{
		{
			name:   "successful close",
			handle: "stmt123",
			setupMock: func(m *MockPreparedStatementHandler) {
				m.On("Close", mock.Anything, "stmt123").
					Return(nil)
			},
			expectedErr: false,
		},
		{
			name:   "failed close",
			handle: "stmt123",
			setupMock: func(m *MockPreparedStatementHandler) {
				m.On("Close", mock.Anything, "stmt123").
					Return(assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockPreparedStatementHandler)
			tt.setupMock(mockHandler)

			err := mockHandler.Close(context.Background(), tt.handle)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestPreparedStatementHandler_ExecuteQuery(t *testing.T) {
	tests := []struct {
		name        string
		handle      string
		params      arrow.Record
		setupMock   func(*MockPreparedStatementHandler)
		expectedErr bool
	}{
		{
			name:   "successful execute query",
			handle: "stmt123",
			params: nil,
			setupMock: func(m *MockPreparedStatementHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("ExecuteQuery", mock.Anything, "stmt123", nil).
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name:   "failed execute query",
			handle: "stmt123",
			params: nil,
			setupMock: func(m *MockPreparedStatementHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("ExecuteQuery", mock.Anything, "stmt123", nil).
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockPreparedStatementHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.ExecuteQuery(context.Background(), tt.handle, tt.params)

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

func TestPreparedStatementHandler_ExecuteUpdate(t *testing.T) {
	tests := []struct {
		name         string
		handle       string
		params       arrow.Record
		setupMock    func(*MockPreparedStatementHandler)
		expectedRows int64
		expectedErr  bool
	}{
		{
			name:   "successful execute update",
			handle: "stmt123",
			params: nil,
			setupMock: func(m *MockPreparedStatementHandler) {
				m.On("ExecuteUpdate", mock.Anything, "stmt123", nil).
					Return(int64(5), nil)
			},
			expectedRows: 5,
			expectedErr:  false,
		},
		{
			name:   "failed execute update",
			handle: "stmt123",
			params: nil,
			setupMock: func(m *MockPreparedStatementHandler) {
				m.On("ExecuteUpdate", mock.Anything, "stmt123", nil).
					Return(int64(0), assert.AnError)
			},
			expectedRows: 0,
			expectedErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockPreparedStatementHandler)
			tt.setupMock(mockHandler)

			rows, err := mockHandler.ExecuteUpdate(context.Background(), tt.handle, tt.params)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Equal(t, int64(0), rows)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedRows, rows)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestPreparedStatementHandler_GetSchema(t *testing.T) {
	tests := []struct {
		name        string
		handle      string
		setupMock   func(*MockPreparedStatementHandler)
		expectedErr bool
	}{
		{
			name:   "successful get schema",
			handle: "stmt123",
			setupMock: func(m *MockPreparedStatementHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				}, nil)
				m.On("GetSchema", mock.Anything, "stmt123").
					Return(schema, nil)
			},
			expectedErr: false,
		},
		{
			name:   "failed get schema",
			handle: "stmt123",
			setupMock: func(m *MockPreparedStatementHandler) {
				m.On("GetSchema", mock.Anything, "stmt123").
					Return(nil, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockPreparedStatementHandler)
			tt.setupMock(mockHandler)

			schema, err := mockHandler.GetSchema(context.Background(), tt.handle)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, schema)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestPreparedStatementHandler_GetParameterSchema(t *testing.T) {
	tests := []struct {
		name        string
		handle      string
		setupMock   func(*MockPreparedStatementHandler)
		expectedErr bool
	}{
		{
			name:   "successful get parameter schema",
			handle: "stmt123",
			setupMock: func(m *MockPreparedStatementHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "param1", Type: arrow.PrimitiveTypes.Int64},
				}, nil)
				m.On("GetParameterSchema", mock.Anything, "stmt123").
					Return(schema, nil)
			},
			expectedErr: false,
		},
		{
			name:   "failed get parameter schema",
			handle: "stmt123",
			setupMock: func(m *MockPreparedStatementHandler) {
				m.On("GetParameterSchema", mock.Anything, "stmt123").
					Return(nil, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockPreparedStatementHandler)
			tt.setupMock(mockHandler)

			schema, err := mockHandler.GetParameterSchema(context.Background(), tt.handle)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, schema)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestPreparedStatementHandler_SetParameters(t *testing.T) {
	tests := []struct {
		name        string
		handle      string
		params      arrow.Record
		setupMock   func(*MockPreparedStatementHandler)
		expectedErr bool
	}{
		{
			name:   "successful set parameters",
			handle: "stmt123",
			params: nil,
			setupMock: func(m *MockPreparedStatementHandler) {
				m.On("SetParameters", mock.Anything, "stmt123", nil).
					Return(nil)
			},
			expectedErr: false,
		},
		{
			name:   "failed set parameters",
			handle: "stmt123",
			params: nil,
			setupMock: func(m *MockPreparedStatementHandler) {
				m.On("SetParameters", mock.Anything, "stmt123", nil).
					Return(assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockPreparedStatementHandler)
			tt.setupMock(mockHandler)

			err := mockHandler.SetParameters(context.Background(), tt.handle, tt.params)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

// mockPreparedStatementService implements services.PreparedStatementService for tests.
type mockPreparedStatementService struct {
	services.PreparedStatementService
	setParametersFunc func(ctx context.Context, handle string, params [][]interface{}) error
}

func (m *mockPreparedStatementService) Create(ctx context.Context, query string, transactionID string) (*models.PreparedStatement, error) {
	return nil, nil
}

func (m *mockPreparedStatementService) Get(ctx context.Context, handle string) (*models.PreparedStatement, error) {
	return nil, nil
}

func (m *mockPreparedStatementService) Close(ctx context.Context, handle string) error {
	return nil
}

func (m *mockPreparedStatementService) ExecuteQuery(ctx context.Context, handle string, params [][]interface{}) (*models.QueryResult, error) {
	return nil, nil
}

func (m *mockPreparedStatementService) ExecuteUpdate(ctx context.Context, handle string, params [][]interface{}) (*models.UpdateResult, error) {
	return nil, nil
}

func (m *mockPreparedStatementService) List(ctx context.Context, transactionID string) ([]*models.PreparedStatement, error) {
	return nil, nil
}

func (m *mockPreparedStatementService) SetParameters(ctx context.Context, handle string, params [][]interface{}) error {
	if m.setParametersFunc != nil {
		return m.setParametersFunc(ctx, handle, params)
	}
	return nil
}

// mockLogger implements services.Logger for tests.
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, keysAndValues ...interface{}) {}
func (m *mockLogger) Info(msg string, keysAndValues ...interface{})  {}
func (m *mockLogger) Warn(msg string, keysAndValues ...interface{})  {}
func (m *mockLogger) Error(msg string, keysAndValues ...interface{}) {}

func TestPreparedStatementHandler_SetParametersRealRecord(t *testing.T) {
	allocator := memory.NewGoAllocator()

	captured := [][]interface{}{}
	svc := &mockPreparedStatementService{
		setParametersFunc: func(ctx context.Context, handle string, params [][]interface{}) error {
			captured = params
			return nil
		},
	}
	handler := NewPreparedStatementHandler(svc, nil, allocator, &mockLogger{}, metrics.NewNoOpCollector())

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(42)
	record := builder.NewRecord()
	defer record.Release()

	assert.NotPanics(t, func() {
		err := handler.SetParameters(context.Background(), "stmt", record)
		assert.NoError(t, err)
	})

	assert.Equal(t, 1, len(captured))
	assert.Equal(t, 1, len(captured[0]))
	assert.Equal(t, int64(42), captured[0][0])
}
