package handlers

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockQueryHandler is a mock implementation of QueryHandler
type MockQueryHandler struct {
	mock.Mock
}

func (m *MockQueryHandler) ExecuteStatement(ctx context.Context, query string, transactionID string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx, query, transactionID)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func (m *MockQueryHandler) ExecuteUpdate(ctx context.Context, query string, transactionID string) (int64, error) {
	args := m.Called(ctx, query, transactionID)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockQueryHandler) GetFlightInfo(ctx context.Context, query string) (*flight.FlightInfo, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*flight.FlightInfo), args.Error(1)
}

func (m *MockQueryHandler) ExecuteQueryAndStream(ctx context.Context, query string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return nil, args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(<-chan flight.StreamChunk), args.Error(2)
}

func TestQueryHandler_ExecuteStatement(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		transactionID string
		setupMock     func(*MockQueryHandler)
		expectedErr   bool
	}{
		{
			name:          "successful execution",
			query:         "SELECT * FROM test",
			transactionID: "tx123",
			setupMock: func(m *MockQueryHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("ExecuteStatement", mock.Anything, "SELECT * FROM test", "tx123").
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name:          "empty query",
			query:         "",
			transactionID: "tx123",
			setupMock: func(m *MockQueryHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("ExecuteStatement", mock.Anything, "", "tx123").
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockQueryHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.ExecuteStatement(context.Background(), tt.query, tt.transactionID)

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

func TestQueryHandler_ExecuteUpdate(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		transactionID string
		setupMock     func(*MockQueryHandler)
		expectedRows  int64
		expectedErr   bool
	}{
		{
			name:          "successful update",
			query:         "UPDATE test SET value = 1",
			transactionID: "tx123",
			setupMock: func(m *MockQueryHandler) {
				m.On("ExecuteUpdate", mock.Anything, "UPDATE test SET value = 1", "tx123").
					Return(int64(5), nil)
			},
			expectedRows: 5,
			expectedErr:  false,
		},
		{
			name:          "failed update",
			query:         "UPDATE test SET value = 1",
			transactionID: "tx123",
			setupMock: func(m *MockQueryHandler) {
				m.On("ExecuteUpdate", mock.Anything, "UPDATE test SET value = 1", "tx123").
					Return(int64(0), assert.AnError)
			},
			expectedRows: 0,
			expectedErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockQueryHandler)
			tt.setupMock(mockHandler)

			rows, err := mockHandler.ExecuteUpdate(context.Background(), tt.query, tt.transactionID)

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

func TestQueryHandler_GetFlightInfo(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		setupMock   func(*MockQueryHandler)
		expectedErr bool
	}{
		{
			name:  "successful flight info",
			query: "SELECT * FROM test",
			setupMock: func(m *MockQueryHandler) {
				info := &flight.FlightInfo{
					Schema: []byte("test schema"),
				}
				m.On("GetFlightInfo", mock.Anything, "SELECT * FROM test").
					Return(info, nil)
			},
			expectedErr: false,
		},
		{
			name:  "failed flight info",
			query: "SELECT * FROM test",
			setupMock: func(m *MockQueryHandler) {
				m.On("GetFlightInfo", mock.Anything, "SELECT * FROM test").
					Return(nil, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockQueryHandler)
			tt.setupMock(mockHandler)

			info, err := mockHandler.GetFlightInfo(context.Background(), tt.query)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, info)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, info)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestQueryHandler_ExecuteQueryAndStream(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		setupMock   func(*MockQueryHandler)
		expectedErr bool
	}{
		{
			name:  "successful query and stream",
			query: "SELECT * FROM stream_test",
			setupMock: func(m *MockQueryHandler) {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "data", Type: arrow.BinaryTypes.String},
				}, nil)
				ch := make(chan flight.StreamChunk)
				recvCh := (<-chan flight.StreamChunk)(ch)
				m.On("ExecuteQueryAndStream", mock.Anything, "SELECT * FROM stream_test").
					Return(schema, recvCh, nil)
			},
			expectedErr: false,
		},
		{
			name:  "failed query and stream",
			query: "SELECT * FROM stream_test",
			setupMock: func(m *MockQueryHandler) {
				var nilCh <-chan flight.StreamChunk
				m.On("ExecuteQueryAndStream", mock.Anything, "SELECT * FROM stream_test").
					Return(nil, nilCh, assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockQueryHandler)
			tt.setupMock(mockHandler)

			schema, ch, err := mockHandler.ExecuteQueryAndStream(context.Background(), tt.query)

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
