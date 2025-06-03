package handlers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockTransactionHandler is a mock implementation of TransactionHandler
type MockTransactionHandler struct {
	mock.Mock
}

func (m *MockTransactionHandler) Begin(ctx context.Context, readOnly bool) (string, error) {
	args := m.Called(ctx, readOnly)
	return args.String(0), args.Error(1)
}

func (m *MockTransactionHandler) Commit(ctx context.Context, transactionID string) error {
	args := m.Called(ctx, transactionID)
	return args.Error(0)
}

func (m *MockTransactionHandler) Rollback(ctx context.Context, transactionID string) error {
	args := m.Called(ctx, transactionID)
	return args.Error(0)
}

func TestTransactionHandler_Begin(t *testing.T) {
	tests := []struct {
		name        string
		readOnly    bool
		setupMock   func(*MockTransactionHandler)
		expectedID  string
		expectedErr bool
	}{
		{
			name:     "successful begin read-write",
			readOnly: false,
			setupMock: func(m *MockTransactionHandler) {
				m.On("Begin", mock.Anything, false).
					Return("tx123", nil)
			},
			expectedID:  "tx123",
			expectedErr: false,
		},
		{
			name:     "successful begin read-only",
			readOnly: true,
			setupMock: func(m *MockTransactionHandler) {
				m.On("Begin", mock.Anything, true).
					Return("tx123", nil)
			},
			expectedID:  "tx123",
			expectedErr: false,
		},
		{
			name:     "failed begin",
			readOnly: false,
			setupMock: func(m *MockTransactionHandler) {
				m.On("Begin", mock.Anything, false).
					Return("", assert.AnError)
			},
			expectedID:  "",
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockTransactionHandler)
			tt.setupMock(mockHandler)

			id, err := mockHandler.Begin(context.Background(), tt.readOnly)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Empty(t, id)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedID, id)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestTransactionHandler_Commit(t *testing.T) {
	tests := []struct {
		name          string
		transactionID string
		setupMock     func(*MockTransactionHandler)
		expectedErr   bool
	}{
		{
			name:          "successful commit",
			transactionID: "tx123",
			setupMock: func(m *MockTransactionHandler) {
				m.On("Commit", mock.Anything, "tx123").
					Return(nil)
			},
			expectedErr: false,
		},
		{
			name:          "failed commit",
			transactionID: "tx123",
			setupMock: func(m *MockTransactionHandler) {
				m.On("Commit", mock.Anything, "tx123").
					Return(assert.AnError)
			},
			expectedErr: true,
		},
		{
			name:          "empty transaction ID",
			transactionID: "",
			setupMock: func(m *MockTransactionHandler) {
				m.On("Commit", mock.Anything, "").
					Return(assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockTransactionHandler)
			tt.setupMock(mockHandler)

			err := mockHandler.Commit(context.Background(), tt.transactionID)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

func TestTransactionHandler_Rollback(t *testing.T) {
	tests := []struct {
		name          string
		transactionID string
		setupMock     func(*MockTransactionHandler)
		expectedErr   bool
	}{
		{
			name:          "successful rollback",
			transactionID: "tx123",
			setupMock: func(m *MockTransactionHandler) {
				m.On("Rollback", mock.Anything, "tx123").
					Return(nil)
			},
			expectedErr: false,
		},
		{
			name:          "failed rollback",
			transactionID: "tx123",
			setupMock: func(m *MockTransactionHandler) {
				m.On("Rollback", mock.Anything, "tx123").
					Return(assert.AnError)
			},
			expectedErr: true,
		},
		{
			name:          "empty transaction ID",
			transactionID: "",
			setupMock: func(m *MockTransactionHandler) {
				m.On("Rollback", mock.Anything, "").
					Return(assert.AnError)
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := new(MockTransactionHandler)
			tt.setupMock(mockHandler)

			err := mockHandler.Rollback(context.Background(), tt.transactionID)

			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}
