package errors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlightError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *FlightError
		expected string
	}{
		{
			name: "error without cause",
			err: &FlightError{
				Code:    CodeInvalidRequest,
				Message: "invalid input",
			},
			expected: "INVALID_REQUEST: invalid input",
		},
		{
			name: "error with cause",
			err: &FlightError{
				Code:    CodeInvalidRequest,
				Message: "invalid input",
				Cause:   fmt.Errorf("underlying error"),
			},
			expected: "INVALID_REQUEST: invalid input (caused by: underlying error)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestFlightError_Unwrap(t *testing.T) {
	cause := fmt.Errorf("underlying error")
	err := &FlightError{
		Code:    CodeInvalidRequest,
		Message: "invalid input",
		Cause:   cause,
	}

	assert.Equal(t, cause, err.Unwrap())
	assert.True(t, errors.Is(err, &FlightError{Code: CodeInvalidRequest}))
}

func TestFlightError_Is(t *testing.T) {
	err1 := &FlightError{Code: CodeNotFound, Message: "not found"}
	err2 := &FlightError{Code: CodeNotFound, Message: "different message"}
	err3 := &FlightError{Code: CodeInvalidRequest, Message: "invalid"}
	stdErr := fmt.Errorf("standard error")

	assert.True(t, err1.Is(err2), "errors with same code should match")
	assert.False(t, err1.Is(err3), "errors with different codes should not match")
	assert.False(t, err1.Is(stdErr), "flight error should not match standard error")
}

func TestFlightError_WithDetails(t *testing.T) {
	err := &FlightError{
		Code:    CodeInvalidRequest,
		Message: "invalid input",
	}

	details := map[string]interface{}{
		"field": "username",
		"value": 123,
	}

	err = err.WithDetails(details)
	assert.Equal(t, details, err.Details)
}

func TestFlightError_WithDetail(t *testing.T) {
	err := &FlightError{
		Code:    CodeInvalidRequest,
		Message: "invalid input",
	}

	err = err.WithDetail("field", "username").WithDetail("value", 123)

	assert.Equal(t, "username", err.Details["field"])
	assert.Equal(t, 123, err.Details["value"])
}

func TestNew(t *testing.T) {
	err := New(CodeInvalidRequest, "test message")
	assert.Equal(t, CodeInvalidRequest, err.Code)
	assert.Equal(t, "test message", err.Message)
	assert.Nil(t, err.Cause)
}

func TestWrap(t *testing.T) {
	cause := fmt.Errorf("underlying error")
	err := Wrap(cause, CodeInvalidRequest, "wrapped message")

	assert.Equal(t, CodeInvalidRequest, err.Code)
	assert.Equal(t, "wrapped message", err.Message)
	assert.Equal(t, cause, err.Cause)

	// Test nil error
	assert.Nil(t, Wrap(nil, CodeInvalidRequest, "message"))
}

func TestWrapf(t *testing.T) {
	cause := fmt.Errorf("underlying error")
	err := Wrapf(cause, CodeInvalidRequest, "wrapped message %d", 42)

	assert.Equal(t, CodeInvalidRequest, err.Code)
	assert.Equal(t, "wrapped message 42", err.Message)
	assert.Equal(t, cause, err.Cause)

	// Test nil error
	assert.Nil(t, Wrapf(nil, CodeInvalidRequest, "message %d", 42))
}

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "not found error",
			err:      ErrTableNotFound,
			expected: true,
		},
		{
			name:     "other flight error",
			err:      ErrInvalidQuery,
			expected: false,
		},
		{
			name:     "standard error",
			err:      fmt.Errorf("standard error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsNotFound(tt.err))
		})
	}
}

func TestIsInvalidRequest(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "invalid request error",
			err:      ErrInvalidQuery,
			expected: true,
		},
		{
			name:     "other flight error",
			err:      ErrTableNotFound,
			expected: false,
		},
		{
			name:     "standard error",
			err:      fmt.Errorf("standard error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsInvalidRequest(tt.err))
		})
	}
}

func TestIsInternal(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "internal error",
			err:      New(CodeInternal, "internal error"),
			expected: true,
		},
		{
			name:     "other flight error",
			err:      ErrTableNotFound,
			expected: false,
		},
		{
			name:     "standard error",
			err:      fmt.Errorf("standard error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsInternal(tt.err))
		})
	}
}

func TestGetCode(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "flight error",
			err:      ErrTableNotFound,
			expected: CodeNotFound,
		},
		{
			name:     "standard error",
			err:      fmt.Errorf("standard error"),
			expected: CodeInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GetCode(tt.err))
		})
	}
}

func TestGetMessage(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "flight error",
			err:      ErrTableNotFound,
			expected: "table not found",
		},
		{
			name:     "standard error",
			err:      fmt.Errorf("standard error"),
			expected: "standard error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GetMessage(tt.err))
		})
	}
}

func TestCommonErrors(t *testing.T) {
	// Test that all common errors are properly initialized
	assert.Equal(t, CodeInvalidRequest, ErrInvalidQuery.Code)
	assert.Equal(t, CodeNotFound, ErrTransactionNotFound.Code)
	assert.Equal(t, CodeNotFound, ErrStatementNotFound.Code)
	assert.Equal(t, CodeNotFound, ErrTableNotFound.Code)
	assert.Equal(t, CodeNotFound, ErrSchemaNotFound.Code)
	assert.Equal(t, CodeNotFound, ErrCatalogNotFound.Code)
	assert.Equal(t, CodeInvalidRequest, ErrInvalidTransaction.Code)
	assert.Equal(t, CodeAlreadyExists, ErrTransactionActive.Code)
	assert.Equal(t, CodeUnavailable, ErrConnectionFailed.Code)
	assert.Equal(t, CodeDeadlineExceeded, ErrQueryTimeout.Code)
	assert.Equal(t, CodeResourceExhausted, ErrResourceExhausted.Code)
	assert.Equal(t, CodeUnimplemented, ErrNotImplemented.Code)
}
