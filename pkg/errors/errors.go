// Package errors provides standardized error types for the Flight SQL server.
package errors

import (
	"errors"
	"fmt"
)

// Error codes matching gRPC/Flight SQL conventions
const (
	CodeInvalidRequest     = "INVALID_REQUEST"
	CodeNotFound           = "NOT_FOUND"
	CodeAlreadyExists      = "ALREADY_EXISTS"
	CodeTransactionFailed  = "TRANSACTION_FAILED"
	CodeQueryFailed        = "QUERY_FAILED"
	CodeStatementFailed    = "STATEMENT_FAILED"
	CodeConnectionFailed   = "CONNECTION_FAILED"
	CodeMetadataFailed     = "METADATA_FAILED"
	CodeInternal           = "INTERNAL_ERROR"
	CodeUnavailable        = "UNAVAILABLE"
	CodeDeadlineExceeded   = "DEADLINE_EXCEEDED"
	CodeCanceled           = "CANCELED"
	CodeFailedPrecondition = "FAILED_PRECONDITION"
	CodeAborted            = "ABORTED"
	CodeResourceExhausted  = "RESOURCE_EXHAUSTED"
	CodeUnimplemented      = "UNIMPLEMENTED"
	CodeUnauthorized       = "UNAUTHORIZED"
	CodePermissionDenied   = "PERMISSION_DENIED"
)

// FlightError represents a Flight SQL error with code, message, and optional details.
type FlightError struct {
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details,omitempty"`
	Cause   error                  `json:"-"`
}

// Error implements the error interface.
func (e *FlightError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s (caused by: %v)", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap returns the underlying error.
func (e *FlightError) Unwrap() error {
	return e.Cause
}

// Is implements error comparison.
func (e *FlightError) Is(target error) bool {
	t, ok := target.(*FlightError)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// WithDetails adds details to the error.
func (e *FlightError) WithDetails(details map[string]interface{}) *FlightError {
	e.Details = details
	return e
}

// WithDetail adds a single detail to the error.
func (e *FlightError) WithDetail(key string, value interface{}) *FlightError {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	e.Details[key] = value
	return e
}

// Common errors
var (
	ErrInvalidQuery        = &FlightError{Code: CodeInvalidRequest, Message: "invalid query"}
	ErrTransactionNotFound = &FlightError{Code: CodeNotFound, Message: "transaction not found"}
	ErrStatementNotFound   = &FlightError{Code: CodeNotFound, Message: "prepared statement not found"}
	ErrTableNotFound       = &FlightError{Code: CodeNotFound, Message: "table not found"}
	ErrSchemaNotFound      = &FlightError{Code: CodeNotFound, Message: "schema not found"}
	ErrCatalogNotFound     = &FlightError{Code: CodeNotFound, Message: "catalog not found"}
	ErrInvalidTransaction  = &FlightError{Code: CodeInvalidRequest, Message: "invalid transaction"}
	ErrTransactionActive   = &FlightError{Code: CodeAlreadyExists, Message: "transaction already active"}
	ErrConnectionFailed    = &FlightError{Code: CodeUnavailable, Message: "database connection failed"}
	ErrQueryTimeout        = &FlightError{Code: CodeDeadlineExceeded, Message: "query execution timeout"}
	ErrResourceExhausted   = &FlightError{Code: CodeResourceExhausted, Message: "resource limit exceeded"}
	ErrNotImplemented      = &FlightError{Code: CodeUnimplemented, Message: "feature not implemented"}
)

// New creates a new FlightError with the given code and message.
func New(code, message string) *FlightError {
	return &FlightError{
		Code:    code,
		Message: message,
	}
}

// Wrap wraps an error with a FlightError.
func Wrap(err error, code, message string) *FlightError {
	if err == nil {
		return nil
	}
	return &FlightError{
		Code:    code,
		Message: message,
		Cause:   err,
	}
}

// Wrapf wraps an error with a formatted message.
func Wrapf(err error, code, format string, args ...interface{}) *FlightError {
	if err == nil {
		return nil
	}
	return &FlightError{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
		Cause:   err,
	}
}

// IsNotFound checks if an error is a not found error.
func IsNotFound(err error) bool {
	var flightErr *FlightError
	if errors.As(err, &flightErr) {
		return flightErr.Code == CodeNotFound
	}
	return false
}

// IsInvalidRequest checks if an error is an invalid request error.
func IsInvalidRequest(err error) bool {
	var flightErr *FlightError
	if errors.As(err, &flightErr) {
		return flightErr.Code == CodeInvalidRequest
	}
	return false
}

// IsInternal checks if an error is an internal error.
func IsInternal(err error) bool {
	var flightErr *FlightError
	if errors.As(err, &flightErr) {
		return flightErr.Code == CodeInternal
	}
	return false
}

// GetCode extracts the error code from an error.
func GetCode(err error) string {
	var flightErr *FlightError
	if errors.As(err, &flightErr) {
		return flightErr.Code
	}
	return CodeInternal
}

// GetMessage extracts the error message from an error.
func GetMessage(err error) string {
	var flightErr *FlightError
	if errors.As(err, &flightErr) {
		return flightErr.Message
	}
	return err.Error()
}
