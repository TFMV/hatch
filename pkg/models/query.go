// Package models provides data structures used throughout the Flight SQL server.
package models

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// QueryRequest represents a query execution request.
type QueryRequest struct {
	Query         string                 `json:"query"`
	TransactionID string                 `json:"transaction_id,omitempty"`
	Parameters    []interface{}          `json:"parameters,omitempty"`
	MaxRows       int64                  `json:"max_rows,omitempty"`
	Timeout       time.Duration          `json:"timeout,omitempty"`
	Properties    map[string]interface{} `json:"properties,omitempty"`
}

// QueryResult represents the result of a query execution.
type QueryResult struct {
	Schema        *arrow.Schema          `json:"-"`
	Records       <-chan arrow.Record    `json:"-"`
	TotalRows     int64                  `json:"total_rows"`
	ExecutionTime time.Duration          `json:"execution_time"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

// UpdateRequest represents an update execution request.
type UpdateRequest struct {
	Statement     string                 `json:"statement"`
	TransactionID string                 `json:"transaction_id,omitempty"`
	Parameters    []interface{}          `json:"parameters,omitempty"`
	Timeout       time.Duration          `json:"timeout,omitempty"`
	Properties    map[string]interface{} `json:"properties,omitempty"`
}

// UpdateResult represents the result of an update execution.
type UpdateResult struct {
	RowsAffected  int64                  `json:"rows_affected"`
	ExecutionTime time.Duration          `json:"execution_time"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

// PreparedStatement represents a prepared SQL statement.
type PreparedStatement struct {
	Handle            string          `json:"handle"`
	Query             string          `json:"query"`
	ParameterSchema   *arrow.Schema   `json:"-"`
	ResultSetSchema   *arrow.Schema   `json:"-"`
	BoundParameters   [][]interface{} `json:"-"`
	CreatedAt         time.Time       `json:"created_at"`
	LastUsedAt        time.Time       `json:"last_used_at"`
	ExecutionCount    int64           `json:"execution_count"`
	TransactionID     string          `json:"transaction_id,omitempty"`
	IsResultSetUpdate bool            `json:"is_result_set_update"`
}

// TransactionOptions represents options for creating a transaction.
type TransactionOptions struct {
	IsolationLevel IsolationLevel         `json:"isolation_level,omitempty"`
	ReadOnly       bool                   `json:"read_only,omitempty"`
	Timeout        time.Duration          `json:"timeout,omitempty"`
	Properties     map[string]interface{} `json:"properties,omitempty"`
}

// IsolationLevel represents the transaction isolation level.
type IsolationLevel string

const (
	// IsolationLevelDefault uses the database default isolation level.
	IsolationLevelDefault IsolationLevel = ""
	// IsolationLevelReadUncommitted allows dirty reads.
	IsolationLevelReadUncommitted IsolationLevel = "READ_UNCOMMITTED"
	// IsolationLevelReadCommitted prevents dirty reads.
	IsolationLevelReadCommitted IsolationLevel = "READ_COMMITTED"
	// IsolationLevelRepeatableRead prevents dirty reads and non-repeatable reads.
	IsolationLevelRepeatableRead IsolationLevel = "REPEATABLE_READ"
	// IsolationLevelSerializable provides the highest isolation level.
	IsolationLevelSerializable IsolationLevel = "SERIALIZABLE"
)

// Transaction represents an active database transaction.
type Transaction struct {
	ID             string           `json:"id"`
	IsolationLevel IsolationLevel   `json:"isolation_level"`
	ReadOnly       bool             `json:"read_only"`
	StartedAt      time.Time        `json:"started_at"`
	LastActivityAt time.Time        `json:"last_activity_at"`
	State          TransactionState `json:"state"`
}

// TransactionState represents the state of a transaction.
type TransactionState string

const (
	// TransactionStateActive indicates an active transaction.
	TransactionStateActive TransactionState = "ACTIVE"
	// TransactionStateCommitting indicates a transaction being committed.
	TransactionStateCommitting TransactionState = "COMMITTING"
	// TransactionStateRollingBack indicates a transaction being rolled back.
	TransactionStateRollingBack TransactionState = "ROLLING_BACK"
	// TransactionStateCommitted indicates a committed transaction.
	TransactionStateCommitted TransactionState = "COMMITTED"
	// TransactionStateRolledBack indicates a rolled back transaction.
	TransactionStateRolledBack TransactionState = "ROLLED_BACK"
)

// TODO: add query metrics and plan models when streaming introspection is implemented
