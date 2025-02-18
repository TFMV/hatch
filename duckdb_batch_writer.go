package flight

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DuckDBBatchWriter writes Arrow record batches into a DuckDB table.
type DuckDBBatchWriter struct {
	db        *sql.DB
	tableName string
	schema    *arrow.Schema
	stmt      *sql.Stmt
}

// NewDuckDBBatchWriter creates a new writer instance. It generates and prepares an INSERT
// statement based on the provided table name and schema.
func NewDuckDBBatchWriter(ctx context.Context, db *sql.DB, tableName string, schema *arrow.Schema) (*DuckDBBatchWriter, error) {
	if db == nil {
		return nil, fmt.Errorf("db cannot be nil")
	}
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	// Generate the INSERT statement.
	stmtStr := generateInsertStmtForTable(tableName, schema)
	stmt, err := db.PrepareContext(ctx, stmtStr)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare insert statement: %w", err)
	}

	return &DuckDBBatchWriter{
		db:        db,
		tableName: tableName,
		schema:    schema,
		stmt:      stmt,
	}, nil
}

// generateInsertStmtForTable returns an INSERT statement string for the given table and schema.
// For example, for table "my_table" and schema with fields "id", "value" the statement will be:
//
//	INSERT INTO my_table (id, value) VALUES (?, ?)
func generateInsertStmtForTable(tableName string, schema *arrow.Schema) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(tableName)
	b.WriteString(" (")

	for i, field := range schema.Fields() {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(field.Name)
	}
	b.WriteString(") VALUES (")

	for i := 0; i < schema.NumFields(); i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("?")
	}
	b.WriteString(")")
	return b.String()
}

// Write writes the given Arrow record to the table. It iterates over each row in the record,
// converts each cell to a Go value using scalarToIFace, and executes the prepared INSERT statement.
// The provided context is used for all database operations.
func (w *DuckDBBatchWriter) Write(ctx context.Context, record arrow.Record) error {
	if record == nil {
		return status.Error(codes.InvalidArgument, "record is nil")
	}

	numRows := int(record.NumRows())
	numCols := int(record.NumCols())
	if numRows == 0 {
		return nil
	}

	// Iterate over each row in the record.
	for row := 0; row < numRows; row++ {
		params := make([]interface{}, numCols)
		for col := 0; col < numCols; col++ {
			// Get the scalar value for this row and column.
			colArray := record.Column(col)
			sc, err := scalar.GetScalar(colArray, row)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to get scalar for row %d, col %d: %v", row, col, err)
			}

			value, err := scalarToIFace(sc)
			if err != nil {
				if r, ok := sc.(scalar.Releasable); ok {
					r.Release()
				}
				return status.Errorf(codes.Internal, "failed to convert scalar for row %d, col %d: %v", row, col, err)
			}
			params[col] = value

			if r, ok := sc.(scalar.Releasable); ok {
				r.Release()
			}
		}

		// Execute the insert statement with the parameters for this row.
		if _, err := w.stmt.ExecContext(ctx, params...); err != nil {
			return status.Errorf(codes.Internal, "failed to execute insert for row %d: %v", row, err)
		}
	}

	return nil
}

// Close releases resources held by the writer (e.g. the prepared statement).
func (w *DuckDBBatchWriter) Close() error {
	if w.stmt != nil {
		err := w.stmt.Close()
		w.stmt = nil
		return err
	}
	return nil
}
