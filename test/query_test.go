package test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// statementQuery implements flightsql.StatementQuery
type statementQuery struct {
	query         string
	transactionID []byte
}

func (s *statementQuery) GetQuery() string {
	return s.query
}

func (s *statementQuery) GetTransactionId() []byte {
	return s.transactionID
}

// statementUpdate implements flightsql.StatementUpdate
type statementUpdate struct {
	query         string
	transactionID []byte
}

func (s *statementUpdate) GetQuery() string {
	return s.query
}

func (s *statementUpdate) GetTransactionId() []byte {
	return s.transactionID
}

// statementQueryTicket implements flightsql.StatementQueryTicket
type statementQueryTicket struct {
	handle []byte
}

func (s *statementQueryTicket) GetStatementHandle() []byte {
	return s.handle
}

func TestQueryOperations(t *testing.T) {
	// Create test server
	srv := NewTestServer(t)
	defer srv.Close()

	// Create test table
	tableName := "test_query_table"
	createTestTable(t, srv, tableName)
	defer cleanupTestTable(t, srv, tableName)

	// Test cases
	t.Run("GetFlightInfoStatement", testGetFlightInfoStatement(srv, tableName))
	t.Run("DoGetStatement", testDoGetStatement(srv, tableName))
	t.Run("DoPutCommandStatementUpdate", testDoPutCommandStatementUpdate(srv, tableName))
	t.Run("QueryCaching", testQueryCaching(srv, tableName))
}

func testGetFlightInfoStatement(srv *TestServer, tableName string) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		query := "SELECT * FROM " + tableName
		stmtQuery := &statementQuery{query: query}

		desc := &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte(query),
		}

		info, err := srv.GetFlightInfoStatement(ctx, stmtQuery, desc)
		require.NoError(t, err)
		assert.NotNil(t, info)
		assertFlightInfo(t, info, nil)

		// Create a ticket for DoGetStatement
		require.NotEmpty(t, info.Endpoint)
		ticket := &statementQueryTicket{handle: info.Endpoint[0].Ticket.Ticket}
		schema, chunks, err := srv.DoGetStatement(ctx, ticket)
		require.NoError(t, err)
		assert.NotNil(t, schema)
		assert.NotNil(t, chunks)
		assertStreamChunks(t, chunks, schema, 1)
	}
}

func testDoGetStatement(srv *TestServer, tableName string) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		query := "SELECT * FROM " + tableName
		stmtQuery := &statementQuery{query: query}

		desc := &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte(query),
		}

		info, err := srv.GetFlightInfoStatement(ctx, stmtQuery, desc)
		require.NoError(t, err)
		assert.NotNil(t, info)

		require.NotEmpty(t, info.Endpoint)
		ticket := &statementQueryTicket{handle: info.Endpoint[0].Ticket.Ticket}
		schema, chunks, err := srv.DoGetStatement(ctx, ticket)
		require.NoError(t, err)
		assert.NotNil(t, schema)
		assert.NotNil(t, chunks)
		assertStreamChunks(t, chunks, schema, 1)
	}
}

func testDoPutCommandStatementUpdate(srv *TestServer, tableName string) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		updateQuery := "INSERT INTO " + tableName + " (id, name) VALUES (1, 'test')"
		stmtUpdate := &statementUpdate{query: updateQuery}

		rowsAffected, err := srv.DoPutCommandStatementUpdate(ctx, stmtUpdate)
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		countQuery := "SELECT COUNT(*) FROM " + tableName
		stmtQuery := &statementQuery{query: countQuery}
		desc := &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte(countQuery),
		}

		info, err := srv.GetFlightInfoStatement(ctx, stmtQuery, desc)
		require.NoError(t, err)
		assert.NotNil(t, info)

		require.NotEmpty(t, info.Endpoint)
		ticket := &statementQueryTicket{handle: info.Endpoint[0].Ticket.Ticket}
		_, chunks, err := srv.DoGetStatement(ctx, ticket)
		require.NoError(t, err)
		count := 0
		for chunk := range chunks {
			record := chunk.Data
			if record.NumRows() > 0 {
				count = int(record.Column(0).(*array.Int64).Value(0))
			}
		}
		assert.Equal(t, 2, count)
	}
}

func testQueryCaching(srv *TestServer, tableName string) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		query := "SELECT * FROM " + tableName
		stmtQuery := &statementQuery{query: query}
		desc := &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte(query),
		}

		info1, err := srv.GetFlightInfoStatement(ctx, stmtQuery, desc)
		require.NoError(t, err)
		info2, err := srv.GetFlightInfoStatement(ctx, stmtQuery, desc)
		require.NoError(t, err)
		assert.Equal(t, info1.Schema, info2.Schema)

		require.NotEmpty(t, info1.Endpoint)
		ticket := &statementQueryTicket{handle: info1.Endpoint[0].Ticket.Ticket}
		schema, chunks, err := srv.DoGetStatement(ctx, ticket)
		require.NoError(t, err)
		assertStreamChunks(t, chunks, schema, 1)
	}
}

// Helper function to create a test record
func createTestRecord(allocator memory.Allocator) arrow.Record {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	// Add data
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"test"}, nil)

	return builder.NewRecord()
}

// createTestTable creates a test table in the database
func createTestTable(t *testing.T, srv *TestServer, tableName string) {
	ctx := context.Background()
	createQuery := "CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR)"
	stmtUpdateCreate := &statementUpdate{query: createQuery}
	_, err := srv.DoPutCommandStatementUpdate(ctx, stmtUpdateCreate)
	require.NoError(t, err)

	// Insert a row for basic select tests
	insertQuery := "INSERT INTO " + tableName + " (id, name) VALUES (42, 'Douglas Adams')"
	stmtUpdateInsert := &statementUpdate{query: insertQuery}
	_, err = srv.DoPutCommandStatementUpdate(ctx, stmtUpdateInsert)
	require.NoError(t, err)
}

// cleanupTestTable cleans up a test table
func cleanupTestTable(t *testing.T, srv *TestServer, tableName string) {
	ctx := context.Background()
	dropQuery := "DROP TABLE IF EXISTS " + tableName
	stmtUpdate := &statementUpdate{query: dropQuery}
	_, err := srv.DoPutCommandStatementUpdate(ctx, stmtUpdate)
	require.NoError(t, err)
}
