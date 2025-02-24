package flight

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	flight_gen "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	_ "github.com/marcboeker/go-duckdb"
)

// genRandomString generates a random 16-byte handle.
func genRandomString() []byte {
	const length = 16
	max := int('z')
	// don't include ':' as a valid byte to generate
	// because we use it as a separator for the transactions
	min := int('<')

	out := make([]byte, length)
	for i := range out {
		out[i] = byte(rand.Intn(max-min+1) + min)
	}
	return out
}

// prepareQueryForGetTables builds a query against DuckDB's catalog views.
func prepareQueryForGetTables(cmd flightsql.GetTables) string {
	var b strings.Builder
	// Query the standard information_schema.tables view.
	b.WriteString(`SELECT table_catalog AS catalog_name,
		table_schema AS schema_name,
		table_name, table_type FROM information_schema.tables WHERE 1=1`)

	if cmd.GetCatalog() != nil {
		b.WriteString(" and table_catalog = '")
		b.WriteString(*cmd.GetCatalog())
		b.WriteByte('\'')
	}

	if cmd.GetDBSchemaFilterPattern() != nil {
		b.WriteString(" and table_schema LIKE '")
		b.WriteString(*cmd.GetDBSchemaFilterPattern())
		b.WriteByte('\'')
	}

	if cmd.GetTableNameFilterPattern() != nil {
		b.WriteString(" and table_name LIKE '")
		b.WriteString(*cmd.GetTableNameFilterPattern())
		b.WriteByte('\'')
	}

	if len(cmd.GetTableTypes()) > 0 {
		b.WriteString(" and table_type IN (")
		for i, t := range cmd.GetTableTypes() {
			if i != 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "'%s'", t)
		}
		b.WriteByte(')')
	}

	b.WriteString(" order by table_name")
	return b.String()
}

// prepareQueryForGetKeys builds a query to retrieve foreign key metadata
// using the standard information_schema views in DuckDB.
func prepareQueryForGetKeys(filter string) string {
	return `
SELECT
    kcu1.table_catalog AS pk_catalog_name,
    kcu1.table_schema AS pk_schema_name,
    kcu1.table_name AS pk_table_name,
    kcu1.column_name AS pk_column_name,
    kcu2.table_catalog AS fk_catalog_name,
    kcu2.table_schema AS fk_schema_name,
    kcu2.table_name AS fk_table_name,
    kcu2.column_name AS fk_column_name,
    kcu2.ordinal_position AS key_sequence,
    NULL AS pk_key_name,
    NULL AS fk_key_name,
    CASE rc.update_rule
        WHEN 'CASCADE' THEN 0
        WHEN 'RESTRICT' THEN 1
        WHEN 'SET NULL' THEN 2
        WHEN 'NO ACTION' THEN 3
        WHEN 'SET DEFAULT' THEN 4
    END AS update_rule,
    CASE rc.delete_rule
        WHEN 'CASCADE' THEN 0
        WHEN 'RESTRICT' THEN 1
        WHEN 'SET NULL' THEN 2
        WHEN 'NO ACTION' THEN 3
        WHEN 'SET DEFAULT' THEN 4
    END AS delete_rule
FROM information_schema.table_constraints tc
JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name
JOIN information_schema.key_column_usage kcu1 ON tc.unique_constraint_name = kcu1.constraint_name
JOIN information_schema.key_column_usage kcu2 ON tc.constraint_name = kcu2.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND ` + filter + `
ORDER BY pk_catalog_name, pk_schema_name, pk_table_name, key_sequence
`
}

// CreateDB creates an in-memory DuckDB database and initializes sample tables.
func CreateDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
	CREATE TABLE foreignTable (
		id INTEGER PRIMARY KEY,
		foreignName VARCHAR(100),
		value INT);

	CREATE TABLE intTable (
		id INTEGER PRIMARY KEY,
		keyName VARCHAR(100),
		value INT,
		foreignId INT REFERENCES foreignTable(id));

	INSERT INTO foreignTable (id, foreignName, value) VALUES (1, 'keyOne', 1);
	INSERT INTO foreignTable (id, foreignName, value) VALUES (2, 'keyTwo', 0);
	INSERT INTO foreignTable (id, foreignName, value) VALUES (3, 'keyThree', -1);
	INSERT INTO intTable (id, keyName, value, foreignId) VALUES (1, 'one', 1, 1);
	INSERT INTO intTable (id, keyName, value, foreignId) VALUES (2, 'zero', 0, 1);
	INSERT INTO intTable (id, keyName, value, foreignId) VALUES (3, 'negative one', -1, 1);
	INSERT INTO intTable (id, keyName, value, foreignId) VALUES (4, NULL, NULL, NULL);
	`)
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func EncodeTransactionQuery(query string, transactionID flightsql.Transaction) ([]byte, error) {
	return flightsql.CreateStatementQueryTicket(
		bytes.Join([][]byte{transactionID, []byte(query)}, []byte(":")))
}

func DecodeTransactionQuery(ticket []byte) (txnID, query string, err error) {
	id, queryBytes, found := bytes.Cut(ticket, []byte(":"))
	if !found {
		err = fmt.Errorf("%w: malformed ticket", arrow.ErrInvalid)
		return
	}

	txnID = string(id)
	query = string(queryBytes)
	return
}

type Statement struct {
	stmt   *sql.Stmt
	params [][]interface{}
}

// DuckDBFlightSQLServer implements FlightSQL endpoints for DuckDB.
type DuckDBFlightSQLServer struct {
	flightsql.BaseServer
	db *sql.DB

	prepared         sync.Map
	openTransactions sync.Map
	tableCreated     bool
	schema           *arrow.Schema
}

func NewDuckDBFlightSQLServer(db *sql.DB) (*DuckDBFlightSQLServer, error) {
	ret := &DuckDBFlightSQLServer{db: db}
	ret.Alloc = memory.DefaultAllocator
	for k, v := range SqlInfoResultMap() {
		ret.RegisterSqlInfo(flightsql.SqlInfo(k), v)
	}
	return ret, nil
}

func (s *DuckDBFlightSQLServer) flightInfoForCommand(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, s.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
}

func (s *DuckDBFlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	ticket := &flight.Ticket{
		Ticket: []byte(fmt.Sprintf("query:%s", cmd.GetQuery())),
	}

	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: ticket},
		},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *DuckDBFlightSQLServer) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	handle := string(cmd.GetStatementHandle())
	if !strings.HasPrefix(handle, "query:") {
		return nil, nil, status.Error(codes.InvalidArgument, "invalid ticket format")
	}
	query := strings.TrimPrefix(handle, "query:")
	return doGetQuery(ctx, s.Alloc, s.db, query, nil)
}

func (s *DuckDBFlightSQLServer) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.Catalogs), nil
}

func (s *DuckDBFlightSQLServer) DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// For DuckDB, we assume the primary catalog is "main"
	schema := schema_ref.Catalogs

	catalogs, _, err := array.FromJSON(s.Alloc, arrow.BinaryTypes.String, strings.NewReader(`["main"]`))
	if err != nil {
		return nil, nil, err
	}
	defer catalogs.Release()

	batch := array.NewRecord(schema, []arrow.Array{catalogs}, 1)

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)

	return schema, ch, nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoSchemas(_ context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.DBSchemas), nil
}

func (s *DuckDBFlightSQLServer) DoGetDBSchemas(_ context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// DuckDB supports multiple schemas; for simplicity we return the "main" schema.
	schema := schema_ref.DBSchemas

	ch := make(chan flight.StreamChunk, 1)

	if cmd.GetDBSchemaFilterPattern() == nil || *cmd.GetDBSchemaFilterPattern() == "" {
		catalogs, _, err := array.FromJSON(s.Alloc, arrow.BinaryTypes.String, strings.NewReader(`["main"]`))
		if err != nil {
			return nil, nil, err
		}
		defer catalogs.Release()

		dbSchemas, _, err := array.FromJSON(s.Alloc, arrow.BinaryTypes.String, strings.NewReader(`["main"]`))
		if err != nil {
			return nil, nil, err
		}
		defer dbSchemas.Release()

		batch := array.NewRecord(schema, []arrow.Array{catalogs, dbSchemas}, 1)
		ch <- flight.StreamChunk{Data: batch}
	}

	close(ch)

	return schema, ch, nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	return s.flightInfoForCommand(desc, schema), nil
}

func (s *DuckDBFlightSQLServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := prepareQueryForGetTables(cmd)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	var rdr array.RecordReader

	rdr, err = NewDuckDBBatchReaderWithSchema(s.Alloc, schema_ref.Tables, rows)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk, 2)
	if cmd.GetIncludeSchema() {
		rdr, err = NewDuckDBTablesSchemaBatchReader(ctx, s.Alloc, rdr, s.db, query)
		if err != nil {
			return nil, nil, err
		}
	}

	schema := rdr.Schema()
	go flight.StreamChunksFromReader(rdr, ch)
	return schema, ch, nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoXdbcTypeInfo(_ context.Context, _ flightsql.GetXdbcTypeInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.XdbcTypeInfo), nil
}

func (s *DuckDBFlightSQLServer) DoGetXdbcTypeInfo(_ context.Context, cmd flightsql.GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var batch arrow.Record
	if cmd.GetDataType() == nil {
		batch = GetTypeInfoResult(s.Alloc)
	} else {
		batch = GetFilteredTypeInfoResult(s.Alloc, *cmd.GetDataType())
	}

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)
	return batch.Schema(), ch, nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.TableTypes), nil
}

func (s *DuckDBFlightSQLServer) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// Use information_schema.tables for table types.
	query := "SELECT DISTINCT table_type AS table_type FROM information_schema.tables"
	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.TableTypes)
}

func (s *DuckDBFlightSQLServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	var (
		res sql.Result
		err error
	)

	if len(cmd.GetTransactionId()) > 0 {
		tx, loaded := s.openTransactions.Load(string(cmd.GetTransactionId()))
		if !loaded {
			return -1, status.Error(codes.InvalidArgument, "invalid transaction handle provided")
		}

		res, err = tx.(*sql.Tx).ExecContext(ctx, cmd.GetQuery())
	} else {
		res, err = s.db.ExecContext(ctx, cmd.GetQuery())
	}

	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *DuckDBFlightSQLServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (result flightsql.ActionCreatePreparedStatementResult, err error) {
	var stmt *sql.Stmt

	if len(req.GetTransactionId()) > 0 {
		tx, loaded := s.openTransactions.Load(string(req.GetTransactionId()))
		if !loaded {
			return result, status.Error(codes.InvalidArgument, "invalid transaction handle provided")
		}
		stmt, err = tx.(*sql.Tx).PrepareContext(ctx, req.GetQuery())
	} else {
		stmt, err = s.db.PrepareContext(ctx, req.GetQuery())
	}

	if err != nil {
		return result, err
	}

	handle := genRandomString()
	s.prepared.Store(string(handle), Statement{stmt: stmt})

	result.Handle = handle
	// no way to get the dataset or parameter schemas from sql.DB
	return
}

func (s *DuckDBFlightSQLServer) ClosePreparedStatement(ctx context.Context, request flightsql.ActionClosePreparedStatementRequest) error {
	handle := request.GetPreparedStatementHandle()
	if val, loaded := s.prepared.LoadAndDelete(string(handle)); loaded {
		stmt := val.(Statement)
		return stmt.stmt.Close()
	}

	return status.Error(codes.InvalidArgument, "prepared statement not found")
}

func (s *DuckDBFlightSQLServer) GetFlightInfoPreparedStatement(_ context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	_, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

type dbQueryCtx interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func doGetQuery(ctx context.Context, mem memory.Allocator, db dbQueryCtx, query string, schema *arrow.Schema, args ...interface{}) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		// Not really useful except for testing Flight SQL clients
		trailers := metadata.Pairs("afsql-duckdb-query", query)
		grpc.SetTrailer(ctx, trailers)
		return nil, nil, err
	}

	var rdr *SqlBatchReader
	if schema != nil {
		rdr, err = NewDuckDBBatchReaderWithSchema(mem, schema, rows)
	} else {
		rdr, err = NewDuckDBBatchReader(mem, rows)
		if err == nil {
			schema = rdr.schema
		}
	}

	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(rdr, ch)
	return schema, ch, nil
}

func (s *DuckDBFlightSQLServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (schema *arrow.Schema, out <-chan flight.StreamChunk, err error) {
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return nil, nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt := val.(Statement)
	readers := make([]array.RecordReader, 0, len(stmt.params))
	if len(stmt.params) == 0 {
		rows, err := stmt.stmt.QueryContext(ctx)
		if err != nil {
			return nil, nil, err
		}

		rdr, err := NewDuckDBBatchReader(s.Alloc, rows)
		if err != nil {
			return nil, nil, err
		}

		schema = rdr.schema
		readers = append(readers, rdr)
	} else {
		defer func() {
			if err != nil {
				for _, r := range readers {
					r.Release()
				}
			}
		}()
		var (
			rows *sql.Rows
			rdr  *SqlBatchReader
		)
		// if we have multiple rows of bound params, execute the query
		// multiple times and concatenate the result sets.
		for _, p := range stmt.params {
			rows, err = stmt.stmt.QueryContext(ctx, p...)
			if err != nil {
				return nil, nil, err
			}

			if schema == nil {
				rdr, err = NewDuckDBBatchReader(s.Alloc, rows)
				if err != nil {
					return nil, nil, err
				}
				schema = rdr.schema
			} else {
				rdr, err = NewDuckDBBatchReaderWithSchema(s.Alloc, schema, rows)
				if err != nil {
					return nil, nil, err
				}
			}

			readers = append(readers, rdr)
		}
	}

	ch := make(chan flight.StreamChunk)
	go flight.ConcatenateReaders(readers, ch)
	out = ch
	return
}

func scalarToIFace(s scalar.Scalar) (interface{}, error) {
	if !s.IsValid() {
		return nil, nil
	}

	switch val := s.(type) {
	case *scalar.Int8:
		return val.Value, nil
	case *scalar.Uint8:
		return val.Value, nil
	case *scalar.Int32:
		return val.Value, nil
	case *scalar.Int64:
		return val.Value, nil
	case *scalar.Float32:
		return val.Value, nil
	case *scalar.Float64:
		return val.Value, nil
	case *scalar.String:
		return string(val.Value.Bytes()), nil
	case *scalar.Binary:
		return val.Value.Bytes(), nil
	case scalar.DateScalar:
		return val.ToTime(), nil
	case scalar.TimeScalar:
		return val.ToTime(), nil
	case *scalar.DenseUnion:
		return scalarToIFace(val.Value)
	default:
		return nil, fmt.Errorf("unsupported type: %s", val)
	}
}

func getParamsForStatement(rdr flight.MessageReader) (params [][]interface{}, err error) {
	params = make([][]interface{}, 0)
	for rdr.Next() {
		rec := rdr.Record()

		nrows := int(rec.NumRows())
		ncols := int(rec.NumCols())

		for i := 0; i < nrows; i++ {
			invokeParams := make([]interface{}, ncols)
			for c := 0; c < ncols; c++ {
				col := rec.Column(c)
				sc, err := scalar.GetScalar(col, i)
				if err != nil {
					return nil, err
				}
				if r, ok := sc.(scalar.Releasable); ok {
					r.Release()
				}

				invokeParams[c], err = scalarToIFace(sc)
				if err != nil {
					return nil, err
				}
			}
			params = append(params, invokeParams)
		}
	}

	return params, rdr.Err()
}

func (s *DuckDBFlightSQLServer) DoPutPreparedStatementQuery(_ context.Context, cmd flightsql.PreparedStatementQuery, rdr flight.MessageReader, _ flight.MetadataWriter) ([]byte, error) {
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt := val.(Statement)
	args, err := getParamsForStatement(rdr)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error gathering parameters for prepared statement query: %s", err.Error())
	}

	stmt.params = args
	s.prepared.Store(string(cmd.GetPreparedStatementHandle()), stmt)
	return cmd.GetPreparedStatementHandle(), nil
}

func (s *DuckDBFlightSQLServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, rdr flight.MessageReader) (int64, error) {
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return 0, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt := val.(Statement)
	args, err := getParamsForStatement(rdr)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "error gathering parameters for prepared statement: %s", err.Error())
	}

	if len(args) == 0 {
		result, err := stmt.stmt.ExecContext(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "no such table") {
				return 0, status.Error(codes.NotFound, err.Error())
			}
			return 0, err
		}

		return result.RowsAffected()
	}

	var totalAffected int64
	for _, p := range args {
		result, err := stmt.stmt.ExecContext(ctx, p...)
		if err != nil {
			if strings.Contains(err.Error(), "no such table") {
				return totalAffected, status.Error(codes.NotFound, err.Error())
			}
			return totalAffected, err
		}

		n, err := result.RowsAffected()
		if err != nil {
			return totalAffected, err
		}
		totalAffected += n
	}

	return totalAffected, nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoPrimaryKeys(_ context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.PrimaryKeys), nil
}

func (s *DuckDBFlightSQLServer) DoGetPrimaryKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var b strings.Builder

	b.WriteString(`
SELECT
    kcu.table_catalog AS catalog_name,
    kcu.table_schema AS schema_name,
    kcu.table_name,
    kcu.column_name,
    kcu.ordinal_position AS key_sequence,
    NULL as key_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
`)
	if cmd.Catalog != nil {
		fmt.Fprintf(&b, " AND kcu.table_catalog LIKE '%s'", *cmd.Catalog)
	}
	if cmd.DBSchema != nil {
		fmt.Fprintf(&b, " AND kcu.table_schema LIKE '%s'", *cmd.DBSchema)
	}

	fmt.Fprintf(&b, " AND kcu.table_name LIKE '%s'", cmd.Table)

	return doGetQuery(ctx, s.Alloc, s.db, b.String(), schema_ref.PrimaryKeys)
}

func (s *DuckDBFlightSQLServer) GetFlightInfoImportedKeys(_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.ImportedKeys), nil
}

func (s *DuckDBFlightSQLServer) DoGetImportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	filter := "fk_table_name = '" + ref.Table + "'"
	if ref.Catalog != nil {
		filter += " AND fk_catalog_name = '" + *ref.Catalog + "'"
	}
	if ref.DBSchema != nil {
		filter += " AND fk_schema_name = '" + *ref.DBSchema + "'"
	}
	query := prepareQueryForGetKeys(filter)
	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.ImportedKeys)
}

func (s *DuckDBFlightSQLServer) GetFlightInfoExportedKeys(_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.ExportedKeys), nil
}

func (s *DuckDBFlightSQLServer) DoGetExportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	filter := "pk_table_name = '" + ref.Table + "'"
	if ref.Catalog != nil {
		filter += " AND pk_catalog_name = '" + *ref.Catalog + "'"
	}
	if ref.DBSchema != nil {
		filter += " AND pk_schema_name = '" + *ref.DBSchema + "'"
	}
	query := prepareQueryForGetKeys(filter)
	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.ExportedKeys)
}

func (s *DuckDBFlightSQLServer) GetFlightInfoCrossReference(_ context.Context, _ flightsql.CrossTableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.CrossReference), nil
}

func (s *DuckDBFlightSQLServer) DoGetCrossReference(ctx context.Context, cmd flightsql.CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	pkref := cmd.PKRef
	filter := "pk_table_name = '" + pkref.Table + "'"
	if pkref.Catalog != nil {
		filter += " AND pk_catalog_name = '" + *pkref.Catalog + "'"
	}
	if pkref.DBSchema != nil {
		filter += " AND pk_schema_name = '" + *pkref.DBSchema + "'"
	}

	fkref := cmd.FKRef
	filter += " AND fk_table_name = '" + fkref.Table + "'"
	if fkref.Catalog != nil {
		filter += " AND fk_catalog_name = '" + *fkref.Catalog + "'"
	}
	if fkref.DBSchema != nil {
		filter += " AND fk_schema_name = '" + *fkref.DBSchema + "'"
	}
	query := prepareQueryForGetKeys(filter)
	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.ExportedKeys)
}

func (s *DuckDBFlightSQLServer) BeginTransaction(ctx context.Context, req flightsql.ActionBeginTransactionRequest) ([]byte, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %s", err.Error())
	}

	handle := genRandomString()
	s.openTransactions.Store(string(handle), tx)
	return handle, nil
}

func (s *DuckDBFlightSQLServer) EndTransaction(_ context.Context, req flightsql.ActionEndTransactionRequest) error {
	if req.GetAction() == flightsql.EndTransactionUnspecified {
		return status.Error(codes.InvalidArgument, "must specify Commit or Rollback to end transaction")
	}

	handle := string(req.GetTransactionId())
	if tx, loaded := s.openTransactions.LoadAndDelete(handle); loaded {
		txn := tx.(*sql.Tx)
		switch req.GetAction() {
		case flightsql.EndTransactionCommit:
			if err := txn.Commit(); err != nil {
				return status.Error(codes.Internal, "failed to commit transaction: "+err.Error())
			}
		case flightsql.EndTransactionRollback:
			if err := txn.Rollback(); err != nil {
				return status.Error(codes.Internal, "failed to rollback transaction: "+err.Error())
			}
		}
		return nil
	}

	return status.Error(codes.InvalidArgument, "transaction id not found")
}

// DoExchange handles exchanging Arrow data between Flight SQL servers
func (s *DuckDBFlightSQLServer) DoExchange(stream flight_gen.FlightService_DoExchangeServer) error {
	for {
		data, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "error reading stream: %v", err)
		}

		// The first message contains the schema.
		if !s.tableCreated {
			schema, err := flight.DeserializeSchema(data.DataHeader, s.Alloc)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to deserialize schema: %v", err)
			}
			createStmt := generateCreateTableStmt(schema)
			if _, err := s.db.ExecContext(context.Background(), createStmt); err != nil {
				return status.Errorf(codes.Internal, "failed to create table: %v", err)
			}
			s.tableCreated = true
			s.schema = schema
			continue
		}

		// Subsequent messages contain record batches.
		messageReader, err := ipc.NewReader(bytes.NewReader(data.DataBody), ipc.WithSchema(s.schema), ipc.WithAllocator(s.Alloc))
		if err != nil {
			return status.Errorf(codes.Internal, "failed to create message reader: %v", err)
		}

		// Process each record batch in the message.
		for messageReader.Next() {
			record := messageReader.Record()
			if record == nil || record.NumRows() == 0 {
				continue
			}

			// Generate the INSERT statement with parameter placeholders.
			insertStmtStr := generateInsertStmt(record)
			stmt, err := s.db.PrepareContext(context.Background(), insertStmtStr)
			if err != nil {
				messageReader.Release()
				return status.Errorf(codes.Internal, "failed to prepare insert statement: %v", err)
			}

			// Loop through each row in the record.
			for row := 0; row < int(record.NumRows()); row++ {
				params := make([]interface{}, record.NumCols())
				// Extract values for each column.
				for col := 0; col < int(record.NumCols()); col++ {
					colArray := record.Column(col)
					sc, err := scalar.GetScalar(colArray, row)
					if err != nil {
						stmt.Close()
						messageReader.Release()
						return status.Errorf(codes.Internal, "failed to get scalar for row %d, col %d: %v", row, col, err)
					}

					value, err := scalarToIFace(sc)
					if err != nil {
						if r, ok := sc.(scalar.Releasable); ok {
							r.Release()
						}
						stmt.Close()
						messageReader.Release()
						return status.Errorf(codes.Internal, "failed to convert scalar to interface for row %d, col %d: %v", row, col, err)
					}
					params[col] = value
					if r, ok := sc.(scalar.Releasable); ok {
						r.Release()
					}
				}

				// Execute the insert with the bound parameters.
				if _, err := stmt.ExecContext(context.Background(), params...); err != nil {
					stmt.Close()
					messageReader.Release()
					return status.Errorf(codes.Internal, "failed to execute insert for row %d: %v", row, err)
				}
			}
			stmt.Close()
		}

		// Check for any errors during reading.
		if err := messageReader.Err(); err != nil {
			messageReader.Release()
			return status.Errorf(codes.Internal, "error reading record batch: %v", err)
		}
		messageReader.Release()

		// Optionally, send a response back (e.g. echoing the received data).
		if err := stream.Send(data); err != nil {
			return status.Errorf(codes.Internal, "failed to write response: %v", err)
		}
	}
	return nil
}

func generateCreateTableStmt(schema *arrow.Schema) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE exchange_data (")
	for i, field := range schema.Fields() {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(field.Name)
		b.WriteString(" ")
		b.WriteString(arrowTypeToDuckDB(field.Type))
	}
	b.WriteString(")")
	return b.String()
}

func arrowTypeToDuckDB(t arrow.DataType) string {
	switch t.ID() {
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT64:
		return "BIGINT"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING:
		return "VARCHAR"
	default:
		return "VARCHAR" // fallback
	}
}

func generateInsertStmt(record arrow.Record) string {
	var b strings.Builder
	b.WriteString("INSERT INTO exchange_data (")
	for i, field := range record.Schema().Fields() {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(field.Name)
	}
	b.WriteString(") VALUES (")
	for i := 0; i < int(record.NumCols()); i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("?")
	}
	b.WriteString(")")
	return b.String()
}
