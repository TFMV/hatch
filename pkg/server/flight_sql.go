package server

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/TFMV/flight/pkg/cache"
	"github.com/TFMV/flight/pkg/handlers"
	"github.com/TFMV/flight/pkg/infrastructure/converter"
	"github.com/TFMV/flight/pkg/infrastructure/pool"
)

// FlightSQLServer implements the Flight SQL protocol
type FlightSQLServer struct {
	flight.BaseFlightServer
	queryHandler             handlers.QueryHandler
	metadataHandler          handlers.MetadataHandler
	transactionHandler       handlers.TransactionHandler
	preparedStatementHandler handlers.PreparedStatementHandler
	pool                     pool.ConnectionPool
	converter                converter.TypeConverter
	allocator                memory.Allocator
	cache                    cache.Cache
	cacheKeyGen              cache.CacheKeyGenerator
}

// NewFlightSQLServer creates a new Flight SQL server
func NewFlightSQLServer(
	queryHandler handlers.QueryHandler,
	metadataHandler handlers.MetadataHandler,
	transactionHandler handlers.TransactionHandler,
	preparedStatementHandler handlers.PreparedStatementHandler,
	pool pool.ConnectionPool,
	converter converter.TypeConverter,
	allocator memory.Allocator,
	cache cache.Cache,
	cacheKeyGen cache.CacheKeyGenerator,
) *FlightSQLServer {
	return &FlightSQLServer{
		queryHandler:             queryHandler,
		metadataHandler:          metadataHandler,
		transactionHandler:       transactionHandler,
		preparedStatementHandler: preparedStatementHandler,
		pool:                     pool,
		converter:                converter,
		allocator:                allocator,
		cache:                    cache,
		cacheKeyGen:              cacheKeyGen,
	}
}

// GetFlightInfoStatement handles statement execution requests.
func (s *FlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery) (*flight.FlightInfo, error) {
	// Generate cache key
	cacheKey := s.cacheKeyGen.GenerateKey(cmd.GetQuery(), nil)

	// Check cache first
	if cached, err := s.cache.Get(ctx, cacheKey); err == nil && cached != nil {
		desc := &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte(cmd.GetQuery()),
		}
		return &flight.FlightInfo{
			Schema:           flight.SerializeSchema(cached.Schema(), s.allocator),
			FlightDescriptor: desc,
		}, nil
	}

	// Execute query
	info, err := s.queryHandler.GetFlightInfo(ctx, cmd.GetQuery())
	if err != nil {
		return nil, err
	}

	return info, nil
}

// DoGetStatement handles statement execution and result streaming.
func (s *FlightSQLServer) DoGetStatement(ctx context.Context, cmd flightsql.StatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	cacheKey := s.cacheKeyGen.GenerateKey(cmd.GetQuery(), nil)

	if cached, err := s.cache.Get(ctx, cacheKey); err == nil && cached != nil {
		chunks := make(chan flight.StreamChunk, 1)
		chunks <- flight.StreamChunk{
			Data: cached,
			Desc: nil,
		}
		close(chunks)
		return cached.Schema(), chunks, nil
	}

	schema, chunks, err := s.queryHandler.ExecuteStatement(ctx, cmd.GetQuery(), "")
	if err != nil {
		return nil, nil, err
	}

	// Read the first chunk to cache it, then re-stream all chunks
	out := make(chan flight.StreamChunk, 16)
	go func() {
		defer close(out)
		first := true
		for chunk := range chunks {
			if first {
				if err := s.cache.Put(ctx, cacheKey, chunk.Data); err != nil {
					fmt.Printf("Failed to cache query result: %v\n", err)
				}
				first = false
			}
			out <- chunk
		}
	}()

	return schema, out, nil
}

// DoPutCommandStatementUpdate handles Flight SQL update statement execution
func (s *FlightSQLServer) DoPutCommandStatementUpdate(
	ctx context.Context,
	request flightsql.StatementUpdate,
) (int64, error) {
	// Execute the update using the query handler
	rowsAffected, err := s.queryHandler.ExecuteUpdate(ctx, request.GetQuery(), string(request.GetTransactionId()))
	if err != nil {
		return 0, status.Error(codes.InvalidArgument, fmt.Sprintf("failed to execute update: %v", err))
	}

	return rowsAffected, nil
}

// GetFlightInfoCatalogs handles catalog metadata requests
func (s *FlightSQLServer) GetFlightInfoCatalogs(
	ctx context.Context,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, _, err := s.metadataHandler.GetCatalogs(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get catalogs: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetCatalogs handles catalog metadata streaming
func (s *FlightSQLServer) DoGetCatalogs(
	ctx context.Context,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetCatalogs(ctx)
}

// GetFlightInfoSchemas handles schema metadata requests
func (s *FlightSQLServer) GetFlightInfoSchemas(
	ctx context.Context,
	cmd flightsql.GetDBSchemas,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, _, err := s.metadataHandler.GetSchemas(ctx, cmd.GetCatalog(), cmd.GetDBSchemaFilterPattern())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get schemas: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetDBSchemas handles schema metadata streaming
func (s *FlightSQLServer) DoGetDBSchemas(
	ctx context.Context,
	cmd flightsql.GetDBSchemas,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetSchemas(ctx, cmd.GetCatalog(), cmd.GetDBSchemaFilterPattern())
}

// GetFlightInfoTables handles table metadata requests
func (s *FlightSQLServer) GetFlightInfoTables(
	ctx context.Context,
	cmd flightsql.GetTables,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, _, err := s.metadataHandler.GetTables(
		ctx,
		cmd.GetCatalog(),
		cmd.GetDBSchemaFilterPattern(),
		cmd.GetTableNameFilterPattern(),
		cmd.GetTableTypes(),
		cmd.GetIncludeSchema(),
	)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get tables: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetTables handles table metadata streaming
func (s *FlightSQLServer) DoGetTables(
	ctx context.Context,
	cmd flightsql.GetTables,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetTables(
		ctx,
		cmd.GetCatalog(),
		cmd.GetDBSchemaFilterPattern(),
		cmd.GetTableNameFilterPattern(),
		cmd.GetTableTypes(),
		cmd.GetIncludeSchema(),
	)
}

// GetFlightInfoTableTypes handles table type metadata requests
func (s *FlightSQLServer) GetFlightInfoTableTypes(
	ctx context.Context,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, _, err := s.metadataHandler.GetTableTypes(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get table types: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetTableTypes handles table type metadata streaming
func (s *FlightSQLServer) DoGetTableTypes(
	ctx context.Context,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetTableTypes(ctx)
}

// GetFlightInfoPrimaryKeys handles primary key metadata requests
func (s *FlightSQLServer) GetFlightInfoPrimaryKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, _, err := s.metadataHandler.GetPrimaryKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get primary keys: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetPrimaryKeys handles primary key metadata streaming
func (s *FlightSQLServer) DoGetPrimaryKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetPrimaryKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
}

// GetFlightInfoImportedKeys handles imported key metadata requests
func (s *FlightSQLServer) GetFlightInfoImportedKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, _, err := s.metadataHandler.GetImportedKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get imported keys: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetImportedKeys handles imported key metadata streaming
func (s *FlightSQLServer) DoGetImportedKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetImportedKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
}

// GetFlightInfoExportedKeys handles exported key metadata requests
func (s *FlightSQLServer) GetFlightInfoExportedKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, _, err := s.metadataHandler.GetExportedKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get exported keys: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetExportedKeys handles exported key metadata streaming
func (s *FlightSQLServer) DoGetExportedKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetExportedKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
}

// GetFlightInfoXdbcTypeInfo handles XDBC type info metadata requests
func (s *FlightSQLServer) GetFlightInfoXdbcTypeInfo(
	ctx context.Context,
	cmd flightsql.GetXdbcTypeInfo,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, _, err := s.metadataHandler.GetXdbcTypeInfo(ctx, cmd.GetDataType())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get XDBC type info: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetXdbcTypeInfo handles XDBC type info metadata streaming
func (s *FlightSQLServer) DoGetXdbcTypeInfo(
	ctx context.Context,
	cmd flightsql.GetXdbcTypeInfo,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetXdbcTypeInfo(ctx, cmd.GetDataType())
}

// GetFlightInfoSqlInfo handles SQL info metadata requests
func (s *FlightSQLServer) GetFlightInfoSqlInfo(
	ctx context.Context,
	cmd flightsql.GetSqlInfo,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, _, err := s.metadataHandler.GetSqlInfo(ctx, cmd.GetInfo())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get SQL info: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetSqlInfo handles SQL info metadata streaming
func (s *FlightSQLServer) DoGetSqlInfo(
	ctx context.Context,
	cmd flightsql.GetSqlInfo,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetSqlInfo(ctx, cmd.GetInfo())
}

// BeginTransaction handles transaction start requests
func (s *FlightSQLServer) BeginTransaction(
	ctx context.Context,
	request flightsql.ActionBeginTransactionRequest,
) ([]byte, error) {
	txID, err := s.transactionHandler.Begin(ctx, false)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to begin transaction: %v", err))
	}
	return []byte(txID), nil
}

// EndTransaction handles transaction end requests
func (s *FlightSQLServer) EndTransaction(
	ctx context.Context,
	request flightsql.ActionEndTransactionRequest,
) error {
	txID := string(request.GetTransactionId())
	if request.GetAction() == flightsql.EndTransactionCommit {
		err := s.transactionHandler.Commit(ctx, txID)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("failed to commit transaction: %v", err))
		}
	} else {
		err := s.transactionHandler.Rollback(ctx, txID)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("failed to rollback transaction: %v", err))
		}
	}
	return nil
}

// CreatePreparedStatement handles prepared statement creation requests
func (s *FlightSQLServer) CreatePreparedStatement(
	ctx context.Context,
	request flightsql.ActionCreatePreparedStatementRequest,
) (flightsql.ActionCreatePreparedStatementResult, error) {
	handle, schema, err := s.preparedStatementHandler.Create(ctx, request.GetQuery(), string(request.GetTransactionId()))
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, status.Error(codes.Internal, fmt.Sprintf("failed to create prepared statement: %v", err))
	}

	return flightsql.ActionCreatePreparedStatementResult{
		Handle:        []byte(handle),
		DatasetSchema: schema,
	}, nil
}

// ClosePreparedStatement handles prepared statement cleanup requests
func (s *FlightSQLServer) ClosePreparedStatement(
	ctx context.Context,
	request flightsql.ActionClosePreparedStatementRequest,
) error {
	err := s.preparedStatementHandler.Close(ctx, string(request.GetPreparedStatementHandle()))
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to close prepared statement: %v", err))
	}
	return nil
}

// GetFlightInfoPreparedStatement handles prepared statement execution requests
func (s *FlightSQLServer) GetFlightInfoPreparedStatement(
	ctx context.Context,
	cmd flightsql.PreparedStatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, err := s.preparedStatementHandler.GetSchema(ctx, string(cmd.GetPreparedStatementHandle()))
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get prepared statement schema: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetPreparedStatement handles prepared statement execution and streaming
func (s *FlightSQLServer) DoGetPreparedStatement(
	ctx context.Context,
	cmd flightsql.PreparedStatementQuery,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// Get the parameter schema
	paramSchema, err := s.preparedStatementHandler.GetParameterSchema(ctx, string(cmd.GetPreparedStatementHandle()))
	if err != nil {
		return nil, nil, status.Error(codes.Internal, fmt.Sprintf("failed to get parameter schema: %v", err))
	}

	// Create an empty record for now since we don't have parameters
	record := array.NewRecord(paramSchema, nil, 0)
	defer record.Release()

	return s.preparedStatementHandler.ExecuteQuery(ctx, string(cmd.GetPreparedStatementHandle()), record)
}

// DoPutPreparedStatementQuery handles prepared statement parameter binding
func (s *FlightSQLServer) DoPutPreparedStatementQuery(
	ctx context.Context,
	cmd flightsql.PreparedStatementQuery,
	reader flight.MessageReader,
) error {
	// Read the parameter record batch
	record, err := reader.Read()
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to read parameter record: %v", err))
	}
	defer record.Release()

	// Execute the prepared statement
	_, _, err = s.preparedStatementHandler.ExecuteQuery(ctx, string(cmd.GetPreparedStatementHandle()), record)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to execute prepared statement: %v", err))
	}

	return nil
}

// DoPutPreparedStatementUpdate handles prepared statement update execution
func (s *FlightSQLServer) DoPutPreparedStatementUpdate(
	ctx context.Context,
	cmd flightsql.PreparedStatementUpdate,
	reader flight.MessageReader,
) (int64, error) {
	// Read the parameter record batch
	record, err := reader.Read()
	if err != nil {
		return 0, status.Error(codes.Internal, fmt.Sprintf("failed to read parameter record: %v", err))
	}
	defer record.Release()

	// Execute the prepared update
	rowsAffected, err := s.preparedStatementHandler.ExecuteUpdate(ctx, string(cmd.GetPreparedStatementHandle()), record)
	if err != nil {
		return 0, status.Error(codes.Internal, fmt.Sprintf("failed to execute prepared update: %v", err))
	}

	return rowsAffected, nil
}
