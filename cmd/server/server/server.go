package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	flightsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	flightpb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/TFMV/flight/cmd/server/config"
	"github.com/TFMV/flight/cmd/server/middleware"
	"github.com/TFMV/flight/pkg/cache"
	"github.com/TFMV/flight/pkg/handlers"
	"github.com/TFMV/flight/pkg/infrastructure"
	"github.com/TFMV/flight/pkg/infrastructure/pool"
	"github.com/TFMV/flight/pkg/repositories/duckdb"
	"github.com/TFMV/flight/pkg/services"
)

// FlightSQLServer implements the Flight SQL protocol.
type FlightSQLServer struct {
	flightsql.BaseServer

	// Configuration
	config *config.Config

	// Core components
	pool        pool.ConnectionPool
	allocator   memory.Allocator
	logger      zerolog.Logger
	metrics     MetricsCollector
	memoryCache cache.Cache
	cacheKeyGen cache.CacheKeyGenerator

	// Handlers
	queryHandler             handlers.QueryHandler
	metadataHandler          handlers.MetadataHandler
	transactionHandler       handlers.TransactionHandler
	preparedStatementHandler handlers.PreparedStatementHandler

	// Services
	queryService             services.QueryService
	metadataService          services.MetadataService
	transactionService       services.TransactionService
	preparedStatementService services.PreparedStatementService

	// State
	mu       sync.RWMutex
	sessions map[string]*Session
	closing  bool
}

// Session represents a client session.
type Session struct {
	ID            string
	User          string
	TransactionID string
	Properties    map[string]interface{}
}

// MetricsCollector defines the metrics interface.
type MetricsCollector interface {
	IncrementCounter(name string, labels ...string)
	RecordHistogram(name string, value float64, labels ...string)
	RecordGauge(name string, value float64, labels ...string)
	StartTimer(name string) Timer
}

// Timer represents a timing measurement.
type Timer interface {
	Stop() float64
}

// New creates a new Flight SQL server.
func New(cfg *config.Config, logger zerolog.Logger, metrics MetricsCollector) (*FlightSQLServer, error) {
	// Create server instance
	srv := &FlightSQLServer{
		config:   cfg,
		logger:   logger,
		metrics:  metrics,
		sessions: make(map[string]*Session),
	}

	// Create memory allocator
	srv.allocator = memory.NewGoAllocator()

	// Initialize cache components
	if cfg.Cache.Enabled {
		srv.memoryCache = cache.NewMemoryCache(cfg.Cache.MaxSize, srv.allocator)
		srv.cacheKeyGen = &cache.DefaultCacheKeyGenerator{}
	}

	// Create connection pool
	poolCfg := pool.Config{
		DSN:                cfg.Database,
		MaxOpenConnections: cfg.ConnectionPool.MaxOpenConnections,
		MaxIdleConnections: cfg.ConnectionPool.MaxIdleConnections,
		ConnMaxLifetime:    cfg.ConnectionPool.ConnMaxLifetime,
		ConnMaxIdleTime:    cfg.ConnectionPool.ConnMaxIdleTime,
		HealthCheckPeriod:  cfg.ConnectionPool.HealthCheckPeriod,
		ConnectionTimeout:  cfg.ConnectionTimeout,
	}

	var err error
	srv.pool, err = pool.New(poolCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Create adapters
	logAdapter := &loggerAdapter{logger: logger}
	handlerMetricsAdapter := &handlerMetricsAdapter{collector: metrics}
	serviceMetricsAdapter := &serviceMetricsAdapter{collector: metrics}

	// Create SQL info provider
	sqlInfo := infrastructure.NewSQLInfoProvider(srv.allocator)

	// Create repositories
	queryRepo := duckdb.NewQueryRepository(srv.pool, srv.allocator, logger)
	metadataRepo := duckdb.NewMetadataRepository(srv.pool, sqlInfo, logger)
	transactionRepo := duckdb.NewTransactionRepository(srv.pool, logger)
	preparedStatementRepo := duckdb.NewPreparedStatementRepository(srv.pool, srv.allocator, logger)

	// Create services
	transactionService := services.NewTransactionService(transactionRepo, 30*time.Minute, logAdapter, serviceMetricsAdapter)
	queryService := services.NewQueryService(queryRepo, transactionService, logAdapter, serviceMetricsAdapter)
	metadataService := services.NewMetadataService(metadataRepo, logAdapter, serviceMetricsAdapter)
	preparedStatementService := services.NewPreparedStatementService(preparedStatementRepo, transactionService, logAdapter, serviceMetricsAdapter)

	// Create handlers
	srv.queryHandler = handlers.NewQueryHandler(queryService, srv.allocator, logAdapter, handlerMetricsAdapter)
	srv.metadataHandler = handlers.NewMetadataHandler(metadataService, srv.allocator, logAdapter, handlerMetricsAdapter)
	srv.transactionHandler = handlers.NewTransactionHandler(transactionService, logAdapter, handlerMetricsAdapter)
	srv.preparedStatementHandler = handlers.NewPreparedStatementHandler(preparedStatementService, queryService, srv.allocator, logAdapter, handlerMetricsAdapter)

	// Initialize the base server
	srv.BaseServer = flightsql.BaseServer{}

	// Register SQL info
	if err := srv.registerSqlInfo(); err != nil {
		return nil, fmt.Errorf("failed to register SQL info: %w", err)
	}

	return srv, nil
}

// Register registers the Flight SQL server with a gRPC server.
func (s *FlightSQLServer) Register(grpcServer *grpc.Server) {
	srv := flightsql.NewFlightServer(s)
	flight.RegisterFlightServiceServer(grpcServer, srv)
}

// GetMiddleware returns gRPC middleware for the server.
func (s *FlightSQLServer) GetMiddleware() []grpc.ServerOption {
	var opts []grpc.ServerOption

	// Add authentication middleware if enabled
	if s.config.Auth.Enabled {
		authMiddleware := middleware.NewAuthMiddleware(s.config.Auth, s.logger)
		opts = append(opts, grpc.UnaryInterceptor(authMiddleware.UnaryInterceptor()))
		opts = append(opts, grpc.StreamInterceptor(authMiddleware.StreamInterceptor()))
	}

	// Add logging middleware
	loggingMiddleware := middleware.NewLoggingMiddleware(s.logger)
	opts = append(opts, grpc.ChainUnaryInterceptor(loggingMiddleware.UnaryInterceptor()))
	opts = append(opts, grpc.ChainStreamInterceptor(loggingMiddleware.StreamInterceptor()))

	// Create metrics adapter for middleware
	middlewareMetrics := &middlewareMetricsAdapter{s.metrics}
	metricsMiddleware := middleware.NewMetricsMiddleware(middlewareMetrics)
	opts = append(opts, grpc.ChainUnaryInterceptor(metricsMiddleware.UnaryInterceptor()))
	opts = append(opts, grpc.ChainStreamInterceptor(metricsMiddleware.StreamInterceptor()))

	// Add recovery middleware
	recoveryMiddleware := middleware.NewRecoveryMiddleware(s.logger)
	opts = append(opts, grpc.ChainUnaryInterceptor(recoveryMiddleware.UnaryInterceptor()))
	opts = append(opts, grpc.ChainStreamInterceptor(recoveryMiddleware.StreamInterceptor()))

	return opts
}

// Close gracefully shuts down the server.
func (s *FlightSQLServer) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()

	s.logger.Info().Msg("Closing Flight SQL server")

	// Stop transaction service if it has a Stop method
	if stopper, ok := s.transactionService.(interface{ Stop() }); ok {
		stopper.Stop()
	}

	// Close cache
	if err := s.memoryCache.Close(); err != nil {
		s.logger.Error().Err(err).Msg("Error closing cache")
	}

	// Close connection pool
	if err := s.pool.Close(); err != nil {
		s.logger.Error().Err(err).Msg("Error closing connection pool")
	}

	s.logger.Info().Msg("Flight SQL server closed")
	return nil
}

// orEmpty returns an empty string if the pointer is nil, otherwise the pointed value
func orEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// GetFlightInfoStatement implements the FlightSQL interface.
func (s *FlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_statement")
	defer timer.Stop()

	s.logger.Debug().Str("query", cmd.GetQuery()).Msg("GetFlightInfoStatement")

	info, err := s.queryHandler.GetFlightInfo(ctx, cmd.GetQuery())
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoStatement")
		return nil, err
	}

	// Create a statement query command
	cmdProto := &flightpb.CommandStatementQuery{
		Query: cmd.GetQuery(),
	}

	// Marshal the command using the correct protobuf message type
	cmdBytes, err := proto.Marshal(cmdProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal statement query command")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoStatement")
		return nil, status.Error(codes.Internal, "failed to create ticket")
	}

	// Create the flight info with the already serialized schema
	flightInfo := &flight.FlightInfo{
		Schema:           info.Schema, // info.Schema is already serialized
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: cmdBytes},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}

	s.metrics.IncrementCounter("flight_get_info_statement_success")
	return flightInfo, nil
}

// DoGetStatement implements the FlightSQL interface for regular statements.
func (s *FlightSQLServer) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_statement")
	defer timer.Stop()

	// Parse the command from the ticket
	cmdProto := &flightpb.CommandStatementQuery{}
	if err := proto.Unmarshal(ticket.GetStatementHandle(), cmdProto); err != nil {
		s.logger.Error().Err(err).
			Str("ticket_bytes", fmt.Sprintf("%x", ticket.GetStatementHandle())).
			Msg("Failed to unmarshal statement query command")
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetStatement", "error", "unmarshal_failed")
		return nil, nil, status.Error(codes.Internal, fmt.Sprintf("failed to unmarshal query from ticket: %v", err))
	}

	s.logger.Debug().Str("query", cmdProto.GetQuery()).Msg("DoGetStatement")

	// For COUNT queries, we need to ensure we get a single row with a single column
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(cmdProto.GetQuery())), "SELECT COUNT") {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		}, nil)

		chunks := make(chan flight.StreamChunk, 1)
		go func() {
			defer close(chunks)

			// Execute the query
			_, result, err := s.queryHandler.ExecuteQueryAndStream(ctx, cmdProto.GetQuery())
			if err != nil {
				s.logger.Error().Err(err).Msg("Failed to execute COUNT query")
				return
			}

			// Read all chunks to get the count
			var count int64
			for chunk := range result {
				if chunk.Data != nil && chunk.Data.NumRows() > 0 {
					countCol := chunk.Data.Column(0)
					if countCol != nil {
						count = countCol.(*array.Int64).Value(0)
						break
					}
				}
			}

			// Create a new record with the count
			builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			builder.Field(0).(*array.Int64Builder).Append(count)
			record := builder.NewRecord()
			chunks <- flight.StreamChunk{Data: record}
		}()

		return schema, chunks, nil
	}

	schema, stream, err := s.queryHandler.ExecuteQueryAndStream(ctx, cmdProto.GetQuery())
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetStatement", "error", "query_failed")
		return nil, nil, err
	}

	s.metrics.IncrementCounter("flight_do_get_statement_success")
	return schema, stream, nil
}

// DoPutCommandStatementUpdate implements the FlightSQL interface.
func (s *FlightSQLServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	timer := s.metrics.StartTimer("flight_do_put_statement_update")
	defer timer.Stop()

	s.logger.Debug().Str("query", cmd.GetQuery()).Msg("DoPutCommandStatementUpdate")

	rowsAffected, err := s.queryHandler.ExecuteUpdate(ctx, cmd.GetQuery(), string(cmd.GetTransactionId()))
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutCommandStatementUpdate")
		return 0, err
	}

	s.metrics.RecordHistogram("flight_update_rows", float64(rowsAffected))
	return rowsAffected, nil
}

// GetFlightInfoCatalogs implements the FlightSQL interface.
func (s *FlightSQLServer) GetFlightInfoCatalogs(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_catalogs")
	defer timer.Stop()

	s.logger.Debug().Msg("GetFlightInfoCatalogs")

	schema, _, err := s.metadataHandler.GetCatalogs(ctx)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoCatalogs")
		return nil, err
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

// DoGetCatalogs implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_catalogs")
	defer timer.Stop()

	s.logger.Debug().Msg("DoGetCatalogs")

	return s.metadataHandler.GetCatalogs(ctx)
}

// GetFlightInfoSchemas implements the FlightSQL interface.
func (s *FlightSQLServer) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_schemas")
	defer timer.Stop()

	s.logger.Debug().Msg("GetFlightInfoSchemas")

	catalog := cmd.GetCatalog()
	pattern := cmd.GetDBSchemaFilterPattern()

	schema, _, err := s.metadataHandler.GetSchemas(ctx, catalog, pattern)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoSchemas")
		return nil, err
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

// DoGetDBSchemas implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_schemas")
	defer timer.Stop()

	s.logger.Debug().Msg("DoGetDBSchemas")

	catalog := cmd.GetCatalog()
	pattern := cmd.GetDBSchemaFilterPattern()

	return s.metadataHandler.GetSchemas(ctx, catalog, pattern)
}

// GetFlightInfoTables implements the FlightSQL interface.
func (s *FlightSQLServer) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_tables")
	defer timer.Stop()

	s.logger.Debug().Msg("GetFlightInfoTables")

	catalog := cmd.GetCatalog()
	schemaPattern := cmd.GetDBSchemaFilterPattern()
	tablePattern := cmd.GetTableNameFilterPattern()
	tableTypes := cmd.GetTableTypes()
	includeSchema := cmd.GetIncludeSchema()

	schema, _, err := s.metadataHandler.GetTables(ctx, catalog, schemaPattern, tablePattern, tableTypes, includeSchema)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoTables")
		return nil, err
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

// DoGetTables implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_tables")
	defer timer.Stop()

	s.logger.Debug().Msg("DoGetTables")

	catalog := cmd.GetCatalog()
	schemaPattern := cmd.GetDBSchemaFilterPattern()
	tablePattern := cmd.GetTableNameFilterPattern()
	tableTypes := cmd.GetTableTypes()
	includeSchema := cmd.GetIncludeSchema()

	return s.metadataHandler.GetTables(ctx, catalog, schemaPattern, tablePattern, tableTypes, includeSchema)
}

// BeginTransaction implements the FlightSQL interface.
func (s *FlightSQLServer) BeginTransaction(ctx context.Context, req flightsql.ActionBeginTransactionRequest) ([]byte, error) {
	timer := s.metrics.StartTimer("flight_begin_transaction")
	defer timer.Stop()

	s.logger.Debug().Msg("BeginTransaction")

	// For now, assume read-write transactions
	txnID, err := s.transactionHandler.Begin(ctx, false)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "BeginTransaction")
		return nil, err
	}

	s.metrics.IncrementCounter("flight_transactions_started")
	return []byte(txnID), nil
}

// EndTransaction implements the FlightSQL interface.
func (s *FlightSQLServer) EndTransaction(ctx context.Context, req flightsql.ActionEndTransactionRequest) error {
	timer := s.metrics.StartTimer("flight_end_transaction")
	defer timer.Stop()

	txnID := string(req.GetTransactionId())
	action := req.GetAction()

	s.logger.Debug().
		Str("transaction_id", txnID).
		Str("action", action.String()).
		Msg("EndTransaction")

	switch action {
	case flightsql.EndTransactionCommit:
		if err := s.transactionHandler.Commit(ctx, txnID); err != nil {
			s.metrics.IncrementCounter("flight_errors", "method", "EndTransaction", "action", "commit")
			return err
		}
		s.metrics.IncrementCounter("flight_transactions_committed")
	case flightsql.EndTransactionRollback:
		if err := s.transactionHandler.Rollback(ctx, txnID); err != nil {
			s.metrics.IncrementCounter("flight_errors", "method", "EndTransaction", "action", "rollback")
			return err
		}
		s.metrics.IncrementCounter("flight_transactions_rolled_back")
	default:
		return status.Errorf(codes.InvalidArgument, "unknown transaction action: %v", action)
	}

	return nil
}

// CreatePreparedStatement implements the FlightSQL interface.
func (s *FlightSQLServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	timer := s.metrics.StartTimer("flight_create_prepared_statement")
	defer timer.Stop()

	s.logger.Debug().Str("query", req.GetQuery()).Msg("CreatePreparedStatement")

	handle, schema, err := s.preparedStatementHandler.Create(ctx, req.GetQuery(), string(req.GetTransactionId()))
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "CreatePreparedStatement")
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	// Get parameter schema separately
	paramSchema, err := s.preparedStatementHandler.GetParameterSchema(ctx, handle)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "CreatePreparedStatement")
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	s.metrics.IncrementCounter("flight_prepared_statements_created")

	return flightsql.ActionCreatePreparedStatementResult{
		Handle:          []byte(handle),
		DatasetSchema:   schema,
		ParameterSchema: paramSchema,
	}, nil
}

// GetFlightInfoPreparedStatement implements the FlightSQL interface.
func (s *FlightSQLServer) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_prepared_statement")
	defer timer.Stop()

	s.logger.Debug().Str("handle", string(cmd.GetPreparedStatementHandle())).Msg("GetFlightInfoPreparedStatement")

	schema, err := s.preparedStatementHandler.GetSchema(ctx, string(cmd.GetPreparedStatementHandle()))
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoPreparedStatement", "error", "get_schema_failed")
		return nil, err
	}

	// Create the prepared statement command
	preparedCmd := &flightpb.CommandPreparedStatementQuery{
		PreparedStatementHandle: cmd.GetPreparedStatementHandle(),
	}

	// Marshal the command
	ticketBytes, err := proto.Marshal(preparedCmd)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal CommandPreparedStatementQuery")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoPreparedStatement", "error", "marshal_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal ticket command: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticketBytes},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetPreparedStatement implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_prepared_statement")
	defer timer.Stop()

	handle := string(cmd.GetPreparedStatementHandle())
	s.logger.Debug().Str("handle", handle).Msg("DoGetPreparedStatement")

	// Get the parameter schema
	paramSchema, err := s.preparedStatementHandler.GetParameterSchema(ctx, handle)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetPreparedStatement", "error", "get_param_schema_failed")
		return nil, nil, status.Error(codes.Internal, fmt.Sprintf("failed to get parameter schema: %v", err))
	}

	// Create an empty record for parameters
	var boundParams arrow.Record
	if paramSchema.NumFields() > 0 {
		b := array.NewRecordBuilder(s.allocator, paramSchema)
		defer b.Release()
		boundParams = b.NewRecord()
		defer boundParams.Release()
	} else {
		boundParams = array.NewRecord(paramSchema, nil, 0)
		defer boundParams.Release()
	}

	// Execute the prepared statement
	schema, chunks, err := s.preparedStatementHandler.ExecuteQuery(ctx, handle, boundParams)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetPreparedStatement", "error", "execute_failed")
		return nil, nil, status.Error(codes.Internal, fmt.Sprintf("failed to execute prepared statement: %v", err))
	}

	s.metrics.IncrementCounter("flight_do_get_prepared_statement_success")
	return schema, chunks, nil
}

// DoPutPreparedStatementQuery handles prepared statement parameter binding
func (s *FlightSQLServer) DoPutPreparedStatementQuery(
	ctx context.Context,
	cmd flightsql.PreparedStatementQuery,
	reader flight.MessageReader,
	writer flight.MetadataWriter,
) ([]byte, error) {
	timer := s.metrics.StartTimer("flight_do_put_prepared_statement_query")
	defer timer.Stop()

	handle := string(cmd.GetPreparedStatementHandle())
	s.logger.Debug().
		Str("handle", handle).
		Msg("DoPutPreparedStatementQuery")

	// Get the parameter schema first
	paramSchema, err := s.preparedStatementHandler.GetParameterSchema(ctx, handle)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "get_param_schema_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get parameter schema: %v", err))
	}

	// Read the parameter record batch
	record, err := reader.Read()
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "read_params_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to read parameter record: %v", err))
	}
	defer record.Release()

	// Validate parameter schema matches
	if !record.Schema().Equal(paramSchema) {
		s.logger.Error().
			Str("expected_schema", paramSchema.String()).
			Str("actual_schema", record.Schema().String()).
			Msg("Parameter schema mismatch")
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "schema_mismatch")
		return nil, status.Error(codes.InvalidArgument, "parameter schema mismatch")
	}

	// Execute the prepared statement with parameters
	_, err = s.preparedStatementHandler.ExecuteUpdate(ctx, handle, record)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "execute_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to execute prepared statement: %v", err))
	}

	s.metrics.IncrementCounter("flight_prepared_statements_executed")
	return nil, nil // Return nil for both update info and error on success
}

// DoPutPreparedStatementUpdate implements the FlightSQL interface.
func (s *FlightSQLServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, reader flight.MessageReader) (int64, error) {
	timer := s.metrics.StartTimer("flight_do_put_prepared_statement_update")
	defer timer.Stop()

	handle := string(cmd.GetPreparedStatementHandle())
	s.logger.Debug().Str("handle", handle).Msg("DoPutPreparedStatementUpdate")

	record, err := reader.Read()
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementUpdate")
		return 0, status.Error(codes.Internal, fmt.Sprintf("failed to read parameter record: %v", err))
	}

	paramSchema, err := s.preparedStatementHandler.GetParameterSchema(ctx, handle)
	if err != nil {
		record.Release()
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementUpdate")
		return 0, status.Error(codes.Internal, fmt.Sprintf("failed to get parameter schema: %v", err))
	}

	if !record.Schema().Equal(paramSchema) {
		record.Release()
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementUpdate")
		return 0, status.Error(codes.InvalidArgument, "parameter schema mismatch")
	}

	rowsAffected, err := s.preparedStatementHandler.ExecuteUpdate(ctx, handle, record)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementUpdate")
		return 0, err
	}

	s.metrics.RecordHistogram("flight_prepared_statement_update_rows", float64(rowsAffected))
	return rowsAffected, nil
}

// ClosePreparedStatement implements the FlightSQL interface.
func (s *FlightSQLServer) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	timer := s.metrics.StartTimer("flight_close_prepared_statement")
	defer timer.Stop()

	handle := string(req.GetPreparedStatementHandle())
	s.logger.Debug().Str("handle", handle).Msg("ClosePreparedStatement")

	if err := s.preparedStatementHandler.Close(ctx, handle); err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "ClosePreparedStatement")
		return err
	}

	s.metrics.IncrementCounter("flight_prepared_statements_closed")
	return nil
}

// GetFlightInfoTableTypes implements the FlightSQL interface.
func (s *FlightSQLServer) GetFlightInfoTableTypes(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_table_types")
	defer timer.Stop()

	s.logger.Debug().Msg("GetFlightInfoTableTypes")

	schema, _, err := s.metadataHandler.GetTableTypes(ctx)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoTableTypes")
		return nil, err
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

// DoGetTableTypes implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_table_types")
	defer timer.Stop()

	s.logger.Debug().Msg("DoGetTableTypes")

	return s.metadataHandler.GetTableTypes(ctx)
}

// GetFlightInfoPrimaryKeys implements the FlightSQL interface.
func (s *FlightSQLServer) GetFlightInfoPrimaryKeys(ctx context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_primary_keys")
	defer timer.Stop()

	s.logger.Debug().
		Str("catalog", orEmpty(cmd.Catalog)).
		Str("schema", orEmpty(cmd.DBSchema)).
		Str("table", cmd.Table).
		Msg("GetFlightInfoPrimaryKeys")

	catalog := cmd.Catalog
	dbSchema := cmd.DBSchema
	table := cmd.Table

	schema, _, err := s.metadataHandler.GetPrimaryKeys(ctx, catalog, dbSchema, table)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoPrimaryKeys")
		return nil, err
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

// DoGetPrimaryKeys implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetPrimaryKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_primary_keys")
	defer timer.Stop()

	s.logger.Debug().
		Str("catalog", orEmpty(cmd.Catalog)).
		Str("schema", orEmpty(cmd.DBSchema)).
		Str("table", cmd.Table).
		Msg("DoGetPrimaryKeys")

	catalog := cmd.Catalog
	dbSchema := cmd.DBSchema
	table := cmd.Table

	return s.metadataHandler.GetPrimaryKeys(ctx, catalog, dbSchema, table)
}

// GetFlightInfoImportedKeys implements the FlightSQL interface.
func (s *FlightSQLServer) GetFlightInfoImportedKeys(ctx context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_imported_keys")
	defer timer.Stop()

	s.logger.Debug().
		Str("catalog", orEmpty(cmd.Catalog)).
		Str("schema", orEmpty(cmd.DBSchema)).
		Str("table", cmd.Table).
		Msg("GetFlightInfoImportedKeys")

	catalog := cmd.Catalog
	dbSchema := cmd.DBSchema
	table := cmd.Table

	schema, _, err := s.metadataHandler.GetImportedKeys(ctx, catalog, dbSchema, table)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoImportedKeys")
		return nil, err
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

// DoGetImportedKeys implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetImportedKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_imported_keys")
	defer timer.Stop()

	s.logger.Debug().
		Str("catalog", orEmpty(cmd.Catalog)).
		Str("schema", orEmpty(cmd.DBSchema)).
		Str("table", cmd.Table).
		Msg("DoGetImportedKeys")

	catalog := cmd.Catalog
	dbSchema := cmd.DBSchema
	table := cmd.Table

	return s.metadataHandler.GetImportedKeys(ctx, catalog, dbSchema, table)
}

// GetFlightInfoExportedKeys implements the FlightSQL interface.
func (s *FlightSQLServer) GetFlightInfoExportedKeys(ctx context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_exported_keys")
	defer timer.Stop()

	s.logger.Debug().
		Str("catalog", orEmpty(cmd.Catalog)).
		Str("schema", orEmpty(cmd.DBSchema)).
		Str("table", cmd.Table).
		Msg("GetFlightInfoExportedKeys")

	catalog := cmd.Catalog
	dbSchema := cmd.DBSchema
	table := cmd.Table

	schema, _, err := s.metadataHandler.GetExportedKeys(ctx, catalog, dbSchema, table)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoExportedKeys")
		return nil, err
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

// DoGetExportedKeys implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetExportedKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_exported_keys")
	defer timer.Stop()

	s.logger.Debug().
		Str("catalog", orEmpty(cmd.Catalog)).
		Str("schema", orEmpty(cmd.DBSchema)).
		Str("table", cmd.Table).
		Msg("DoGetExportedKeys")

	catalog := cmd.Catalog
	dbSchema := cmd.DBSchema
	table := cmd.Table

	return s.metadataHandler.GetExportedKeys(ctx, catalog, dbSchema, table)
}

// GetFlightInfoXdbcTypeInfo implements the FlightSQL interface.
func (s *FlightSQLServer) GetFlightInfoXdbcTypeInfo(ctx context.Context, cmd flightsql.GetXdbcTypeInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_xdbc_type_info")
	defer timer.Stop()

	s.logger.Debug().Msg("GetFlightInfoXdbcTypeInfo")

	dataType := cmd.GetDataType()

	schema, _, err := s.metadataHandler.GetXdbcTypeInfo(ctx, dataType)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoXdbcTypeInfo")
		return nil, err
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

// DoGetXdbcTypeInfo implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetXdbcTypeInfo(ctx context.Context, cmd flightsql.GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_xdbc_type_info")
	defer timer.Stop()

	s.logger.Debug().Msg("DoGetXdbcTypeInfo")

	dataType := cmd.GetDataType()

	return s.metadataHandler.GetXdbcTypeInfo(ctx, dataType)
}

// GetFlightInfoSqlInfo implements the FlightSQL interface.
func (s *FlightSQLServer) GetFlightInfoSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_sql_info")
	defer timer.Stop()

	s.logger.Debug().
		Int("info_count", len(cmd.GetInfo())).
		Msg("GetFlightInfoSqlInfo")

	info := cmd.GetInfo()

	schema, _, err := s.metadataHandler.GetSqlInfo(ctx, info)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoSqlInfo")
		return nil, err
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

// DoGetSqlInfo implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_sql_info")
	defer timer.Stop()

	s.logger.Debug().
		Int("info_count", len(cmd.GetInfo())).
		Msg("DoGetSqlInfo")

	info := cmd.GetInfo()

	return s.metadataHandler.GetSqlInfo(ctx, info)
}

// registerSqlInfo registers SQL info with the base server.
func (s *FlightSQLServer) registerSqlInfo() error {
	// Server info
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerName, "DuckDB Flight SQL Server"); err != nil {
		return err
	}
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerVersion, "1.0.0"); err != nil {
		return err
	}
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerArrowVersion, "18.0.0"); err != nil {
		return err
	}

	// SQL language support
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoDDLCatalog, true); err != nil {
		return err
	}
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoDDLSchema, true); err != nil {
		return err
	}
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoDDLTable, true); err != nil {
		return err
	}
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoIdentifierCase, int32(1)); err != nil { // Case sensitive
		return err
	}
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoQuotedIdentifierCase, int32(1)); err != nil { // Case sensitive
		return err
	}

	// Transaction support
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerTransaction, int32(0)); err != nil { // Transactions supported
		return err
	}
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerCancel, false); err != nil {
		return err
	}
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerStatementTimeout, int32(0)); err != nil {
		return err
	}
	if err := s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerTransactionTimeout, int32(0)); err != nil {
		return err
	}

	return nil
}

// middlewareMetricsAdapter adapts MetricsCollector to middleware.MetricsCollector interface.
type middlewareMetricsAdapter struct {
	collector MetricsCollector
}

func (m *middlewareMetricsAdapter) IncrementCounter(name string, labels ...string) {
	m.collector.IncrementCounter(name, labels...)
}

func (m *middlewareMetricsAdapter) RecordHistogram(name string, value float64, labels ...string) {
	m.collector.RecordHistogram(name, value, labels...)
}

func (m *middlewareMetricsAdapter) RecordGauge(name string, value float64, labels ...string) {
	m.collector.RecordGauge(name, value, labels...)
}

func (m *middlewareMetricsAdapter) StartTimer(name string) middleware.Timer {
	return &middlewareTimerAdapter{timer: m.collector.StartTimer(name)}
}

// middlewareTimerAdapter adapts Timer to middleware.Timer interface.
type middlewareTimerAdapter struct {
	timer Timer
}

func (t *middlewareTimerAdapter) Stop() float64 {
	return t.timer.Stop()
}
