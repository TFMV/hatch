package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

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
	"google.golang.org/protobuf/types/known/anypb"

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
	transactionService services.TransactionService

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
	srv.transactionService = transactionService

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

	query := cleanQuery(cmd.GetQuery())
	s.logger.Debug().
		Str("query", query).
		Msg("GetFlightInfoStatement")

	info, err := s.queryHandler.GetFlightInfo(ctx, query)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoStatement")
		return nil, err
	}

	// For simple statement queries, the ticket can just be the query string itself.
	ticketBytes := []byte(query)

	// Create the flight info
	flightInfo := &flight.FlightInfo{
		Schema: info.Schema, // Schema of the result
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  ticketBytes, // Use the query bytes for the descriptor command
		},
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{
				Ticket: ticketBytes, // Use the query bytes for the ticket
			},
			Location: nil, // No specific location for now
		}},
		TotalRecords: -1, // Or info.TotalRecords if available and accurate
		TotalBytes:   -1, // Or info.TotalBytes if available and accurate
	}

	s.metrics.IncrementCounter("flight_get_info_statement_success")
	return flightInfo, nil
}

// cleanQuery removes any control characters from the query string
func cleanQuery(query string) string {
	// Remove any leading/trailing whitespace and control characters
	query = strings.TrimSpace(query)

	// Remove any control characters from the query
	var cleaned strings.Builder
	for _, r := range query {
		if !unicode.IsControl(r) {
			cleaned.WriteRune(r)
		}
	}
	return cleaned.String()
}

// DoGetStatement implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_statement")
	defer timer.Stop()

	// Extract the query from the ticket
	var anyCmd anypb.Any
	if err := proto.Unmarshal(ticket.GetStatementHandle(), &anyCmd); err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetStatement", "error", "unmarshal_any_failed")
		return nil, nil, status.Errorf(codes.InvalidArgument, "failed to unmarshal ticket as Any: %v", err)
	}

	// Check if the command is a statement query
	if !anyCmd.MessageIs(&flightpb.CommandStatementQuery{}) {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetStatement", "error", "invalid_command_type")
		return nil, nil, status.Error(codes.InvalidArgument, "invalid command type: expected CommandStatementQuery")
	}

	var cmdProto flightpb.CommandStatementQuery
	if err := anyCmd.UnmarshalTo(&cmdProto); err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetStatement", "error", "unmarshal_cmd_failed")
		return nil, nil, status.Errorf(codes.InvalidArgument, "failed to unmarshal CommandStatementQuery from Any: %v", err)
	}

	query := cleanQuery(cmdProto.GetQuery())
	if query == "" {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetStatement", "error", "empty_query")
		return nil, nil, status.Error(codes.InvalidArgument, "query cannot be empty")
	}

	s.logger.Debug().
		Str("query", query).
		Msg("DoGetStatement")

	// Execute the query and get the results
	schema, resultStream, err := s.queryHandler.ExecuteQueryAndStream(ctx, query)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetStatement", "error", "execute_failed")
		return nil, nil, err
	}

	// For COUNT queries, ensure we have a single column
	isCountQuery := strings.Contains(strings.ToUpper(query), "COUNT(*)")
	if isCountQuery {
		if schema.NumFields() != 1 {
			s.metrics.IncrementCounter("flight_errors", "method", "DoGetStatement", "error", "invalid_count_schema")
			return nil, nil, status.Error(codes.Internal, "COUNT query must return exactly one column")
		}
	}

	// Create a new channel for the stream
	stream := make(chan flight.StreamChunk, 16)

	// Start streaming results in background
	go func() {
		defer close(stream)

		firstChunkLogged := false
		for chunk := range resultStream {
			if chunk.Err != nil {
				s.logger.Error().Err(chunk.Err).Msg("Error reading query results")
				continue
			}

			record := chunk.Data
			record.Retain() // Retain before sending to ensure it's not released prematurely

			// Log details for COUNT(*) query's first data chunk
			if isCountQuery && !firstChunkLogged && record.NumRows() > 0 {
				s.logger.Info().
					Str("query", query).
					Str("record_schema", record.Schema().String()).
					Int64("num_rows_in_chunk", record.NumRows()).
					Msg("Logging first data chunk for COUNT query")

				// Log the value of the first column of the first row for COUNT query
				if record.NumCols() > 0 {
					col := record.Column(0)
					colField := record.Schema().Field(0) // Get field for name/type
					s.logger.Info().Str("column_name", colField.Name).Str("column_type", colField.Type.String()).Int("column_len", col.Len()).Msg("COUNT query first column details")

					if col.Len() > 0 {
						switch c := col.(type) {
						case *array.Int64:
							s.logger.Info().Int64("count_value_int64", c.Value(0)).Msg("COUNT query first row value (Int64)")
						case *array.Uint64:
							s.logger.Info().Uint64("count_value_uint64", c.Value(0)).Msg("COUNT query first row value (Uint64)")
						default:
							s.logger.Info().Str("count_array_string", col.String()).Str("count_array_type_reflected", fmt.Sprintf("%T", col)).Msg("COUNT query first column value (generic)")
						}
					} else {
						s.logger.Warn().Msg("COUNT query first column has no rows to log value from")
					}
				}
				firstChunkLogged = true
			}

			// Send the record to the stream
			stream <- flight.StreamChunk{Data: record}
			s.logger.Debug().
				Int64("rows", record.NumRows()).
				Msg("Sent record chunk to stream")
		}
	}()

	s.metrics.IncrementCounter("flight_do_get_statement_success")
	return schema, stream, nil
}

// DoPutCommandStatementUpdate implements the FlightSQL interface.
func (s *FlightSQLServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	timer := s.metrics.StartTimer("flight_do_put_command_statement_update")
	defer timer.Stop()

	query := cleanQuery(cmd.GetQuery())
	if query == "" {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutCommandStatementUpdate", "error", "empty_query")
		return 0, status.Error(codes.InvalidArgument, "query cannot be empty")
	}

	s.logger.Debug().
		Str("query", query).
		Msg("DoPutCommandStatementUpdate")

	// Execute the update command
	rowsAffected, err := s.queryHandler.ExecuteUpdate(ctx, query, string(cmd.GetTransactionId()))
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutCommandStatementUpdate", "error", "execute_failed")
		return 0, err
	}

	s.logger.Info().
		Str("query", query).
		Int64("rows_affected", rowsAffected).
		Msg("Successfully executed update command")

	s.metrics.IncrementCounter("flight_do_put_command_statement_update_success")
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

	cmdProto := &flightpb.CommandGetCatalogs{}
	anyCmd, err := anypb.New(cmdProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to create Any for CommandGetCatalogs")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoCatalogs", "error", "anypb_new_failed")
		return nil, status.Errorf(codes.Internal, "failed to create Any message: %v", err)
	}
	cmdBytes, err := proto.Marshal(anyCmd)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal Any(CommandGetCatalogs)")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoCatalogs", "error", "marshal_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal ticket command: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: cmdBytes},
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

	cmdProto := &flightpb.CommandGetDbSchemas{
		Catalog:               catalog,
		DbSchemaFilterPattern: pattern,
	}
	anyCmd, err := anypb.New(cmdProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to create Any for CommandGetDBSchemas")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoSchemas", "error", "anypb_new_failed")
		return nil, status.Errorf(codes.Internal, "failed to create Any message: %v", err)
	}
	cmdBytes, err := proto.Marshal(anyCmd)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal Any(CommandGetDBSchemas)")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoSchemas", "error", "marshal_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal ticket command: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: cmdBytes},
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

	cmdProto := &flightpb.CommandGetTables{
		Catalog:                catalog,
		DbSchemaFilterPattern:  schemaPattern,
		TableNameFilterPattern: tablePattern,
		TableTypes:             tableTypes,
		IncludeSchema:          includeSchema,
	}
	anyCmd, err := anypb.New(cmdProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to create Any for CommandGetTables")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoTables", "error", "anypb_new_failed")
		return nil, status.Errorf(codes.Internal, "failed to create Any message: %v", err)
	}
	cmdBytes, err := proto.Marshal(anyCmd)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal Any(CommandGetTables)")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoTables", "error", "marshal_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal ticket command: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: cmdBytes},
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

	handle := string(cmd.GetPreparedStatementHandle())
	if handle == "" {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoPreparedStatement", "error", "empty_handle")
		return nil, status.Error(codes.InvalidArgument, "prepared statement handle cannot be empty")
	}

	s.logger.Debug().
		Str("handle", handle).
		Msg("GetFlightInfoPreparedStatement")

	// Get the schema for the prepared statement
	schema, err := s.preparedStatementHandler.GetSchema(ctx, handle)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoPreparedStatement", "error", "get_schema_failed")
		return nil, err
	}

	// Create a prepared statement query command
	cmdProto := &flightpb.CommandPreparedStatementQuery{
		PreparedStatementHandle: []byte(handle),
	}

	anyCmd, err := anypb.New(cmdProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to create Any for CommandPreparedStatementQuery")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoPreparedStatement", "error", "anypb_new_failed")
		return nil, status.Errorf(codes.Internal, "failed to create Any message: %v", err)
	}

	// Marshal the Any command
	cmdBytes, err := proto.Marshal(anyCmd)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal Any(CommandPreparedStatementQuery)")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoPreparedStatement", "error", "marshal_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal ticket command: %v", err))
	}

	// Create the flight info
	flightInfo := &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{
				Ticket: cmdBytes,
			},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}

	s.metrics.IncrementCounter("flight_get_info_prepared_statement_success")
	return flightInfo, nil
}

// DoGetPreparedStatement implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_prepared_statement")
	defer timer.Stop()

	handle := string(cmd.GetPreparedStatementHandle()) // This is the actual statement handle string
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
		return nil, err
	}

	// Read the parameter values from the message reader
	record, err := reader.Read()
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "read_params_failed")
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("failed to read parameters: %v", err))
	}
	defer record.Release()

	// Create a new record with the correct schema
	builder := array.NewRecordBuilder(s.allocator, paramSchema)
	defer builder.Release()

	// Process all rows in the input record
	for row := 0; row < int(record.NumRows()); row++ {
		// Copy values from the input record to the builder for each row
		for i := 0; i < paramSchema.NumFields(); i++ {
			if int64(i) >= record.NumCols() {
				s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "missing_params")
				return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("missing parameter at index %d", i))
			}

			// Handle different parameter types
			col := record.Column(i)
			if col.IsNull(row) {
				builder.Field(i).AppendNull()
				continue
			}

			switch paramSchema.Field(i).Type.ID() {
			case arrow.INT64:
				if col.DataType().ID() == arrow.INT64 {
					builder.Field(i).(*array.Int64Builder).Append(array.NewInt64Data(col.Data()).Value(row))
				} else if col.DataType().ID() == arrow.FLOAT64 {
					// Convert float64 to int64 if needed
					val := array.NewFloat64Data(col.Data()).Value(row)
					builder.Field(i).(*array.Int64Builder).Append(int64(val))
				} else {
					return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("parameter %d: expected Int64, got %T", i, col))
				}
			case arrow.FLOAT64:
				if col.DataType().ID() == arrow.FLOAT64 {
					builder.Field(i).(*array.Float64Builder).Append(array.NewFloat64Data(col.Data()).Value(row))
				} else if col.DataType().ID() == arrow.INT64 {
					// Convert int64 to float64 if needed
					val := array.NewInt64Data(col.Data()).Value(row)
					builder.Field(i).(*array.Float64Builder).Append(float64(val))
				} else {
					return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("parameter %d: expected Float64, got %T", i, col))
				}
			case arrow.STRING:
				if col.DataType().ID() == arrow.STRING {
					builder.Field(i).(*array.StringBuilder).Append(array.NewStringData(col.Data()).Value(row))
				} else {
					return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("parameter %d: expected String, got %T", i, col))
				}
			default:
				return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("unsupported parameter type: %v", paramSchema.Field(i).Type))
			}
		}
	}

	// Create the final record with all rows
	boundParams := builder.NewRecord()
	defer boundParams.Release()

	// Execute the prepared statement with all rows
	_, _, err = s.preparedStatementHandler.ExecuteQuery(ctx, handle, boundParams)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "execute_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to execute prepared statement: %v", err))
	}

	s.metrics.IncrementCounter("flight_do_put_prepared_statement_query_success")
	return nil, nil
}

// DoPutPreparedStatementUpdate implements the FlightSQL interface.
func (s *FlightSQLServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, reader flight.MessageReader) (int64, error) {
	timer := s.metrics.StartTimer("flight_do_put_prepared_statement_update")
	defer timer.Stop()

	handle := string(cmd.GetPreparedStatementHandle())
	if handle == "" {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementUpdate", "error", "empty_handle")
		return 0, status.Error(codes.InvalidArgument, "prepared statement handle cannot be empty")
	}
	s.logger.Debug().Str("handle", handle).Msg("DoPutPreparedStatementUpdate")

	// Get the parameter schema first
	paramSchema, err := s.preparedStatementHandler.GetParameterSchema(ctx, handle)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementUpdate", "error", "get_param_schema_failed")
		return 0, err
	}

	// Read the parameter values from the message reader
	record, err := reader.Read()
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementUpdate", "error", "read_params_failed")
		return 0, status.Error(codes.InvalidArgument, fmt.Sprintf("failed to read parameters: %v", err))
	}
	defer record.Release()

	// Validate parameter schema matches
	if !record.Schema().Equal(paramSchema) {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementUpdate", "error", "schema_mismatch")
		return 0, status.Error(codes.InvalidArgument, "parameter schema mismatch")
	}

	var totalRowsAffected int64

	// Iterate over each row in the record if it's a batch
	if record.NumRows() > 1 {
		s.logger.Debug().Int64("num_rows_in_batch", record.NumRows()).Msg("Processing batch update")
		for i := 0; i < int(record.NumRows()); i++ {
			// Create a new record for each row to pass to the handler
			rowRecord := record.NewSlice(int64(i), int64(i+1))

			rowsAffected, err := s.preparedStatementHandler.ExecuteUpdate(ctx, handle, rowRecord)
			rowRecord.Release() // Release immediately after use
			if err != nil {
				s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementUpdate", "error", "execute_batch_row_failed")
				return totalRowsAffected, status.Error(codes.Internal, fmt.Sprintf("failed to execute prepared statement for row %d: %v", i, err))
			}
			totalRowsAffected += rowsAffected
		}
	} else if record.NumRows() == 1 {
		// Execute the prepared statement with parameters for a single record
		rowsAffected, err := s.preparedStatementHandler.ExecuteUpdate(ctx, handle, record)
		if err != nil {
			s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementUpdate", "error", "execute_failed")
			return 0, status.Error(codes.Internal, fmt.Sprintf("failed to execute prepared statement: %v", err))
		}
		totalRowsAffected = rowsAffected
	}

	s.logger.Info().
		Str("handle", handle).
		Int64("rows_affected", totalRowsAffected).
		Msg("Successfully executed prepared update")

	s.metrics.IncrementCounter("flight_do_put_prepared_statement_update_success")
	return totalRowsAffected, nil
}

// ClosePreparedStatement implements the FlightSQL interface.
func (s *FlightSQLServer) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	timer := s.metrics.StartTimer("flight_close_prepared_statement")
	defer timer.Stop()

	handle := string(req.GetPreparedStatementHandle())
	if handle == "" {
		s.metrics.IncrementCounter("flight_errors", "method", "ClosePreparedStatement", "error", "empty_handle")
		return status.Error(codes.InvalidArgument, "prepared statement handle cannot be empty")
	}
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

	cmdProto := &flightpb.CommandGetTableTypes{}
	anyCmd, err := anypb.New(cmdProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to create Any for CommandGetTableTypes")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoTableTypes", "error", "anypb_new_failed")
		return nil, status.Errorf(codes.Internal, "failed to create Any message: %v", err)
	}
	cmdBytes, err := proto.Marshal(anyCmd)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal Any(CommandGetTableTypes)")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoTableTypes", "error", "marshal_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal ticket command: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: cmdBytes},
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

	cmdProto := &flightpb.CommandGetExportedKeys{
		Catalog:  catalog,
		DbSchema: dbSchema,
		Table:    table,
	}
	anyCmd, err := anypb.New(cmdProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to create Any for CommandGetExportedKeys")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoExportedKeys", "error", "anypb_new_failed")
		return nil, status.Errorf(codes.Internal, "failed to create Any message: %v", err)
	}
	cmdBytes, err := proto.Marshal(anyCmd)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal Any(CommandGetExportedKeys)")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoExportedKeys", "error", "marshal_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal ticket command: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: cmdBytes},
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

	dataTypePtr := cmd.GetDataType()

	schema, _, err := s.metadataHandler.GetXdbcTypeInfo(ctx, dataTypePtr)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoXdbcTypeInfo")
		return nil, err
	}

	cmdProto := &flightpb.CommandGetXdbcTypeInfo{}
	if dataTypePtr != nil {
		cmdProto.DataType = dataTypePtr
	}
	anyCmd, err := anypb.New(cmdProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to create Any for CommandGetXdbcTypeInfo")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoXdbcTypeInfo", "error", "anypb_new_failed")
		return nil, status.Errorf(codes.Internal, "failed to create Any message: %v", err)
	}
	cmdBytes, err := proto.Marshal(anyCmd)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal Any(CommandGetXdbcTypeInfo)")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoXdbcTypeInfo", "error", "marshal_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal ticket command: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: cmdBytes},
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

	cmdProto := &flightpb.CommandGetSqlInfo{
		Info: info,
	}
	anyCmd, err := anypb.New(cmdProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to create Any for CommandGetSqlInfo")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoSqlInfo", "error", "anypb_new_failed")
		return nil, status.Errorf(codes.Internal, "failed to create Any message: %v", err)
	}
	cmdBytes, err := proto.Marshal(anyCmd)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal Any(CommandGetSqlInfo)")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoSqlInfo", "error", "marshal_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal ticket command: %v", err))
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: cmdBytes},
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
