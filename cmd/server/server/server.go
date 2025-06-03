package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	flightsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	flightpb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/TFMV/hatch/cmd/server/config"
	"github.com/TFMV/hatch/cmd/server/middleware"
	"github.com/TFMV/hatch/pkg/cache"
	"github.com/TFMV/hatch/pkg/handlers"
	"github.com/TFMV/hatch/pkg/infrastructure"
	"github.com/TFMV/hatch/pkg/infrastructure/memory"
	"github.com/TFMV/hatch/pkg/infrastructure/pool"
	"github.com/TFMV/hatch/pkg/repositories/duckdb"
	"github.com/TFMV/hatch/pkg/services"
)

// FlightSQLServer implements the Flight SQL protocol.
type FlightSQLServer struct {
	flightsql.BaseServer

	// Configuration
	config *config.Config

	// Core components
	pool        pool.ConnectionPool
	allocator   arrowmemory.Allocator
	logger      zerolog.Logger
	metrics     MetricsCollector
	memoryCache cache.Cache
	cacheKeyGen cache.CacheKeyGenerator

	// Object pools
	byteBufferPool    *pool.ByteBufferPool
	recordBuilderPool *pool.RecordBuilderPool

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

	// OAuth2 components
	authMiddleware *middleware.AuthMiddleware
	httpServer     *http.Server
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

	// Use global tracked allocator
	srv.allocator = memory.GetAllocator()

	// Initialize object pools
	srv.byteBufferPool = pool.NewByteBufferPool()
	srv.recordBuilderPool = pool.NewRecordBuilderPool(srv.allocator)

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

	// Initialize OAuth2 endpoints if enabled
	if cfg.Auth.Enabled && cfg.Auth.Type == "oauth2" {
		srv.authMiddleware = middleware.NewAuthMiddleware(cfg.Auth, logger)

		// Create HTTP server for OAuth2 endpoints
		mux := http.NewServeMux()
		mux.HandleFunc("/oauth2/authorize", srv.authMiddleware.HandleOAuth2Authorize)
		mux.HandleFunc("/oauth2/token", srv.authMiddleware.HandleOAuth2Token)

		srv.httpServer = &http.Server{
			Addr:    cfg.Auth.OAuth2Auth.RedirectURL,
			Handler: mux,
		}

		// Start HTTP server in a goroutine
		go func() {
			if err := srv.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error().Err(err).Msg("OAuth2 HTTP server error")
			}
		}()

		logger.Info().
			Str("authorize_endpoint", "/oauth2/authorize").
			Str("token_endpoint", "/oauth2/token").
			Msg("OAuth2 endpoints initialized")
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

	// Stop OAuth2 HTTP server if running
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			s.logger.Error().Err(err).Msg("Error shutting down OAuth2 HTTP server")
		}
	}

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

	// Prepare CommandStatementQuery for FlightDescriptor.Cmd
	cmdQueryProto := &flightpb.CommandStatementQuery{
		Query: query,
	}
	descCmdBytes, err := proto.Marshal(cmdQueryProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal CommandStatementQuery for FlightDescriptor")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoStatement", "error", "marshal_desc_cmd_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal CommandStatementQuery for descriptor: %v", err))
	}

	// The StatementHandle will contain the raw query string bytes
	ticketQueryProto := &flightpb.TicketStatementQuery{
		StatementHandle: []byte(query),
	}
	anyTicket, err := anypb.New(ticketQueryProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to create Any for TicketStatementQuery for Endpoint Ticket")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoStatement", "error", "anypb_new_ticket_failed")
		return nil, status.Errorf(codes.Internal, "failed to create Any message for ticket query: %v", err)
	}
	epTicketBytes, err := proto.Marshal(anyTicket)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal Any(TicketStatementQuery) for Endpoint Ticket")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoStatement", "error", "marshal_ep_ticket_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal Any(TicketStatementQuery) for endpoint: %v", err))
	}
	s.logger.Debug().Hex("endpoint_ticket_bytes", epTicketBytes).Int("len", len(epTicketBytes)).Str("underlying_handle_content", query).Msg("GetFlightInfoStatement: generated endpoint ticket bytes (Any wrapping TicketStatementQuery with raw query handle)")

	// Create the flight info
	flightInfo := &flight.FlightInfo{
		Schema: info.Schema,
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  descCmdBytes,
		},
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{
				Ticket: epTicketBytes,
			},
			Location: nil,
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
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

	handleBytes := ticket.GetStatementHandle()
	s.logger.Debug().Hex("received_handle_bytes", handleBytes).Int("len", len(handleBytes)).Msg("DoGetStatement: initial handle bytes received")

	var query string
	var anyCmd anypb.Any

	// Attempt to unmarshal handleBytes as Any -> TicketStatementQuery
	if errUnmarshalAny := proto.Unmarshal(handleBytes, &anyCmd); errUnmarshalAny == nil {
		var tsqProto flightpb.TicketStatementQuery
		if errUnmarshalTSQ := anyCmd.UnmarshalTo(&tsqProto); errUnmarshalTSQ == nil {
			query = string(tsqProto.GetStatementHandle())
			s.logger.Debug().Str("query_from_tsq", query).Msg("DoGetStatement: Got query from TicketStatementQuery")
		} else {
			s.logger.Debug().Err(errUnmarshalTSQ).Msg("DoGetStatement: Unmarshal as TicketStatementQuery FAILED")
		}
	} else {
		s.logger.Debug().Err(errUnmarshalAny).Hex("handle_bytes", handleBytes).Msg("DoGetStatement: Unmarshal as Any FAILED. Assuming handle bytes are raw query string.")
		query = string(handleBytes)
	}

	query = cleanQuery(query)
	s.logger.Debug().Str("final_query_for_sql_execution", query).Msg("DoGetStatement")

	// Execute the query and get the schema and record channel
	schema, recordChan, err := s.queryHandler.ExecuteQueryAndStream(ctx, query)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetStatement", "error", "execute_failed")
		return nil, nil, status.Error(codes.Internal, fmt.Sprintf("failed to execute query: %v", err))
	}

	// Create a buffered channel for flight chunks
	chunkChan := make(chan flight.StreamChunk, runtime.NumCPU()*2)

	// Start a goroutine to process records and send chunks
	go func() {
		defer close(chunkChan)

		for chunk := range recordChan {
			if chunk.Err != nil {
				s.logger.Error().Err(chunk.Err).Msg("Error in record stream")
				continue
			}

			record := chunk.Data
			if record == nil {
				continue
			}

			// Implement zero-copy record handling
			if !s.config.SafeCopy {
				record.Retain() // Transfer ownership to the goroutine

				// Try to send with back-pressure handling
			sendLoop:
				for {
					select {
					case <-ctx.Done():
						record.Release()
						return
					case chunkChan <- flight.StreamChunk{Data: record}:
						break sendLoop
					default:
						runtime.Gosched() // Yield instead of blocking
					}
				}
				continue
			}

			// Safe copy path for unsafe clients
			builder := s.recordBuilderPool.Get(record.Schema())
			defer s.recordBuilderPool.Put(builder)

			// Copy data from each column
			for i := 0; i < int(record.NumCols()); i++ {
				col := record.Column(i)
				if col == nil {
					continue
				}

				fieldBuilder := builder.Field(i)
				if fieldBuilder == nil {
					continue
				}

				// Copy column data
				for j := 0; j < int(col.Len()); j++ {
					if col.IsNull(j) {
						fieldBuilder.AppendNull()
						continue
					}

					switch arr := col.(type) {
					case *array.Int8:
						fieldBuilder.(*array.Int8Builder).Append(arr.Value(j))
					case *array.Int16:
						fieldBuilder.(*array.Int16Builder).Append(arr.Value(j))
					case *array.Int32:
						fieldBuilder.(*array.Int32Builder).Append(arr.Value(j))
					case *array.Int64:
						fieldBuilder.(*array.Int64Builder).Append(arr.Value(j))
					case *array.Uint8:
						fieldBuilder.(*array.Uint8Builder).Append(arr.Value(j))
					case *array.Uint16:
						fieldBuilder.(*array.Uint16Builder).Append(arr.Value(j))
					case *array.Uint32:
						fieldBuilder.(*array.Uint32Builder).Append(arr.Value(j))
					case *array.Uint64:
						fieldBuilder.(*array.Uint64Builder).Append(arr.Value(j))
					case *array.Float32:
						fieldBuilder.(*array.Float32Builder).Append(arr.Value(j))
					case *array.Float64:
						fieldBuilder.(*array.Float64Builder).Append(arr.Value(j))
					case *array.String:
						fieldBuilder.(*array.StringBuilder).Append(arr.Value(j))
					case *array.Boolean:
						fieldBuilder.(*array.BooleanBuilder).Append(arr.Value(j))
					case *array.Binary:
						fieldBuilder.(*array.BinaryBuilder).Append(arr.Value(j))
					case *array.Decimal128:
						fieldBuilder.(*array.Decimal128Builder).Append(arr.Value(j))
					case *array.Timestamp:
						fieldBuilder.(*array.TimestampBuilder).Append(arr.Value(j))
					case *array.Time32:
						fieldBuilder.(*array.Time32Builder).Append(arr.Value(j))
					case *array.Time64:
						fieldBuilder.(*array.Time64Builder).Append(arr.Value(j))
					case *array.Date32:
						fieldBuilder.(*array.Date32Builder).Append(arr.Value(j))
					case *array.Date64:
						fieldBuilder.(*array.Date64Builder).Append(arr.Value(j))
					default:
						s.logger.Error().
							Str("type", fmt.Sprintf("%T", col)).
							Int("column", i).
							Msg("Unsupported column type")
						continue
					}
				}
			}

			// Create a new record from the builder
			recordCopy := builder.NewRecord()
			defer recordCopy.Release()

			// Send the record copy as a chunk with back-pressure handling
		sendCopyLoop:
			for {
				select {
				case <-ctx.Done():
					return
				case chunkChan <- flight.StreamChunk{Data: recordCopy}:
					break sendCopyLoop
				default:
					runtime.Gosched() // Yield instead of blocking
				}
			}
		}
	}()

	s.metrics.IncrementCounter("flight_do_get_statement_success")
	return schema, chunkChan, nil
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

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "server is shutting down")
	}
	s.mu.Unlock()

	// For now, assume read-write transactions
	txnID, err := s.transactionHandler.Begin(ctx, false)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "BeginTransaction")
		return nil, err
	}

	// Update session with transaction ID
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if sessionIDs := md.Get("session_id"); len(sessionIDs) > 0 {
			sessionID := sessionIDs[0]
			s.mu.Lock()
			if session, exists := s.sessions[sessionID]; exists {
				session.TransactionID = txnID
			}
			s.mu.Unlock()
		}
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

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "server is shutting down")
	}
	s.mu.Unlock()

	// Clear transaction ID from session
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if sessionIDs := md.Get("session_id"); len(sessionIDs) > 0 {
			sessionID := sessionIDs[0]
			s.mu.Lock()
			if session, exists := s.sessions[sessionID]; exists && session.TransactionID == txnID {
				session.TransactionID = ""
			}
			s.mu.Unlock()
		}
	}

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

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return flightsql.ActionCreatePreparedStatementResult{}, status.Error(codes.Unavailable, "server is shutting down")
	}
	s.mu.Unlock()

	handle, schema, err := s.preparedStatementHandler.Create(ctx, req.GetQuery(), string(req.GetTransactionId()))
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "CreatePreparedStatement")
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	// Get parameter schema separately
	paramSchema, err := s.preparedStatementHandler.GetParameterSchema(ctx, handle)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "CreatePreparedStatement")
		// Clean up the created statement since we failed
		if cleanupErr := s.preparedStatementHandler.Close(ctx, handle); cleanupErr != nil {
			s.logger.Error().Err(cleanupErr).Str("handle", handle).Msg("failed to clean up prepared statement after parameter schema error")
		}
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

	// Marshal the CommandPreparedStatementQuery directly
	cmdBytes, err := proto.Marshal(cmdProto)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal CommandPreparedStatementQuery for ticket")
		s.metrics.IncrementCounter("flight_errors", "method", "GetFlightInfoPreparedStatement", "error", "marshal_direct_cmd_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal direct command: %v", err))
	}
	s.logger.Debug().Hex("ticket_bytes_gen_direct", cmdBytes).Int("len", len(cmdBytes)).Msg("GetFlightInfoPreparedStatement: generated direct ticket bytes")

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

	handle := string(cmd.GetPreparedStatementHandle())
	s.logger.Debug().Str("handle", handle).Msg("DoGetPreparedStatement")

	s.mu.RLock()
	if s.closing {
		s.mu.RUnlock()
		return nil, nil, status.Error(codes.Unavailable, "server is shutting down")
	}
	s.mu.RUnlock()

	// Get the parameter schema
	paramSchema, err := s.preparedStatementHandler.GetParameterSchema(ctx, handle)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetPreparedStatement", "error", "get_param_schema_failed")
		return nil, nil, status.Error(codes.Internal, fmt.Sprintf("failed to get parameter schema: %v", err))
	}

	// Create an empty record for parameters with proper memory management
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

	// Execute the prepared statement with proper error handling
	schema, chunks, err := s.preparedStatementHandler.ExecuteQuery(ctx, handle, boundParams)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetPreparedStatement", "error", "execute_failed")
		return nil, nil, status.Error(codes.Internal, fmt.Sprintf("failed to execute prepared statement: %v", err))
	}

	// Create a buffered channel to prevent blocking
	resultChan := make(chan flight.StreamChunk, 1)

	// Start a goroutine to handle streaming results
	go func() {
		defer close(resultChan)
		for chunk := range chunks {
			select {
			case <-ctx.Done():
				s.logger.Debug().Msg("Context cancelled while streaming prepared statement results")
				return
			case resultChan <- chunk:
				// Successfully sent chunk
			}
		}
	}()

	s.metrics.IncrementCounter("flight_do_get_prepared_statement_success")
	return schema, resultChan, nil
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
	s.logger.Debug().Str("handle", handle).Msg("DoPutPreparedStatementQuery")

	s.mu.RLock()
	if s.closing {
		s.mu.RUnlock()
		return nil, status.Error(codes.Unavailable, "server is shutting down")
	}
	s.mu.RUnlock()

	// Get the parameter schema first
	paramSchema, err := s.preparedStatementHandler.GetParameterSchema(ctx, handle)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "get_param_schema_failed")
		return nil, err
	}

	// Read the parameter values from the message reader with proper error handling
	record, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "no_parameters")
			return nil, status.Error(codes.InvalidArgument, "no parameters provided")
		}
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "read_params_failed")
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("failed to read parameters: %v", err))
	}
	defer record.Release()

	// Validate parameter count
	if int(record.NumCols()) != paramSchema.NumFields() {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "param_count_mismatch")
		return nil, status.Errorf(codes.InvalidArgument, "parameter count mismatch: expected %d, got %d", paramSchema.NumFields(), record.NumCols())
	}

	// Execute the query with proper error handling
	rowsAffected, err := s.preparedStatementHandler.ExecuteUpdate(ctx, handle, record)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "execute_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to execute prepared statement: %v", err))
	}

	s.logger.Debug().Int64("rows_affected", rowsAffected).Msg("Prepared statement execution completed")

	// Create a ticket for retrieving results
	ticket := &flightpb.TicketStatementQuery{
		StatementHandle: []byte(handle),
	}

	ticketBytes, err := proto.Marshal(ticket)
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoPutPreparedStatementQuery", "error", "marshal_ticket_failed")
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal ticket: %v", err))
	}

	s.metrics.IncrementCounter("flight_do_put_prepared_statement_query_success")
	return ticketBytes, nil
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
