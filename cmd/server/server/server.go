// Package server implements the Flight SQL server.
package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	cache       cache.Cache
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

	connPool, err := pool.New(poolCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Create allocator
	allocator := memory.NewGoAllocator()

	// Create cache
	cacheCfg := cache.DefaultConfig().
		WithMaxSize(cfg.Cache.MaxSize).
		WithTTL(cfg.Cache.TTL).
		WithAllocator(allocator).
		WithStats(cfg.Cache.EnableStats)
	cache := cache.NewMemoryCache(cacheCfg.MaxSize, cacheCfg.Allocator)
	cacheKeyGen := &cache.DefaultCacheKeyGenerator{}

	// Create SQL info provider
	sqlInfoProvider := infrastructure.NewSQLInfoProvider(allocator)

	// Create repositories
	queryRepo := duckdb.NewQueryRepository(connPool, allocator, logger)
	metadataRepo := duckdb.NewMetadataRepository(connPool, sqlInfoProvider, logger)
	transactionRepo := duckdb.NewTransactionRepository(connPool, logger)
	preparedStatementRepo := duckdb.NewPreparedStatementRepository(connPool, allocator, logger)

	// Create service adapters
	serviceLogger := &loggerAdapter{logger}
	serviceMetrics := &serviceMetricsAdapter{metrics}

	// Create services
	metadataService := services.NewMetadataService(metadataRepo, serviceLogger, serviceMetrics)
	transactionService := services.NewTransactionService(
		transactionRepo,
		cfg.Transaction.CleanupInterval,
		serviceLogger,
		serviceMetrics,
	)
	preparedStatementService := services.NewPreparedStatementService(
		preparedStatementRepo,
		transactionService,
		serviceLogger,
		serviceMetrics,
	)

	// Create query service with transaction service
	queryServiceWithTxn := services.NewQueryService(queryRepo, transactionService, serviceLogger, serviceMetrics)

	// Create handler adapters
	handlerLogger := &loggerAdapter{logger}
	handlerMetrics := &handlerMetricsAdapter{metrics}

	// Create handlers
	queryHandler := handlers.NewQueryHandler(queryServiceWithTxn, allocator, handlerLogger, handlerMetrics)
	metadataHandler := handlers.NewMetadataHandler(metadataService, allocator, handlerLogger, handlerMetrics)
	transactionHandler := handlers.NewTransactionHandler(transactionService, handlerLogger, handlerMetrics)
	preparedStatementHandler := handlers.NewPreparedStatementHandler(
		preparedStatementService,
		queryServiceWithTxn,
		allocator,
		handlerLogger,
		handlerMetrics,
	)

	srv := &FlightSQLServer{
		config:                   cfg,
		pool:                     connPool,
		allocator:                allocator,
		logger:                   logger,
		metrics:                  metrics,
		cache:                    cache,
		cacheKeyGen:              cacheKeyGen,
		queryHandler:             queryHandler,
		metadataHandler:          metadataHandler,
		transactionHandler:       transactionHandler,
		preparedStatementHandler: preparedStatementHandler,
		queryService:             queryServiceWithTxn,
		metadataService:          metadataService,
		transactionService:       transactionService,
		preparedStatementService: preparedStatementService,
		sessions:                 make(map[string]*Session),
	}

	// Register SQL info
	if err := srv.registerSqlInfo(); err != nil {
		return nil, fmt.Errorf("failed to register SQL info: %w", err)
	}

	return srv, nil
}

// Register registers the Flight SQL server with a gRPC server.
func (s *FlightSQLServer) Register(grpcServer *grpc.Server) {
	srv := flightsql.NewFlightServer(&s.BaseServer)
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
	if err := s.cache.Close(); err != nil {
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

	return info, nil
}

// DoGetStatement implements the FlightSQL interface.
func (s *FlightSQLServer) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_statement")
	defer timer.Stop()

	s.logger.Debug().Msg("DoGetStatement")

	// Extract the query handle from the ticket
	handle := ticket.GetStatementHandle()

	// TODO:For now, we'll execute the query directly from the handle
	// In a production system, you might cache query results and retrieve them here
	query := string(handle)

	s.logger.Debug().Str("query", query).Msg("Executing statement from ticket")

	// Execute the query using the query handler
	schema, chunks, err := s.queryHandler.ExecuteStatement(ctx, query, "")
	if err != nil {
		s.metrics.IncrementCounter("flight_errors", "method", "DoGetStatement")
		return nil, nil, err
	}

	s.metrics.IncrementCounter("flight_queries_executed")
	return schema, chunks, nil
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

	s.metrics.IncrementCounter("flight_prepared_statements_created")

	return flightsql.ActionCreatePreparedStatementResult{
		Handle:        []byte(handle),
		DatasetSchema: schema,
		// ParameterSchema would be set if we supported parameterized queries
	}, nil
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
