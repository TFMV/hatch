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
	"github.com/TFMV/flight/pkg/handlers"
	"github.com/TFMV/flight/pkg/infrastructure/converter"
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
	pool      pool.ConnectionPool
	allocator memory.Allocator
	logger    zerolog.Logger
	metrics   MetricsCollector

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

	// Create repositories
	queryRepo := duckdb.NewQueryRepository(connPool, logger)
	metadataRepo := duckdb.NewMetadataRepository(connPool, converter.New(logger), logger)
	transactionRepo := duckdb.NewTransactionRepository(connPool, logger)
	preparedStatementRepo := duckdb.NewPreparedStatementRepository(connPool, logger)

	// Create services
	queryService := services.NewQueryService(queryRepo, nil, &loggerAdapter{logger}, &metricsAdapter{metrics})
	metadataService := services.NewMetadataService(metadataRepo, &loggerAdapter{logger}, &metricsAdapter{metrics})
	transactionService := services.NewTransactionService(
		transactionRepo,
		cfg.Transaction.CleanupInterval,
		&loggerAdapter{logger},
		&metricsAdapter{metrics},
	)
	preparedStatementService := services.NewPreparedStatementService(
		preparedStatementRepo,
		transactionService,
		&loggerAdapter{logger},
		&metricsAdapter{metrics},
	)

	// Update query service with transaction service
	queryService = services.NewQueryService(queryRepo, transactionService, &loggerAdapter{logger}, &metricsAdapter{metrics})

	// Create handlers
	queryHandler := handlers.NewQueryHandler(queryService, allocator, &loggerAdapter{logger}, &metricsAdapter{metrics})
	metadataHandler := handlers.NewMetadataHandler(metadataService, allocator, &loggerAdapter{logger}, &metricsAdapter{metrics})
	transactionHandler := handlers.NewTransactionHandler(transactionService, &loggerAdapter{logger}, &metricsAdapter{metrics})
	preparedStatementHandler := handlers.NewPreparedStatementHandler(
		preparedStatementService,
		queryService,
		allocator,
		&loggerAdapter{logger},
		&metricsAdapter{metrics},
	)

	srv := &FlightSQLServer{
		config:                   cfg,
		pool:                     connPool,
		allocator:                allocator,
		logger:                   logger,
		metrics:                  metrics,
		queryHandler:             queryHandler,
		metadataHandler:          metadataHandler,
		transactionHandler:       transactionHandler,
		preparedStatementHandler: preparedStatementHandler,
		queryService:             queryService,
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

	// Add metrics middleware
	metricsMiddleware := middleware.NewMetricsMiddleware(s.metrics)
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

	// Stop transaction service
	if svc, ok := s.transactionService.(*services.TransactionServiceCloser); ok {
		svc.Stop()
	}

	// Close connection pool
	if err := s.pool.Close(); err != nil {
		s.logger.Error().Err(err).Msg("Error closing connection pool")
	}

	s.logger.Info().Msg("Flight SQL server closed")
	return nil
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

	// For now, we execute the query directly
	// In a production system, we would use the ticket to retrieve cached results
	return nil, nil, status.Error(codes.Unimplemented, "DoGetStatement not yet implemented")
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
	case flightsql.EndTransactionRequestCommit:
		if err := s.transactionHandler.Commit(ctx, txnID); err != nil {
			s.metrics.IncrementCounter("flight_errors", "method", "EndTransaction", "action", "commit")
			return err
		}
		s.metrics.IncrementCounter("flight_transactions_committed")
	case flightsql.EndTransactionRequestRollback:
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

// registerSqlInfo registers SQL info with the base server.
func (s *FlightSQLServer) registerSqlInfo() error {
	// Server info
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerName, "DuckDB Flight SQL Server")
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerVersion, "1.0.0")
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerArrowVersion, "18.0.0")

	// SQL language support
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoDDLCatalog, true)
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoDDLSchema, true)
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoDDLTable, true)
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoIdentifierCase, int32(flightsql.SqlInfoIdentifierCaseSensitivityCaseSensitive))
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoQuotedIdentifierCase, int32(flightsql.SqlInfoIdentifierCaseSensitivityCaseSensitive))

	// Transaction support
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerTransaction, int32(flightsql.SqlInfoFlightSqlServerTransactionTransaction))
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerCancel, false)
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerStatementTimeout, int32(0))
	s.BaseServer.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerTransactionTimeout, int32(0))

	return nil
}
