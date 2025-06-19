// Package main provides the entry point for the DuckDB Flight SQL Server.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/TFMV/porter/cmd/server/config"
	"github.com/TFMV/porter/pkg/cache"
	"github.com/TFMV/porter/pkg/handlers"
	"github.com/TFMV/porter/pkg/infrastructure"
	"github.com/TFMV/porter/pkg/infrastructure/metrics"
	"github.com/TFMV/porter/pkg/infrastructure/pool"
	"github.com/TFMV/porter/pkg/models"
	"github.com/TFMV/porter/pkg/repositories/duckdb"
	"github.com/TFMV/porter/pkg/services"

	"github.com/TFMV/porter/cmd/server/middleware"

	_ "github.com/marcboeker/go-duckdb/v2"
)

var (
	// Version information (set by build flags)
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

var rootCmd = &cobra.Command{
	Use:   "porter",
	Short: "Porter Flight SQL Server",
	Long: `A high-performance Flight SQL server backed by DuckDB.

Porter implements a Flight SQL Server backed by a DuckDB database.`,
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the Porter Flight SQL Server",
	Long: `Start the Porter Flight SQL Server with the specified configuration.

Example:
  porter serve --config ./config.yaml
  porter serve --address 0.0.0.0:32010 --database :memory:`,
	RunE: runServer,
}

func init() {
	// Add serve command
	rootCmd.AddCommand(serveCmd)

	// Command flags
	serveCmd.Flags().StringP("config", "c", "", "config file path")
	serveCmd.Flags().String("address", "0.0.0.0:32010", "server listen address")
	serveCmd.Flags().String("database", ":memory:", "DuckDB database path")
	serveCmd.Flags().String("log-level", "info", "log level (debug, info, warn, error)")
	serveCmd.Flags().Bool("tls", false, "enable TLS")
	serveCmd.Flags().String("tls-cert", "", "TLS certificate file")
	serveCmd.Flags().String("tls-key", "", "TLS key file")
	serveCmd.Flags().Bool("auth", false, "enable authentication")
	serveCmd.Flags().Bool("metrics", true, "enable Prometheus metrics")
	serveCmd.Flags().String("metrics-address", ":9090", "metrics server address")
	serveCmd.Flags().Bool("health", true, "enable health checks")
	serveCmd.Flags().Int("max-connections", 100, "maximum concurrent connections")
	serveCmd.Flags().Duration("connection-timeout", 30*time.Second, "connection timeout")
	serveCmd.Flags().Duration("query-timeout", 5*time.Minute, "default query timeout")
	serveCmd.Flags().Int64("max-message-size", 16*1024*1024, "maximum message size in bytes")
	serveCmd.Flags().Bool("reflection", true, "enable gRPC reflection")
	serveCmd.Flags().Duration("shutdown-timeout", 30*time.Second, "graceful shutdown timeout")

	// Bind flags to viper
	if err := viper.BindPFlags(serveCmd.Flags()); err != nil {
		panic(fmt.Errorf("failed to bind flags: %w", err))
	}
	viper.SetEnvPrefix("PORTER")
	viper.AutomaticEnv()

	// Add version command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Porter Flight SQL Server\n")
			fmt.Printf("Version:    %s\n", version)
			fmt.Printf("Commit:     %s\n", commit)
			fmt.Printf("Build Date: %s\n", buildDate)
		},
	})
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// EnterpriseFlightSQLServer wraps the base FlightSQLServer with enterprise features.
type EnterpriseFlightSQLServer struct {
	flightsql.BaseServer
	logger    zerolog.Logger
	allocator memory.Allocator
	metrics   metrics.Collector

	authMW    *middleware.AuthMiddleware
	logMW     *middleware.LoggingMiddleware
	metricsMW *middleware.MetricsMiddleware
	recoverMW *middleware.RecoveryMiddleware

	// Core components
	pool        pool.ConnectionPool
	memoryCache cache.Cache
	cacheKeyGen cache.CacheKeyGenerator

	// Handlers
	queryHandler             handlers.QueryHandler
	metadataHandler          handlers.MetadataHandler
	transactionHandler       handlers.TransactionHandler
	preparedStatementHandler handlers.PreparedStatementHandler
}

func runServer(cmd *cobra.Command, args []string) error {
	// Load configuration
	cfg, err := loadConfig(cmd)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Setup logging
	logger := setupLogging(cfg.LogLevel)
	logger.Info().
		Str("version", version).
		Str("commit", commit).
		Str("build_date", buildDate).
		Msg("Starting Porter Flight SQL Server")

	// Create metrics collector
	var metricsCollector metrics.Collector
	var metricsServer *metrics.MetricsServer
	if cfg.Metrics.Enabled {
		metricsCollector = metrics.NewPrometheusCollector()
		metricsServer = metrics.NewMetricsServer(cfg.Metrics.Address)
		go func() {
			logger.Info().Str("address", cfg.Metrics.Address).Msg("Starting metrics server")
			if err := metricsServer.Start(); err != nil {
				logger.Error().Err(err).Msg("Failed to start metrics server")
			}
		}()
	} else {
		metricsCollector = metrics.NewNoOpCollector()
	}

	// Create enterprise server
	srv, err := createEnterpriseServer(cfg, logger, metricsCollector)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}
	defer srv.Close(context.Background())

	// Setup gRPC server
	grpcServer, err := setupGRPCServer(cfg, srv, logger)
	if err != nil {
		return fmt.Errorf("failed to setup gRPC server: %w", err)
	}

	// Create listener
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

	// Start server
	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info().
			Str("address", cfg.Address).
			Bool("tls", cfg.TLS.Enabled).
			Bool("auth", cfg.Auth.Enabled).
			Msg("Server listening")

		if err := grpcServer.Serve(listener); err != nil {
			serverErrCh <- fmt.Errorf("server error: %w", err)
		}
	}()

	// Wait for shutdown signal or server error
	select {
	case <-shutdownCh:
		logger.Info().Msg("Received shutdown signal")
	case err := <-serverErrCh:
		return err
	case <-ctx.Done():
		logger.Info().Msg("Context cancelled")
	}

	// Graceful shutdown
	logger.Info().Dur("timeout", cfg.ShutdownTimeout).Msg("Starting graceful shutdown")

	// Stop accepting new connections
	grpcServer.GracefulStop()

	// Close server
	if err := srv.Close(ctx); err != nil {
		logger.Error().Err(err).Msg("Error during server shutdown")
	}

	// Stop metrics server
	if metricsServer != nil {
		if err := metricsServer.Stop(); err != nil {
			logger.Error().Err(err).Msg("Error stopping metrics server")
		}
	}

	logger.Info().Msg("Server shutdown complete")
	return nil
}

func createEnterpriseServer(cfg *config.Config, logger zerolog.Logger, metricsCollector metrics.Collector) (*EnterpriseFlightSQLServer, error) {
	// Create memory allocator
	allocator := memory.NewGoAllocator()

	// Create connection pool configuration
	poolCfg := pool.Config{
		DSN:                cfg.Database,
		MaxOpenConnections: cfg.MaxConnections,
		MaxIdleConnections: cfg.MaxConnections / 2,
		ConnMaxLifetime:    cfg.ConnectionTimeout,
		ConnMaxIdleTime:    cfg.ConnectionTimeout / 2,
		HealthCheckPeriod:  time.Minute,
		ConnectionTimeout:  cfg.ConnectionTimeout,
	}

	// Create connection pool
	connPool, err := pool.New(poolCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Create SQL info provider
	sqlInfoProvider := infrastructure.NewSQLInfoProvider(allocator)

	// Create repositories
	queryRepo := duckdb.NewQueryRepository(connPool, allocator, logger)
	metadataRepo := duckdb.NewMetadataRepository(connPool, sqlInfoProvider, logger)
	transactionRepo := duckdb.NewTransactionRepository(connPool, logger)
	preparedStmtRepo := duckdb.NewPreparedStatementRepository(connPool, allocator, logger)

	// Create services
	transactionService := services.NewTransactionService(
		transactionRepo,
		cfg.QueryTimeout,
		&serviceLoggerAdapter{logger: logger.With().Str("component", "transaction_service").Logger()},
		&serviceMetricsAdapter{collector: metricsCollector},
	)

	queryService := services.NewQueryService(
		queryRepo,
		transactionService,
		&serviceLoggerAdapter{logger: logger.With().Str("component", "query_service").Logger()},
		&serviceMetricsAdapter{collector: metricsCollector},
	)

	metadataService := services.NewMetadataService(
		metadataRepo,
		&serviceLoggerAdapter{logger: logger.With().Str("component", "metadata_service").Logger()},
		&serviceMetricsAdapter{collector: metricsCollector},
	)

	preparedStmtService := services.NewPreparedStatementService(
		preparedStmtRepo,
		transactionService,
		&serviceLoggerAdapter{logger: logger.With().Str("component", "prepared_stmt_service").Logger()},
		&serviceMetricsAdapter{collector: metricsCollector},
	)

	// Create handlers
	queryHandler := handlers.NewQueryHandler(
		queryService,
		allocator,
		&handlerLoggerAdapter{logger: logger.With().Str("component", "query_handler").Logger()},
		&handlerMetricsAdapter{collector: metricsCollector},
	)

	metadataHandler := handlers.NewMetadataHandler(
		metadataService,
		allocator,
		&handlerLoggerAdapter{logger: logger.With().Str("component", "metadata_handler").Logger()},
		&handlerMetricsAdapter{collector: metricsCollector},
	)

	transactionHandler := handlers.NewTransactionHandler(
		transactionService,
		&handlerLoggerAdapter{logger: logger.With().Str("component", "transaction_handler").Logger()},
		&handlerMetricsAdapter{collector: metricsCollector},
	)

	preparedStmtHandler := handlers.NewPreparedStatementHandler(
		preparedStmtService,
		queryService,
		allocator,
		&handlerLoggerAdapter{logger: logger.With().Str("component", "prepared_stmt_handler").Logger()},
		&handlerMetricsAdapter{collector: metricsCollector},
	)

	// Create cache
	memCache := cache.NewMemoryCache(cfg.Cache.MaxSize, allocator)
	cacheKeyGen := &cache.DefaultCacheKeyGenerator{}

	// Middleware
	authMW := middleware.NewAuthMiddleware(cfg.Auth, logger.With().Str("component", "auth_middleware").Logger())
	logMW := middleware.NewLoggingMiddleware(logger.With().Str("component", "logging_middleware").Logger())
	metricsMW := middleware.NewMetricsMiddleware(&middlewareMetricsAdapter{collector: metricsCollector})
	recoverMW := middleware.NewRecoveryMiddleware(logger.With().Str("component", "recovery_middleware").Logger())

	// Create enterprise server
	server := &EnterpriseFlightSQLServer{
		BaseServer: flightsql.BaseServer{
			Alloc: allocator,
		},
		logger:                   logger,
		allocator:                allocator,
		metrics:                  metricsCollector,
		authMW:                   authMW,
		logMW:                    logMW,
		metricsMW:                metricsMW,
		recoverMW:                recoverMW,
		pool:                     connPool,
		memoryCache:              memCache,
		cacheKeyGen:              cacheKeyGen,
		queryHandler:             queryHandler,
		metadataHandler:          metadataHandler,
		transactionHandler:       transactionHandler,
		preparedStatementHandler: preparedStmtHandler,
	}

	// Register SQL info
	if err := server.registerSqlInfo(); err != nil {
		return nil, fmt.Errorf("failed to register SQL info: %w", err)
	}

	return server, nil
}

// Close gracefully shuts down the server.
func (s *EnterpriseFlightSQLServer) Close(ctx context.Context) error {
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

// Register registers the Flight SQL server with a gRPC server.
func (s *EnterpriseFlightSQLServer) Register(grpcServer *grpc.Server) {
	flight.RegisterFlightServiceServer(grpcServer, flightsql.NewFlightServer(s))
}

// GetMiddleware returns enterprise-grade gRPC middleware
func (s *EnterpriseFlightSQLServer) GetMiddleware() []grpc.ServerOption {
	unary := grpc.ChainUnaryInterceptor(
		s.recoverMW.UnaryInterceptor(),
		s.logMW.UnaryInterceptor(),
		s.metricsMW.UnaryInterceptor(),
		s.authMW.UnaryInterceptor(),
	)
	stream := grpc.ChainStreamInterceptor(
		s.recoverMW.StreamInterceptor(),
		s.logMW.StreamInterceptor(),
		s.metricsMW.StreamInterceptor(),
		s.authMW.StreamInterceptor(),
	)
	return []grpc.ServerOption{unary, stream}
}

func loadConfig(cmd *cobra.Command) (*config.Config, error) {
	// Load config file if specified
	if configFile := viper.GetString("config"); configFile != "" {
		viper.SetConfigFile(configFile)
		if err := viper.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Build configuration
	cfg := &config.Config{
		Address:           viper.GetString("address"),
		Database:          viper.GetString("database"),
		LogLevel:          viper.GetString("log-level"),
		MaxConnections:    viper.GetInt("max-connections"),
		ConnectionTimeout: viper.GetDuration("connection-timeout"),
		QueryTimeout:      viper.GetDuration("query-timeout"),
		MaxMessageSize:    viper.GetInt64("max-message-size"),
		ShutdownTimeout:   viper.GetDuration("shutdown-timeout"),
		TLS: config.TLSConfig{
			Enabled:  viper.GetBool("tls"),
			CertFile: viper.GetString("tls-cert"),
			KeyFile:  viper.GetString("tls-key"),
		},
		Auth: config.AuthConfig{
			Enabled: viper.GetBool("auth"),
		},
		Metrics: config.MetricsConfig{
			Enabled: viper.GetBool("metrics"),
			Address: viper.GetString("metrics-address"),
		},
		Health: config.HealthConfig{
			Enabled: viper.GetBool("health"),
		},
		Reflection: viper.GetBool("reflection"),
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

func setupLogging(level string) zerolog.Logger {
	// Configure zerolog
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.DurationFieldUnit = time.Millisecond

	// Set log level
	var logLevel zerolog.Level
	switch level {
	case "debug":
		logLevel = zerolog.DebugLevel
		// Enable caller info for debug level
		zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
			short := file
			for i := len(file) - 1; i > 0; i-- {
				if file[i] == '/' {
					short = file[i+1:]
					break
				}
			}
			return fmt.Sprintf("%s:%d", short, line)
		}
	case "info":
		logLevel = zerolog.InfoLevel
	case "warn":
		logLevel = zerolog.WarnLevel
	case "error":
		logLevel = zerolog.ErrorLevel
	default:
		logLevel = zerolog.InfoLevel
	}

	// Create logger with caller info for debug level
	logger := zerolog.New(os.Stdout).
		Level(logLevel).
		With().
		Timestamp().
		Str("service", "flight-sql-server")

	if logLevel == zerolog.DebugLevel {
		logger = logger.Caller()
	}

	return logger.Logger()
}

func setupGRPCServer(cfg *config.Config, srv *EnterpriseFlightSQLServer, logger zerolog.Logger) (*grpc.Server, error) {
	// Create gRPC options
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(int(cfg.MaxMessageSize)),
		grpc.MaxSendMsgSize(int(cfg.MaxMessageSize)),
		grpc.MaxConcurrentStreams(uint32(cfg.MaxConnections)),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	// Add TLS if enabled
	if cfg.TLS.Enabled {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	// Add middleware
	opts = append(opts, srv.GetMiddleware()...)

	// Create gRPC server
	grpcServer := grpc.NewServer(opts...)

	// Register Flight SQL service
	srv.Register(grpcServer)

	// Register health service
	if cfg.Health.Enabled {
		healthServer := health.NewServer()
		grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
		healthServer.SetServingStatus("flight.sql", grpc_health_v1.HealthCheckResponse_SERVING)
	}

	// Register reflection service
	if cfg.Reflection {
		reflection.Register(grpcServer)
	}

	return grpcServer, nil
}

// registerSqlInfo registers SQL info with the base server.
func (s *EnterpriseFlightSQLServer) registerSqlInfo() error {
	// Server info
	if err := s.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerName, "Enterprise Flight SQL Server"); err != nil {
		return err
	}
	if err := s.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerVersion, "2.0.0"); err != nil {
		return err
	}
	if err := s.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerArrowVersion, "18.0.0"); err != nil {
		return err
	}

	// SQL language support
	if err := s.RegisterSqlInfo(flightsql.SqlInfoDDLCatalog, true); err != nil {
		return err
	}
	if err := s.RegisterSqlInfo(flightsql.SqlInfoDDLSchema, true); err != nil {
		return err
	}
	if err := s.RegisterSqlInfo(flightsql.SqlInfoDDLTable, true); err != nil {
		return err
	}
	if err := s.RegisterSqlInfo(flightsql.SqlInfoIdentifierCase, int32(1)); err != nil {
		return err
	}
	if err := s.RegisterSqlInfo(flightsql.SqlInfoQuotedIdentifierCase, int32(1)); err != nil {
		return err
	}

	// Transaction support
	if err := s.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerTransaction, int32(1)); err != nil {
		return err
	}
	if err := s.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerCancel, true); err != nil {
		return err
	}

	return nil
}

// Handshake implements the Flight authentication handshake.
func (s *EnterpriseFlightSQLServer) Handshake(stream flight.FlightService_HandshakeServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	user, err := s.authMW.ValidateHandshakePayload(req.Payload)
	if err != nil {
		s.logger.Warn().Err(err).Msg("handshake failed")
		return status.Error(codes.Unauthenticated, "handshake failed")
	}
	token := s.authMW.CreateSessionToken(user)
	s.logger.Info().Str("user", user).Msg("handshake authenticated")
	resp := &flight.HandshakeResponse{Payload: []byte(token)}
	return stream.Send(resp)
}

// FlightSQL interface implementations
func (s *EnterpriseFlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	timer := s.metrics.StartTimer("flight_get_info_statement")
	defer timer.Stop()

	key := s.cacheKeyGen.GenerateKey(cmd.GetQuery(), nil)
	if rec, _ := s.memoryCache.Get(ctx, key); rec != nil {
		return s.infoFromSchema(cmd.GetQuery(), rec.Schema()), nil
	}

	return s.queryHandler.GetFlightInfo(ctx, cmd.GetQuery())
}

func (s *EnterpriseFlightSQLServer) BeginTransaction(ctx context.Context, req flightsql.ActionBeginTransactionRequest) ([]byte, error) {
	timer := s.metrics.StartTimer("flight_begin_transaction")
	defer timer.Stop()

	// Extract options from request
	opts := models.TransactionOptions{
		ReadOnly: false, // Default to read-write
	}

	// Begin transaction
	txnID, err := s.transactionHandler.Begin(ctx, opts.ReadOnly)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to begin transaction")
		return nil, status.Errorf(codes.Internal, "begin transaction: %v", err)
	}

	s.logger.Info().Str("transaction_id", txnID).Msg("Transaction started")
	return []byte(txnID), nil
}

func (s *EnterpriseFlightSQLServer) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	timer := s.metrics.StartTimer("flight_do_get_statement")
	defer timer.Stop()

	return s.queryHandler.ExecuteStatement(ctx, string(ticket.GetStatementHandle()), "")
}

func (s *EnterpriseFlightSQLServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	timer := s.metrics.StartTimer("flight_do_put_command_statement_update")
	defer timer.Stop()

	return s.queryHandler.ExecuteUpdate(ctx, cmd.GetQuery(), string(cmd.GetTransactionId()))
}

// Helper methods
func (s *EnterpriseFlightSQLServer) infoFromSchema(query string, schema *arrow.Schema) *flight.FlightInfo {
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(query),
	}
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}
}

// Adapter implementations for different interface requirements

// serviceLoggerAdapter adapts zerolog.Logger to services.Logger
type serviceLoggerAdapter struct {
	logger zerolog.Logger
}

func (l *serviceLoggerAdapter) Debug(msg string, keysAndValues ...interface{}) {
	event := l.logger.Debug()
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := fmt.Sprintf("%v", keysAndValues[i])
			value := keysAndValues[i+1]
			event = event.Interface(key, value)
		}
	}
	event.Msg(msg)
}

func (l *serviceLoggerAdapter) Info(msg string, keysAndValues ...interface{}) {
	event := l.logger.Info()
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := fmt.Sprintf("%v", keysAndValues[i])
			value := keysAndValues[i+1]
			event = event.Interface(key, value)
		}
	}
	event.Msg(msg)
}

func (l *serviceLoggerAdapter) Warn(msg string, keysAndValues ...interface{}) {
	event := l.logger.Warn()
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := fmt.Sprintf("%v", keysAndValues[i])
			value := keysAndValues[i+1]
			event = event.Interface(key, value)
		}
	}
	event.Msg(msg)
}

func (l *serviceLoggerAdapter) Error(msg string, keysAndValues ...interface{}) {
	event := l.logger.Error()
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := fmt.Sprintf("%v", keysAndValues[i])
			value := keysAndValues[i+1]
			event = event.Interface(key, value)
		}
	}
	event.Msg(msg)
}

// serviceMetricsAdapter adapts metrics.Collector to services.MetricsCollector
type serviceMetricsAdapter struct {
	collector metrics.Collector
}

func (m *serviceMetricsAdapter) IncrementCounter(name string, labels ...string) {
	m.collector.IncrementCounter(name, labels...)
}

func (m *serviceMetricsAdapter) RecordHistogram(name string, value float64, labels ...string) {
	m.collector.RecordHistogram(name, value, labels...)
}

func (m *serviceMetricsAdapter) RecordGauge(name string, value float64, labels ...string) {
	m.collector.RecordGauge(name, value, labels...)
}

func (m *serviceMetricsAdapter) StartTimer(name string) services.Timer {
	return &serviceTimerAdapter{timer: m.collector.StartTimer(name)}
}

// serviceTimerAdapter adapts metrics.Timer to services.Timer
type serviceTimerAdapter struct {
	timer metrics.Timer
}

func (t *serviceTimerAdapter) Stop() time.Duration {
	seconds := t.timer.Stop()
	return time.Duration(seconds * float64(time.Second))
}

// handlerLoggerAdapter adapts zerolog.Logger to handlers.Logger
type handlerLoggerAdapter struct {
	logger zerolog.Logger
}

func (l *handlerLoggerAdapter) Debug(msg string, fields ...interface{}) {
	event := l.logger.Debug()
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			key := fmt.Sprintf("%v", fields[i])
			value := fields[i+1]
			event = event.Interface(key, value)
		}
	}
	event.Msg(msg)
}

func (l *handlerLoggerAdapter) Info(msg string, fields ...interface{}) {
	event := l.logger.Info()
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			key := fmt.Sprintf("%v", fields[i])
			value := fields[i+1]
			event = event.Interface(key, value)
		}
	}
	event.Msg(msg)
}

func (l *handlerLoggerAdapter) Warn(msg string, fields ...interface{}) {
	event := l.logger.Warn()
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			key := fmt.Sprintf("%v", fields[i])
			value := fields[i+1]
			event = event.Interface(key, value)
		}
	}
	event.Msg(msg)
}

func (l *handlerLoggerAdapter) Error(msg string, fields ...interface{}) {
	event := l.logger.Error()
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			key := fmt.Sprintf("%v", fields[i])
			value := fields[i+1]
			event = event.Interface(key, value)
		}
	}
	event.Msg(msg)
}

// handlerMetricsAdapter adapts metrics.Collector to handlers.MetricsCollector
type handlerMetricsAdapter struct {
	collector metrics.Collector
}

func (m *handlerMetricsAdapter) IncrementCounter(name string, labels ...string) {
	m.collector.IncrementCounter(name, labels...)
}

func (m *handlerMetricsAdapter) RecordHistogram(name string, value float64, labels ...string) {
	m.collector.RecordHistogram(name, value, labels...)
}

func (m *handlerMetricsAdapter) RecordGauge(name string, value float64, labels ...string) {
	m.collector.RecordGauge(name, value, labels...)
}

func (m *handlerMetricsAdapter) StartTimer(name string) handlers.Timer {
	return &handlerTimerAdapter{timer: m.collector.StartTimer(name)}
}

// handlerTimerAdapter adapts metrics.Timer to handlers.Timer
type handlerTimerAdapter struct {
	timer metrics.Timer
}

func (t *handlerTimerAdapter) Stop() {
	t.timer.Stop()
}

// middlewareMetricsAdapter adapts metrics.Collector to middleware.MetricsCollector
type middlewareMetricsAdapter struct {
	collector metrics.Collector
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

// middlewareTimerAdapter adapts metrics.Timer to middleware.Timer
type middlewareTimerAdapter struct {
	timer metrics.Timer
}

func (t *middlewareTimerAdapter) Stop() float64 {
	return t.timer.Stop()
}
