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

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/TFMV/flight/cmd/server/config"
	"github.com/TFMV/flight/cmd/server/server"
	"github.com/TFMV/flight/pkg/infrastructure/metrics"
)

var (
	// Version information (set by build flags)
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

var rootCmd = &cobra.Command{
	Use:   "flight-server",
	Short: "DuckDB Flight SQL Server",
	Long: `A high-performance Flight SQL server backed by DuckDB.
	
This server implements the Apache Arrow Flight SQL protocol, providing
a standard interface for SQL queries over the Arrow Flight RPC framework.`,
	RunE: runServer,
}

func init() {
	// Command flags
	rootCmd.PersistentFlags().StringP("config", "c", "", "config file path")
	rootCmd.PersistentFlags().String("address", "0.0.0.0:8815", "server listen address")
	rootCmd.PersistentFlags().String("database", ":memory:", "DuckDB database path")
	rootCmd.PersistentFlags().String("log-level", "info", "log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().Bool("tls", false, "enable TLS")
	rootCmd.PersistentFlags().String("tls-cert", "", "TLS certificate file")
	rootCmd.PersistentFlags().String("tls-key", "", "TLS key file")
	rootCmd.PersistentFlags().Bool("auth", false, "enable authentication")
	rootCmd.PersistentFlags().Bool("metrics", true, "enable Prometheus metrics")
	rootCmd.PersistentFlags().String("metrics-address", ":9090", "metrics server address")
	rootCmd.PersistentFlags().Bool("health", true, "enable health checks")
	rootCmd.PersistentFlags().Int("max-connections", 100, "maximum concurrent connections")
	rootCmd.PersistentFlags().Duration("connection-timeout", 30*time.Second, "connection timeout")
	rootCmd.PersistentFlags().Duration("query-timeout", 5*time.Minute, "default query timeout")
	rootCmd.PersistentFlags().Int64("max-message-size", 16*1024*1024, "maximum message size in bytes")
	rootCmd.PersistentFlags().Bool("reflection", true, "enable gRPC reflection")
	rootCmd.PersistentFlags().Duration("shutdown-timeout", 30*time.Second, "graceful shutdown timeout")

	// Bind flags to viper
	viper.BindPFlags(rootCmd.PersistentFlags())
	viper.SetEnvPrefix("FLIGHT")
	viper.AutomaticEnv()

	// Add version command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("DuckDB Flight SQL Server\n")
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
		Msg("Starting DuckDB Flight SQL Server")

	// Create metrics collector
	var metricsCollector metrics.Collector
	if cfg.Metrics.Enabled {
		metricsCollector = metrics.NewPrometheusCollector()
		go startMetricsServer(cfg.Metrics.Address, logger)
	} else {
		metricsCollector = metrics.NewNoOpCollector()
	}

	// Create server
	srv, err := server.New(cfg, logger, metricsCollector)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

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

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	// Stop accepting new connections
	grpcServer.GracefulStop()

	// Close server
	if err := srv.Close(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("Error during server shutdown")
	}

	logger.Info().Msg("Server shutdown complete")
	return nil
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
	case "info":
		logLevel = zerolog.InfoLevel
	case "warn":
		logLevel = zerolog.WarnLevel
	case "error":
		logLevel = zerolog.ErrorLevel
	default:
		logLevel = zerolog.InfoLevel
	}

	// Create logger
	logger := zerolog.New(os.Stdout).
		Level(logLevel).
		With().
		Timestamp().
		Str("service", "flight-sql-server").
		Logger()

	// Set global logger
	log.Logger = logger

	return logger
}

func setupGRPCServer(cfg *config.Config, srv *server.FlightSQLServer, logger zerolog.Logger) (*grpc.Server, error) {
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

func startMetricsServer(address string, logger zerolog.Logger) {
	server := metrics.NewMetricsServer(address)
	logger.Info().Str("address", address).Msg("Starting metrics server")

	if err := server.Start(); err != nil {
		logger.Error().Err(err).Msg("Failed to start metrics server")
	}
}
