// Package server wires the Flight SQL handlers to gRPC.
package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/TFMV/hatch/cmd/server/config"
	"github.com/TFMV/hatch/cmd/server/middleware"
	"github.com/TFMV/hatch/pkg/cache"
	"github.com/TFMV/hatch/pkg/handlers"
	"github.com/TFMV/hatch/pkg/infrastructure/pool"
	"github.com/TFMV/hatch/pkg/services"
)

//───────────────────────────────────
// Types and Interfaces
//───────────────────────────────────

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

// CrossReference represents a request for foreign key relationships
// between a primary key table and a foreign key table. This mirrors the
// fields provided by the Flight SQL spec for the cross reference command.
type CrossReference struct {
        PkCatalog *string
        PkDbSchema *string
        PkTable   string
        FkCatalog *string
        FkDbSchema *string
        FkTable   string
}

//───────────────────────────────────
// FlightSQLServer
//───────────────────────────────────

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

// New creates a new Flight SQL server (placeholder - use NewFlightSQLServer for now).
func New(cfg *config.Config, logger zerolog.Logger, metrics MetricsCollector) (*FlightSQLServer, error) {
	return nil, fmt.Errorf("use NewFlightSQLServer constructor instead")
}

// Register registers the Flight SQL server with a gRPC server.
func (s *FlightSQLServer) Register(grpcServer *grpc.Server) {
	// Create a Flight SQL server that properly handles SQL commands
	flightServer := flightsql.NewFlightServer(s)
	flight.RegisterFlightServiceServer(grpcServer, flightServer)
}

// GetMiddleware returns gRPC middleware for the server.
func (s *FlightSQLServer) GetMiddleware() []grpc.ServerOption {
	var opts []grpc.ServerOption
	// TODO: Add middleware implementation
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

//───────────────────────────────────
// Query helpers (deprecated - keeping for compatibility)
//───────────────────────────────────

// NewFlightSQLServer wires up all dependencies (deprecated - use New instead).
func NewFlightSQLServer(
	qh handlers.QueryHandler,
	mh handlers.MetadataHandler,
	th handlers.TransactionHandler,
	ph handlers.PreparedStatementHandler,
	p pool.ConnectionPool,
	conv interface{}, // converter.TypeConverter - keeping for compatibility
	alloc arrowmemory.Allocator,
	c cache.Cache,
	kg cache.CacheKeyGenerator,
	m interface{}, // metrics.Collector - keeping for compatibility
	lg zerolog.Logger,
) *FlightSQLServer {
	return &FlightSQLServer{
		queryHandler:             qh,
		metadataHandler:          mh,
		transactionHandler:       th,
		preparedStatementHandler: ph,
		pool:                     p,
		allocator:                alloc,
		memoryCache:              c,
		cacheKeyGen:              kg,
		logger:                   lg.With().Str("component", "server").Logger(),
		sessions:                 make(map[string]*Session),
	}
}

// infoFromSchema creates a FlightInfo from a query and schema.
func (s *FlightSQLServer) infoFromSchema(query string, schema *arrow.Schema) *flight.FlightInfo {
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

// infoStatic creates a FlightInfo for static metadata endpoints.
func (s *FlightSQLServer) infoStatic(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
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

//───────────────────────────────────
// Query helpers
//───────────────────────────────────

// GetFlightInfoStatement handles "planning" a query.
func (s *FlightSQLServer) GetFlightInfoStatement(
	ctx context.Context,
	cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	key := s.cacheKeyGen.GenerateKey(cmd.GetQuery(), nil)

	if rec, _ := s.memoryCache.Get(ctx, key); rec != nil {
		return s.infoFromSchema(cmd.GetQuery(), rec.Schema()), nil
	}

	return s.queryHandler.GetFlightInfo(ctx, cmd.GetQuery())
}

// DoGetStatement streams the query results, with a fast path to the cache.
func (s *FlightSQLServer) DoGetStatement(
	ctx context.Context,
	ticket flightsql.StatementQueryTicket,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	key := s.cacheKeyGen.GenerateKey(string(ticket.GetStatementHandle()), nil)

	// ── cache hit ───────────────────────────────────────────────
	if rec, _ := s.memoryCache.Get(ctx, key); rec != nil {
		ch := make(chan flight.StreamChunk, 1)
		ch <- flight.StreamChunk{Data: rec}
		close(ch)
		return rec.Schema(), ch, nil
	}

	// ── cache miss: ask handler ─────────────────────────────────
	schema, upstream, err := s.queryHandler.ExecuteStatement(ctx, string(ticket.GetStatementHandle()), "")
	if err != nil {
		return nil, nil, err
	}

	down := make(chan flight.StreamChunk, 16)

	go func() {
		defer close(down)
		first := true
		for c := range upstream {
			if first && c.Data != nil {
				_ = s.memoryCache.Put(ctx, key, c.Data)
				first = false
			}
			down <- c
		}
	}()

	return schema, down, nil
}

//───────────────────────────────────
// Update
//───────────────────────────────────

func (s *FlightSQLServer) DoPutCommandStatementUpdate(
	ctx context.Context,
	req flightsql.StatementUpdate,
) (int64, error) {
	affected, err := s.queryHandler.ExecuteUpdate(ctx, req.GetQuery(), string(req.GetTransactionId()))
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "execute update: %v", err)
	}
	return affected, nil
}

//───────────────────────────────────
// Metadata helpers
//───────────────────────────────────

func (s *FlightSQLServer) infoFromHandler(
	ctx context.Context,
	desc *flight.FlightDescriptor,
	get func() (*arrow.Schema, <-chan flight.StreamChunk, error),
) (*flight.FlightInfo, error) {
	schema, _, err := get()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return s.infoStatic(desc, schema), nil
}

//───────────────────────────────────
// Metadata endpoints
//───────────────────────────────────

func (s *FlightSQLServer) GetFlightInfoCatalogs(
	ctx context.Context,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, _, err := s.metadataHandler.GetCatalogs(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "catalogs: %v", err)
	}
	return s.infoStatic(desc, schema), nil
}

func (s *FlightSQLServer) DoGetCatalogs(
	ctx context.Context,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetCatalogs(ctx)
}

func (s *FlightSQLServer) GetFlightInfoSchemas(
	ctx context.Context,
	cmd flightsql.GetDBSchemas,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.infoFromHandler(ctx, desc, func() (*arrow.Schema, <-chan flight.StreamChunk, error) {
		return s.metadataHandler.GetSchemas(ctx, cmd.GetCatalog(), cmd.GetDBSchemaFilterPattern())
	})
}

func (s *FlightSQLServer) DoGetDBSchemas(
	ctx context.Context,
	cmd flightsql.GetDBSchemas,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetSchemas(ctx, cmd.GetCatalog(), cmd.GetDBSchemaFilterPattern())
}

func (s *FlightSQLServer) GetFlightInfoTables(
	ctx context.Context,
	cmd flightsql.GetTables,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.infoFromHandler(ctx, desc, func() (*arrow.Schema, <-chan flight.StreamChunk, error) {
		return s.metadataHandler.GetTables(
			ctx,
			cmd.GetCatalog(),
			cmd.GetDBSchemaFilterPattern(),
			cmd.GetTableNameFilterPattern(),
			cmd.GetTableTypes(),
			cmd.GetIncludeSchema(),
		)
	})
}

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

func (s *FlightSQLServer) GetFlightInfoTableTypes(
	ctx context.Context,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.infoFromHandler(ctx, desc, func() (*arrow.Schema, <-chan flight.StreamChunk, error) {
		return s.metadataHandler.GetTableTypes(ctx)
	})
}

func (s *FlightSQLServer) DoGetTableTypes(
	ctx context.Context,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetTableTypes(ctx)
}

func (s *FlightSQLServer) GetFlightInfoPrimaryKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.infoFromHandler(ctx, desc, func() (*arrow.Schema, <-chan flight.StreamChunk, error) {
		return s.metadataHandler.GetPrimaryKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
	})
}

func (s *FlightSQLServer) DoGetPrimaryKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetPrimaryKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
}

func (s *FlightSQLServer) GetFlightInfoImportedKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.infoFromHandler(ctx, desc, func() (*arrow.Schema, <-chan flight.StreamChunk, error) {
		return s.metadataHandler.GetImportedKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
	})
}

func (s *FlightSQLServer) DoGetImportedKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetImportedKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
}

func (s *FlightSQLServer) GetFlightInfoExportedKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.infoFromHandler(ctx, desc, func() (*arrow.Schema, <-chan flight.StreamChunk, error) {
		return s.metadataHandler.GetExportedKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
	})
}

func (s *FlightSQLServer) DoGetExportedKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetExportedKeys(ctx, cmd.Catalog, cmd.DBSchema, cmd.Table)
}

func (s *FlightSQLServer) GetFlightInfoCrossReference(
        ctx context.Context,
        cmd CrossReference,
        desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
        return nil, status.Error(codes.Unimplemented, "cross reference not supported")
}

func (s *FlightSQLServer) DoGetCrossReference(
        ctx context.Context,
        cmd CrossReference,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
        return nil, nil, status.Error(codes.Unimplemented, "cross reference not supported")
}

func (s *FlightSQLServer) GetFlightInfoXdbcTypeInfo(
	ctx context.Context,
	cmd flightsql.GetXdbcTypeInfo,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.infoFromHandler(ctx, desc, func() (*arrow.Schema, <-chan flight.StreamChunk, error) {
		return s.metadataHandler.GetXdbcTypeInfo(ctx, cmd.GetDataType())
	})
}

func (s *FlightSQLServer) DoGetXdbcTypeInfo(
	ctx context.Context,
	cmd flightsql.GetXdbcTypeInfo,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetXdbcTypeInfo(ctx, cmd.GetDataType())
}

func (s *FlightSQLServer) GetFlightInfoSqlInfo(
	ctx context.Context,
	cmd flightsql.GetSqlInfo,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.infoFromHandler(ctx, desc, func() (*arrow.Schema, <-chan flight.StreamChunk, error) {
		return s.metadataHandler.GetSqlInfo(ctx, cmd.GetInfo())
	})
}

func (s *FlightSQLServer) DoGetSqlInfo(
	ctx context.Context,
	cmd flightsql.GetSqlInfo,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.metadataHandler.GetSqlInfo(ctx, cmd.GetInfo())
}

//───────────────────────────────────
// Transactions
//───────────────────────────────────

func (s *FlightSQLServer) BeginTransaction(
	ctx context.Context,
	req flightsql.ActionBeginTransactionRequest,
) ([]byte, error) {
	id, err := s.transactionHandler.Begin(ctx, false)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
	}
	return []byte(id), nil
}

func (s *FlightSQLServer) EndTransaction(
	ctx context.Context,
	req flightsql.ActionEndTransactionRequest,
) error {
	id := string(req.GetTransactionId())
	var err error
	switch req.GetAction() {
	case flightsql.EndTransactionCommit:
		err = s.transactionHandler.Commit(ctx, id)
	default:
		err = s.transactionHandler.Rollback(ctx, id)
	}
	if err != nil {
		return status.Errorf(codes.Internal, "end tx: %v", err)
	}
	return nil
}

//───────────────────────────────────
// Prepared statements
//───────────────────────────────────

func (s *FlightSQLServer) CreatePreparedStatement(
	ctx context.Context,
	req flightsql.ActionCreatePreparedStatementRequest,
) (flightsql.ActionCreatePreparedStatementResult, error) {
	h, sch, err := s.preparedStatementHandler.Create(ctx, req.GetQuery(), string(req.GetTransactionId()))
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, status.Errorf(codes.Internal, "create ps: %v", err)
	}
	return flightsql.ActionCreatePreparedStatementResult{
		Handle:        []byte(h),
		DatasetSchema: sch,
	}, nil
}

func (s *FlightSQLServer) ClosePreparedStatement(
	ctx context.Context,
	req flightsql.ActionClosePreparedStatementRequest,
) error {
	if err := s.preparedStatementHandler.Close(ctx, string(req.GetPreparedStatementHandle())); err != nil {
		return status.Errorf(codes.Internal, "close ps: %v", err)
	}
	return nil
}

func (s *FlightSQLServer) GetFlightInfoPreparedStatement(
	ctx context.Context,
	cmd flightsql.PreparedStatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema, err := s.preparedStatementHandler.GetSchema(ctx, string(cmd.GetPreparedStatementHandle()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get ps schema: %v", err)
	}
	return s.infoStatic(desc, schema), nil
}

func (s *FlightSQLServer) DoGetPreparedStatement(
	ctx context.Context,
	cmd flightsql.PreparedStatementQuery,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.preparedStatementHandler.ExecuteQuery(ctx, string(cmd.GetPreparedStatementHandle()), nil)
}

func (s *FlightSQLServer) DoPutPreparedStatementQuery(
	ctx context.Context,
	cmd flightsql.PreparedStatementQuery,
	reader flight.MessageReader,
	writer flight.MetadataWriter,
) ([]byte, error) {
	record, err := reader.Read()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read params: %v", err)
	}
	defer record.Release()

	if err := s.preparedStatementHandler.SetParameters(ctx, string(cmd.GetPreparedStatementHandle()), record); err != nil {
		return nil, status.Errorf(codes.Internal, "set ps params: %v", err)
	}
	return []byte(cmd.GetPreparedStatementHandle()), nil
}

func (s *FlightSQLServer) DoPutPreparedStatementUpdate(
	ctx context.Context,
	cmd flightsql.PreparedStatementUpdate,
	reader flight.MessageReader,
) (int64, error) {
	record, err := reader.Read()
	if err != nil {
		return 0, status.Errorf(codes.Internal, "read params: %v", err)
	}
	defer record.Release()

	affected, err := s.preparedStatementHandler.ExecuteUpdate(ctx, string(cmd.GetPreparedStatementHandle()), record)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "execute ps update: %v", err)
	}
	return affected, nil
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
