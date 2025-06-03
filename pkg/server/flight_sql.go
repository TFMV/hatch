// Package server wires the Flight SQL handlers to gRPC.
package server

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/TFMV/flight/pkg/cache"
	"github.com/TFMV/flight/pkg/handlers"
	"github.com/TFMV/flight/pkg/infrastructure/converter"
	"github.com/TFMV/flight/pkg/infrastructure/metrics"
	"github.com/TFMV/flight/pkg/infrastructure/pool"
)

//───────────────────────────────────
// FlightSQLServer
//───────────────────────────────────

// FlightSQLServer implements the Flight SQL protocol.
type FlightSQLServer struct {
	flight.BaseFlightServer

	queryHandler             handlers.QueryHandler
	metadataHandler          handlers.MetadataHandler
	transactionHandler       handlers.TransactionHandler
	preparedStatementHandler handlers.PreparedStatementHandler

	pool      pool.ConnectionPool
	converter converter.TypeConverter
	alloc     memory.Allocator
	cache     cache.Cache
	keyGen    cache.CacheKeyGenerator
	metrics   metrics.Collector
	log       zerolog.Logger
}

// NewFlightSQLServer wires up all dependencies.
func NewFlightSQLServer(
	qh handlers.QueryHandler,
	mh handlers.MetadataHandler,
	th handlers.TransactionHandler,
	ph handlers.PreparedStatementHandler,
	p pool.ConnectionPool,
	conv converter.TypeConverter,
	alloc memory.Allocator,
	c cache.Cache,
	kg cache.CacheKeyGenerator,
	m metrics.Collector,
	lg zerolog.Logger,
) *FlightSQLServer {
	return &FlightSQLServer{
		queryHandler:             qh,
		metadataHandler:          mh,
		transactionHandler:       th,
		preparedStatementHandler: ph,

		pool:      p,
		converter: conv,
		alloc:     alloc,
		cache:     c,
		keyGen:    kg,
		metrics:   m,
		log:       lg.With().Str("component", "server").Logger(),
	}
}

//───────────────────────────────────
// Query helpers
//───────────────────────────────────

// infoFromSchema creates a FlightInfo from a query and schema.
func (s *FlightSQLServer) infoFromSchema(query string, schema *arrow.Schema) *flight.FlightInfo {
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(query),
	}
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.alloc),
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
		Schema:           flight.SerializeSchema(schema, s.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}
}

// GetFlightInfoStatement handles "planning" a query.
func (s *FlightSQLServer) GetFlightInfoStatement(
	ctx context.Context,
	cmd flightsql.StatementQuery,
) (*flight.FlightInfo, error) {
	key := s.keyGen.GenerateKey(cmd.GetQuery(), nil)

	if rec, _ := s.cache.Get(ctx, key); rec != nil {
		return s.infoFromSchema(cmd.GetQuery(), rec.Schema()), nil
	}

	return s.queryHandler.GetFlightInfo(ctx, cmd.GetQuery())
}

// DoGetStatement streams the query results, with a fast path to the cache.
func (s *FlightSQLServer) DoGetStatement(
	ctx context.Context,
	cmd flightsql.StatementQuery,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	key := s.keyGen.GenerateKey(cmd.GetQuery(), nil)

	// ── cache hit ───────────────────────────────────────────────
	if rec, _ := s.cache.Get(ctx, key); rec != nil {
		ch := make(chan flight.StreamChunk, 1)
		ch <- flight.StreamChunk{Data: rec}
		close(ch)
		return rec.Schema(), ch, nil
	}

	// ── cache miss: ask handler ─────────────────────────────────
	schema, upstream, err := s.queryHandler.ExecuteStatement(ctx, cmd.GetQuery(), "")
	if err != nil {
		return nil, nil, err
	}

	down := make(chan flight.StreamChunk, 16)

	go func() {
		defer close(down)
		first := true
		for c := range upstream {
			if first && c.Data != nil {
				_ = s.cache.Put(ctx, key, c.Data)
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
) error {
	record, err := reader.Read()
	if err != nil {
		return status.Errorf(codes.Internal, "read params: %v", err)
	}
	defer record.Release()

	if err := s.preparedStatementHandler.SetParameters(ctx, string(cmd.GetPreparedStatementHandle()), record); err != nil {
		return status.Errorf(codes.Internal, "set ps params: %v", err)
	}
	return nil
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
