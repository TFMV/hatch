package flight

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	duckdb_flight "github.com/TFMV/flight"
)

type DuckDBClient struct {
	db   *duckdb_flight.DuckDBFlightSQLServer
	conn *flightsql.Client
}

func NewDuckDBClient(db *duckdb_flight.DuckDBFlightSQLServer) *DuckDBClient {
	return &DuckDBClient{db: db}
}

func (c *DuckDBClient) Connect() error {
	client, err := flightsql.NewClient("localhost:8815", nil, []flight.ClientMiddleware{
		{
			Stream: func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return streamer(ctx, desc, cc, method, opts...)
			},
			Unary: func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				return invoker(ctx, method, req, reply, cc, opts...)
			},
		},
	}, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c.conn = client
	return nil
}

func (c *DuckDBClient) Close() error {
	return c.conn.Close()
}
