package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

const (
	postgresConnStr = "postgres://username:password@localhost:5432/dbname?sslmode=disable"
	port            = ":8080"
)

type FlightSQLServer struct {
	flightsql.BaseServer
	db *pgxpool.Pool
}

func NewFlightSQLServer(db *pgxpool.Pool) *FlightSQLServer {
	return &FlightSQLServer{db: db}
}

func (s *FlightSQLServer) GetSQLInfo(ctx context.Context, req *flightsql.GetSQLInfoRequest) (*flightsql.GetSQLInfoResponse, error) {
	// Implement your SQL Info handling logic here
	return nil, nil
}

func main() {
	config, err := pgxpool.ParseConfig(context.Background(), postgresConnStr)
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	db, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer db.Close()

	server := grpc.NewServer()
	flightsql.RegisterFlightSQLServer(server, NewFlightSQLServer(db))

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Printf("Arrow Flight SQL server listening on %s\n", port)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
