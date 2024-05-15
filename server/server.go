package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	return &flightsql.GetSQLInfoResponse{}, nil
}

func (s *FlightSQLServer) DoGet(req *flight.Ticket, server flightsql.FlightService_DoGetServer) error {
	query := string(req.Ticket)
	rows, err := s.db.Query(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Implement logic to convert pgx rows to Arrow RecordBatches and send via server.Send
	// Example placeholder logic
	for rows.Next() {
		// Here you would convert the rows to an Arrow RecordBatch and send it
		// This is a simplified example and does not include the actual conversion
	}

	return nil
}

func (s *FlightSQLServer) DoAction(ctx context.Context, req *flight.ActionRequest) (*flight.ActionResponse, error) {
	// Implement your DoAction logic here
	return &flight.ActionResponse{}, nil
}

func main() {
	config, err := pgxpool.ParseConfig(postgresConnStr)
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	db, err := pgxpool.New(context.Background(), config)

	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer db.Close()

	server := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	flight.RegisterFlightServiceServer(server, NewFlightSQLServer(db))

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Printf("Arrow Flight SQL server listening on %s\n", port)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
