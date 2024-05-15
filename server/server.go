package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/apache/arrow/go/v17/arrow/flight/gen/flight"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

const (
	postgresConnStr = "postgres://username:password@localhost:5432/dbname?sslmode=disable"
	port            = ":8080"
)

// FlightSQLServer implements the FlightServiceServer interface.
type FlightSQLServer struct {
	flight.UnimplementedFlightServiceServer
	db *pgxpool.Pool
}

// NewFlightSQLServer initializes a new FlightSQLServer.
func NewFlightSQLServer(db *pgxpool.Pool) *FlightSQLServer {
	return &FlightSQLServer{db: db}
}

// GetFlightInfo handles the FlightInfo request.
func (s *FlightSQLServer) GetFlightInfo(ctx context.Context, req *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	// Implement your Flight Info handling logic here
	return &flight.FlightInfo{}, nil
}

// DoGet handles the DoGet request.
func (s *FlightSQLServer) DoGet(req *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	// Implement the logic to handle DoGet requests here
	return nil
}

func main() {
	config, err := pgxpool.ParseConfig(context.Background(), postgresConnStr)
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	db, err := pgxpool.New(context.Background(), config)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer db.Close()

	server := grpc.NewServer()
	flight.RegisterFlightServiceServer(server, NewFlightSQLServer(db))

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Printf("Arrow Flight SQL server listening on %s\n", port)

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, os.Kill)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down server...")
		server.GracefulStop()
	}()

	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
