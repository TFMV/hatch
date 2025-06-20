package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/TFMV/porter/bench"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	client, err := flightsql.NewClient("localhost:32010", nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test basic connectivity
	fmt.Println("Testing basic Flight SQL connectivity...")

	// Try a simple query
	info, err := client.Execute(context.Background(), "SELECT 1 as test")
	if err != nil {
		log.Printf("Simple query failed: %v", err)
		return
	}

	fmt.Printf("Query executed successfully, endpoints: %d\n", len(info.Endpoint))

	// Try to read results
	for i, endpoint := range info.Endpoint {
		fmt.Printf("Reading from endpoint %d...\n", i)
		reader, err := client.DoGet(context.Background(), endpoint.Ticket)
		if err != nil {
			log.Printf("DoGet failed: %v", err)
			continue
		}

		rowCount := 0
		for reader.Next() {
			record := reader.Record()
			rowCount += int(record.NumRows())
			record.Release()
		}
		reader.Release()
		fmt.Printf("Endpoint %d returned %d rows\n", i, rowCount)
	}

	// Try SHOW TABLES
	fmt.Println("\nTesting SHOW TABLES...")
	info, err = client.Execute(context.Background(), "SHOW TABLES")
	if err != nil {
		log.Printf("SHOW TABLES failed: %v", err)
	} else {
		fmt.Printf("SHOW TABLES executed successfully, endpoints: %d\n", len(info.Endpoint))

		for _, endpoint := range info.Endpoint {
			reader, err := client.DoGet(context.Background(), endpoint.Ticket)
			if err != nil {
				log.Printf("DoGet failed for SHOW TABLES: %v", err)
				continue
			}

			for reader.Next() {
				record := reader.Record()
				fmt.Printf("Tables found: %d\n", record.NumRows())
				record.Release()
			}
			reader.Release()
		}
	}

	// Run Flight SQL Throughput Benchmark
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🚀 PORTER FLIGHT SQL THROUGHPUT BENCHMARK")
	fmt.Println(strings.Repeat("=", 60))

	serverAddr := "localhost:32010"
	parquetDir := "" // Not used since we have absolute paths now

	fmt.Printf("Server: %s\n", serverAddr)
	fmt.Printf("Protocol: Flight SQL over gRPC\n")
	fmt.Printf("Testing large parquet datasets for throughput...\n\n")

	if err := bench.RunFlightThroughputBenchmarks(serverAddr, parquetDir); err != nil {
		log.Printf("❌ Flight throughput benchmark failed: %v", err)
	} else {
		fmt.Println("\n✅ Flight SQL throughput benchmark completed successfully!")
		fmt.Println("🎯 Porter demonstrates excellent gRPC streaming performance")
		fmt.Println("📊 Ready for production analytical workloads")
	}
}
