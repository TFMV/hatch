package main

import (
	"context"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to Porter server
	client, err := flightsql.NewClient("localhost:32010", nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()

	// Create a test table with some data
	log.Println("Creating test table...")
	_, err = client.Execute(ctx, "CREATE TABLE test_table (id INTEGER, name VARCHAR, value DOUBLE)")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	log.Println("Inserting test data...")
	_, err = client.Execute(ctx, "INSERT INTO test_table VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.7), (3, 'Charlie', 300.9)")
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	// Test basic query first
	log.Println("Testing basic query...")
	info, err := client.Execute(ctx, "SELECT COUNT(*) FROM test_table")
	if err != nil {
		log.Fatalf("Failed to execute basic query: %v", err)
	}

	for _, endpoint := range info.Endpoint {
		reader, err := client.DoGet(ctx, endpoint.Ticket)
		if err != nil {
			log.Fatalf("Failed to get results: %v", err)
		}

		for reader.Next() {
			record := reader.Record()
			log.Printf("Basic query result: %d rows, %d cols", record.NumRows(), record.NumCols())
			record.Release()
		}
		reader.Release()
	}

	// Test prepared statement
	log.Println("Testing prepared statement...")
	stmt, err := client.Prepare(ctx, "SELECT * FROM test_table WHERE id = ?")
	if err != nil {
		log.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close(ctx)

	// Create parameter record
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(allocator, schema)
	builder.Field(0).(*array.Int64Builder).Append(int64(2))
	paramRecord := builder.NewRecord()
	builder.Release()

	// Set parameters
	log.Println("Setting parameters...")
	stmt.SetParameters(paramRecord)
	paramRecord.Release()

	// Execute prepared statement
	log.Println("Executing prepared statement...")
	info, err = stmt.Execute(ctx)
	if err != nil {
		log.Fatalf("Failed to execute prepared statement: %v", err)
	}

	// Read results
	for _, endpoint := range info.Endpoint {
		reader, err := client.DoGet(ctx, endpoint.Ticket)
		if err != nil {
			log.Fatalf("Failed to get prepared statement results: %v", err)
		}

		for reader.Next() {
			record := reader.Record()
			log.Printf("Prepared statement result: %d rows, %d cols", record.NumRows(), record.NumCols())

			// Print the actual data
			if record.NumRows() > 0 {
				for i := int64(0); i < record.NumRows(); i++ {
					idCol := record.Column(0).(*array.Int64)
					nameCol := record.Column(1).(*array.String)
					valueCol := record.Column(2).(*array.Float64)

					log.Printf("Row %d: id=%d, name=%s, value=%f",
						i, idCol.Value(int(i)), nameCol.Value(int(i)), valueCol.Value(int(i)))
				}
			}
			record.Release()
		}
		reader.Release()
	}

	log.Println("Prepared statement test completed successfully!")
}
