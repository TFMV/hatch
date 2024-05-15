package main

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const serverAddr = "localhost:8080"

func main() {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("failed to connect to server: %v\n", err)
		return
	}
	defer conn.Close()

	client := flightsql.NewFlightSQLClient(conn)

	ctx := context.Background()
	query := "SELECT * FROM your_table_name"

	// Execute the query
	info, err := client.Execute(ctx, query)
	if err != nil {
		fmt.Printf("failed to execute query: %v\n", err)
		return
	}

	printQueryResults(ctx, client, info)
}

func printQueryResults(ctx context.Context, client *flightsql.Client, info *flightsql.FlightInfo) {
	for _, endpoint := range info.Endpoint {
		reader, err := client.DoGet(ctx, endpoint.GetTicket())
		if err != nil {
			fmt.Printf("failed to get ticket: %v\n", err)
			return
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetAutoFormatHeaders(false)
		table.SetRowLine(false)
		table.SetBorder(false)
		table.SetAutoWrapText(true)

		for reader.Next() {
			record := reader.Record()
			headers := getHeaders(record)
			table.SetHeader(headers)

			for i := 0; i < int(record.NumRows()); i++ {
				row := getRow(record, i)
				table.Append(row)
			}
		}
		reader.Release()
		table.Render()
	}
}

func getHeaders(record flightsql.Record) []string {
	headers := make([]string, record.NumCols())
	for i := 0; i < int(record.NumCols()); i++ {
		headers[i] = record.ColumnName(i)
	}
	return headers
}

func getRow(record flightsql.Record, row int) []string {
	data := make([]string, record.NumCols())
	for i := 0; i < int(record.NumCols()); i++ {
		data[i] = renderText(record.Column(i), row)
	}
	return data
}

func renderText(column flightsql.Column, row int) string {
	if column.IsNull(row) {
		return "NULL"
	}
	switch col := column.(type) {
	case *flightsql.StringColumn:
		return col.Value(row)
	case *flightsql.Int32Column:
		return fmt.Sprintf("%d", col.Value(row))
	case *flightsql.Float64Column:
		return fmt.Sprintf("%f", col.Value(row))
	default:
		return "unknown type"
	}
}
