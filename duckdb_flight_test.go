// duckdb_flight_test.go
package flight

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/stretchr/testify/require"
)

// collectRecords drains all StreamChunks from a channel and returns the contained records.
func collectRecords(ch <-chan flight.StreamChunk) ([]arrow.Record, error) {
	var records []arrow.Record
	for chunk := range ch {
		if chunk.Data != nil {
			records = append(records, chunk.Data)
		}
		if chunk.Err != nil {
			return records, chunk.Err
		}
	}
	return records, nil
}

type getTables struct {
	catalog       *string
	schemaFilter  *string
	tableFilter   *string
	tableTypes    []string
	includeSchema bool
}

func (g getTables) GetCatalog() *string                { return g.catalog }
func (g getTables) GetDBSchemaFilterPattern() *string  { return g.schemaFilter }
func (g getTables) GetTableNameFilterPattern() *string { return g.tableFilter }
func (g getTables) GetTableTypes() []string            { return g.tableTypes }
func (g getTables) GetIncludeSchema() bool             { return g.includeSchema }

func TestDuckDBFlightSQLServer_GetObjects(t *testing.T) {
	db, err := CreateDB()
	require.NoError(t, err)
	defer db.Close()

	server, err := NewDuckDBFlightSQLServer(db)
	require.NoError(t, err)

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
	}

	cmd := getTables{
		catalog:       nil,
		schemaFilter:  nil,
		tableFilter:   nil,
		tableTypes:    nil,
		includeSchema: false,
	}

	info, err := server.GetFlightInfoTables(context.Background(), cmd, desc)
	require.NoError(t, err)
	require.NotNil(t, info)

	schema, ch, err := server.DoGetTables(context.Background(), cmd)
	require.NoError(t, err)
	require.NotNil(t, schema)

	records, err := collectRecords(ch)
	require.NoError(t, err)
	require.NotEmpty(t, records)
}
