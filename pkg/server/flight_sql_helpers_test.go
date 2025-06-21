package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestInfoHelpers(t *testing.T) {
	alloc := memory.NewGoAllocator()
	srv := &FlightSQLServer{allocator: alloc}
	schema := arrow.NewSchema([]arrow.Field{{Name: "c", Type: arrow.PrimitiveTypes.Int64}}, nil)
	info := srv.infoFromSchema("SELECT 1", schema)
	require.Equal(t, flight.DescriptorCMD, info.FlightDescriptor.Type)
	require.Equal(t, []byte("SELECT 1"), info.FlightDescriptor.Cmd)
	require.NotNil(t, info.Endpoint[0].Ticket)

	desc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"meta"}}
	info2 := srv.infoStatic(desc, schema)
	require.Equal(t, desc, info2.FlightDescriptor)
	require.NotNil(t, info2.Endpoint[0].Ticket)
}

func TestInfoFromHandlerErrors(t *testing.T) {
	srv := &FlightSQLServer{}
	_, err := srv.infoFromHandler(context.Background(), nil, func() (*arrow.Schema, <-chan flight.StreamChunk, error) {
		return nil, nil, nil
	})
	require.Error(t, err)

	called := false
	desc := &flight.FlightDescriptor{Type: flight.DescriptorCMD}
	_, err = srv.infoFromHandler(context.Background(), desc, func() (*arrow.Schema, <-chan flight.StreamChunk, error) {
		called = true
		return nil, nil, assertAnError
	})
	require.True(t, called)
	require.Error(t, err)
}

var assertAnError = fmt.Errorf("upstream error")
