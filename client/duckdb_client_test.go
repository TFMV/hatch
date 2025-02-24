package flight_test

import (
	"testing"

	flight "github.com/TFMV/flight/client"
)

func TestDuckDBClient(t *testing.T) {
	client := flight.NewDuckDBClient(nil)

	client.Connect()
}
