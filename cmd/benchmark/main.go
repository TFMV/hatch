package main

import (
	"flag"
	"log"
	"os"

	"github.com/TFMV/porter/bench"
)

func main() {
	var (
		serverAddr = flag.String("server", "localhost:32010", "Flight SQL server address")
		parquetDir = flag.String("parquet-dir", "", "Directory containing parquet files (not used with absolute paths)")
	)
	flag.Parse()

	log.Printf("Running Flight SQL throughput benchmarks against %s", *serverAddr)

	if err := bench.RunFlightThroughputBenchmarks(*serverAddr, *parquetDir); err != nil {
		log.Fatalf("Benchmark failed: %v", err)
		os.Exit(1)
	}

	log.Println("All benchmarks completed successfully!")
}
