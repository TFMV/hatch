# Hatch Documentation

## Overview

Hatch is a minimal Flight SQL server powered by DuckDB. It aims to make
Arrow-native networking easy to deploy without the overhead of a full
analytics stack. By speaking the Flight SQL protocol directly, Hatch can
stream Arrow data to any compatible client with very little overhead.

## Core Components

- **Flight SQL Handler** – Implements the Flight SQL protocol and exposes
  query, metadata, transaction, and prepared statement operations.
- **DuckDB Integration** – Repositories that execute SQL against DuckDB
  using a connection pool.
- **Schema Manager** – Arrow schema definitions and helper functions used
  across handlers and services.
- **Cache** – An in-memory LRU cache for Arrow record batches to avoid
  recomputing results.
- **Metrics** – Pluggable metrics collection with a Prometheus backend by
  default.
- **Connection and Object Pools** – Efficient resource pools for database
  connections and Arrow buffers/builders.

## Architecture

1. **Repositories** interact with DuckDB to run queries and gather
   metadata.
2. **Services** wrap repositories with business logic and expose a simple
   interface to the handlers.
3. **Handlers** adapt services to the Flight SQL server, translating
   protocol calls into service operations.
4. The **Flight SQL server** wires the handlers to gRPC and manages
   sessions, caching, and middleware.
5. Optional middleware (authentication, logging, metrics) can be added at
   the gRPC layer.

This layered approach keeps DuckDB logic isolated while providing clear
extension points for custom features.

## Usage

Build and run the server:

```bash
make build
./hatch serve --database my.db --address 0.0.0.0:32010
```

Clients can connect using any Flight SQL client (e.g. Python, Java, or Go)
and issue standard SQL queries. Results are streamed back as Arrow record
batches. When Prometheus metrics are enabled, a metrics endpoint is
available on port `9090` by default.

## Development Notes

- **Dependencies** – Go 1.24 with modules listed in `go.mod`. Key
  libraries include Apache Arrow, gRPC, Zerolog, Viper, and Cobra.
- **Building** – Use `make build` for an optimized binary. `make run`
  will build and run with the default configuration.
- **Testing** – `make test` runs the Go unit tests under `./pkg`.
- **Extensibility** – Implement new services or middleware and wire them
  into `cmd/server/main.go`. The modular design keeps components loosely
  coupled, making it easy to swap implementations.
