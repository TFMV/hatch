# Hatch Documentation

## Overview

Hatch is a lightweight server that exposes DuckDB over the Arrow Flight SQL protocol. It implements all standard metadata and query RPCs along with prepared statements and JDBC-compatible metadata. Results are streamed as Arrow record batches and can be cached in memory. Optional metrics are exported via Prometheus and both TLS and basic authentication can be enabled through configuration.

## Architecture

1. **Flight SQL handler** – gRPC implementation of the Flight SQL service. It translates protocol calls into internal service operations.
2. **Metadata repository** – Collects catalogs, schemas and table information from DuckDB.
3. **DuckDB backend** – Executes SQL using a connection pool. Query results may be cached for reuse.
4. **Services and middleware** – Wrap the repository with business logic and add features such as metrics, authentication or logging.
5. **Server wiring** – `cmd/server/main.go` ties everything together and starts the gRPC server.

This layered approach isolates DuckDB while providing clear extension points for additional features or alternative backends.

## Usage

Build and start the server:

```bash
make build
./hatch serve --database example.db --address 0.0.0.0:32010
```

### Testing RPCs

The service speaks pure gRPC. Tools such as `grpcurl` can be used to call metadata endpoints:

```bash
# List available services
grpcurl -plaintext localhost:32010 list

# Fetch catalogs
grpcurl -plaintext -d '{}' localhost:32010 \
  arrow.flight.protocol.sql.FlightSqlService/GetCatalogs
```

### JDBC

Connect using the Flight SQL JDBC driver with a URL similar to:

```
jdbc:arrow-flight-sql://localhost:32010/?useEncryption=false
```

Any Flight SQL client (Python, Java or Go) can issue standard SQL queries and receive Arrow data streams.

## Extensibility

* **Add new metadata** – Implement additional repository methods under `pkg/repositories` and expose them via a service in `pkg/services`.
* **Swap query engines** – Implement the repository interfaces against another engine and register the new implementation in `cmd/server/main.go`.
* **Middleware** – Custom authentication, logging or metrics interceptors can be added where the gRPC server is configured.

The modular design keeps components loosely coupled, making it straightforward to experiment with new features.

