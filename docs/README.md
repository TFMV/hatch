# Porter Documentation

## Overview

Porter is a lightweight server that exposes analytical databases over the Arrow Flight SQL protocol. It implements all standard metadata and query RPCs along with prepared statements and JDBC-compatible metadata. Results are streamed as Arrow record batches and can be cached in memory. Optional metrics are exported via Prometheus and both TLS and basic authentication can be enabled through configuration.

## Architecture

1. **Flight SQL handler** – gRPC implementation of the Flight SQL service. It translates protocol calls into internal service operations.
2. **Metadata repository** – Collects catalogs, schemas and table information from the database backend.
3. **Database backends** – Flexible abstraction layer for different databases (currently DuckDB, with plans for ClickHouse and others). Each backend implements the repository interfaces.
4. **Services and middleware** – Wrap the repository with business logic and add features such as metrics, authentication or logging.
5. **Server wiring** – `cmd/server/main.go` ties everything together and starts the gRPC server.

This layered approach provides a clean abstraction over different database backends while maintaining clear extension points for additional features.

## Usage

Build and start the server:

```bash
make build
./porter serve --database example.db --address 0.0.0.0:32010
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
* **Add new backends** – Implement the repository interfaces for a new database engine and register it in `cmd/server/main.go`.
* **Middleware** – Custom authentication, logging or metrics interceptors can be added where the gRPC server is configured.

The modular design keeps components loosely coupled, making it straightforward to experiment with new features and database backends.

