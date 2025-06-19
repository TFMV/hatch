# Porter

*Zero‑copy analytics, delivered at Mach Arrow.*

[![Go Report Card](https://goreportcard.com/badge/github.com/TFMV/porter)](https://goreportcard.com/report/github.com/TFMV/porter)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build and Test](https://github.com/TFMV/porter/actions/workflows/ci.yml/badge.svg)](https://github.com/TFMV/porter/actions/workflows/ci.yml)

> **Status: Experimental**  
> Porter is under **active development** and is currently **experimental**.  
> It is **not yet ready for production use**. APIs may change, and features may be incomplete.
> 
> For a stable Flight SQL implementation, see [GizmoSQL](https://github.com/gizmodata/gizmosql).

---

## What is Porter?

Porter is a **high-performance Flight SQL server** that makes Arrow-native analytics effortless. It provides a flexible abstraction layer for different database backends, currently supporting DuckDB with plans to add more (like ClickHouse). It bridges the gap between analytical databases and modern data infrastructure by speaking the Flight SQL protocol natively.

Think of it as **analytics with wings** – all the analytical power you love, now network-accessible with zero-copy Arrow streaming.

## Key Features

### Blazing Fast
- **Zero-copy Arrow streaming** for maximum throughput
- **Connection pooling** and resource optimization
- **In-memory caching** with LRU eviction
- **Concurrent query execution** with transaction support

### Developer Friendly
- **Single binary** deployment
- **Configuration via YAML** or environment variables  
- **Docker support** with optimized images
- **Comprehensive CLI** with intuitive commands
- **Extensive middleware** support for customization

### Standards Compliant
- **Apache Arrow Flight SQL** protocol
- **JDBC/ODBC compatibility** through Flight SQL drivers
- **SQL standard compliance** via database backends
- **Cross-platform** support (Linux, macOS, Windows)

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Flight SQL    │    │     Porter       │    │    Database     │
│    Clients      │◄──►│   Flight SQL     │◄──►│    Backends     │
│                 │    │     Server       │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
        │                        │                        │
        │                        │                        │
    ┌───▼────┐              ┌────▼────┐              ┌────▼────┐
    │ Arrow  │              │ Metrics │              │ Schema  │
    │Streamng│              │Promtheus│              │ Cache   │
    └────────┘              └─────────┘              └─────────┘
```

**Core Components:**
- **Flight SQL Handler** – Protocol implementation and query routing
- **Database Backends** – Flexible abstraction for different databases (DuckDB, ClickHouse, etc.)
- **Connection Pool** – Efficient database connection management
- **Cache Layer** – Smart caching for schemas and query results
- **Metrics Collection** – Comprehensive observability and monitoring
- **Middleware Stack** – Authentication, logging, and custom extensions

## Quick Start

### Prerequisites
- Go 1.24+ (for building from source)
- DuckDB (bundled with Go bindings)

### Installation

#### Option 1: Build from Source
```bash
git clone https://github.com/TFMV/porter.git
cd porter
make build
```

#### Option 2: Using Go Install
```bash
go install github.com/TFMV/porter/cmd/server@latest
```

### Running Porter

#### Basic Usage
```bash
# Start with in-memory database
./porter serve

# Start with persistent database
./porter serve --database ./my-data.db --address 0.0.0.0:32010

# Start with configuration file
./porter serve --config ./config.yaml
```

#### Using Docker
```bash
# Build the image
make docker-build

# Run the container
make docker-run

# Or run directly
docker run -p 32010:32010 -p 9090:9090 porter:latest
```

### Configuration

Create a `config.yaml` file:

```yaml
server:
  address: "0.0.0.0:32010"
  max_connections: 100
  connection_timeout: "30s"
  query_timeout: "5m"
  max_message_size: 16777216  # 16MB

database:
  dsn: "duckdb://./data.db"
  max_open_conns: 32
  max_idle_conns: 8
  conn_max_lifetime: "1h"
  health_check_period: "30s"

logging:
  level: "info"
  format: "json"

metrics:
  enabled: true
  address: ":9090"

tls:
  enabled: false
  cert_file: ""
  key_file: ""

auth:
  enabled: false
  type: "oauth2"
```

## Connecting to Porter

### Python (using ADBC)
```python
import adbc_driver_flightsql.dbapi

# Connect to Porter
conn = adbc_driver_flightsql.dbapi.connect(
    "grpc://localhost:32010"
)

# Execute queries
cursor = conn.cursor()
cursor.execute("SELECT * FROM my_table LIMIT 10")
results = cursor.fetchall()
```

### Python (using PyArrow)
```python
from pyarrow import flight

# Connect to Porter Flight SQL server
client = flight.FlightClient("grpc://localhost:32010")

# Execute query and get results as Arrow table
info = client.get_flight_info(
    flight.FlightDescriptor.for_command(b"SELECT * FROM my_table")
)
table = client.do_get(info.endpoints[0].ticket).read_all()
```

### Java (using Arrow Flight SQL JDBC)
```java
String url = "jdbc:arrow-flight-sql://localhost:32010";
Connection conn = DriverManager.getConnection(url);

Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT * FROM my_table");

while (rs.next()) {
    System.out.println(rs.getString(1));
}
```

## Development

### Building
```bash
# Standard build
make build

# Optimized build (smaller binary)
make optimize

# Show binary size and dependencies
make size
```

### Testing
```bash
# Run all tests
make test

# Run with verbose output
go test -v ./...

# Run specific package tests
go test -v ./pkg/handlers
```

### Development Server
```bash
# Run with hot reload (requires air)
air

# Or run directly
make run
```

## Monitoring & Observability

### Metrics
Porter exposes Prometheus metrics on `:9090` by default:

- **Query Performance**: Execution time, throughput, error rates
- **Connection Pool**: Active connections, wait times, health status  
- **Cache Performance**: Hit/miss ratios, eviction rates
- **Resource Usage**: Memory allocation, GC statistics
- **Flight SQL Operations**: RPC call counts, streaming metrics

### Health Checks
```bash
# Check server health
curl http://localhost:9090/health

# Prometheus metrics
curl http://localhost:9090/metrics
```

### Logging
Structured logging with configurable levels:
```bash
# Set log level
export PORTER_LOG_LEVEL=debug

# JSON format (default)
export PORTER_LOG_FORMAT=json

# Human-readable format
export PORTER_LOG_FORMAT=console
```

## Advanced Configuration

### Environment Variables
All configuration options can be set via environment variables with the `PORTER_` prefix:

```bash
export PORTER_ADDRESS="0.0.0.0:32010"
export PORTER_DATABASE=":memory:"
export PORTER_LOG_LEVEL="debug"
export PORTER_METRICS_ENABLED="true"
export PORTER_MAX_CONNECTIONS="100"
```

### Command Line Options
```bash
./porter serve --help

# Key options:
--address string              Server listen address (default "0.0.0.0:32010")
--database string             DuckDB database path (default ":memory:")
--config string               Configuration file path
--log-level string            Log level: debug, info, warn, error (default "info")
--max-connections int         Maximum concurrent connections (default 100)
--metrics                     Enable Prometheus metrics (default true)
--metrics-address string      Metrics server address (default ":9090")
--tls                         Enable TLS encryption
--auth                        Enable authentication
```

## Contributing

We welcome contributions! Porter is designed to be extensible and community-driven.

### Areas for Contribution
- **Performance optimizations** and benchmarking
- **Additional authentication providers** (JWT, LDAP, etc.)
- **Enhanced monitoring** and alerting capabilities  
- **Client libraries** and integrations
- **Documentation** and examples
- **Testing** and quality assurance

### Development Setup
```bash
# Clone the repository
git clone https://github.com/TFMV/porter.git
cd porter

# Install dependencies
go mod download

# Run tests
make test

# Build and run
make build
./porter serve
```

## Documentation

- **[Flight SQL Protocol](docs/flightsql-manifest.md)** - Protocol specification and implementation details
- **[Architecture Guide](docs/README.md)** - Deep dive into Porter's architecture
- **[API Reference](pkg/)** - Go package documentation
- **[Configuration Reference](config/)** - Complete configuration options

## Roadmap

### Planned Features
- [ ] **Multi-database support** (PostgreSQL, ClickHouse adapters)
- [ ] **Advanced authentication** (JWT, SAML, LDAP)
- [ ] **Query result caching** with TTL and invalidation
- [ ] **Horizontal scaling** with query distribution
- [ ] **WebAssembly UDFs** for custom functions
- [ ] **Real-time streaming** with Apache Kafka integration
- [ ] **SQL proxy mode** for existing databases

### Performance Goals
- [ ] **Sub-millisecond** query planning
- [ ] **Multi-GB/s** streaming throughput  
- [ ] **10,000+ concurrent** connections
- [ ] **Microsecond-level** connection pooling

## Known Issues & Limitations

- **Experimental status**: APIs may change without notice
- **Limited authentication**: Only basic auth and OAuth2 currently supported
- **Single-node only**: No distributed query execution yet
- **Memory constraints**: Large result sets may cause memory pressure
- **Platform support**: Some features may be limited on Windows

## License

Released under the MIT License. See [LICENSE](LICENSE) for details.

---

