# 🦆 Hatch

*Zero‑copy analytics, delivered at Mach Arrow.*

[![Go Report Card](https://goreportcard.com/badge/github.com/TFMV/flight)](https://goreportcard.com/report/github.com/TFMV/flight)
[![Build](https://github.com/TFMV/flight/actions/workflows/ci.yml/badge.svg)](https://github.com/TFMV/flight/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **Hatch: DuckDB ↔︎ Arrow Flight.** One binary. No JVM. No friction. Query local Parquet or remote object storage, stream Arrow IPC back in real time, and keep your CPU caches warm while you do it.

---

## ✨ Highlights

| Capability               | Details                                                                |
| ------------------------ | ---------------------------------------------------------------------- |
| **Flight SQL 1.0**       | Full‑spec read/write, prepared statements, transactions                |
| **Turbo Streaming**      | Arrow IPC rows at > **20 M rows/s** on commodity hardware              |
| **Pluggable Middleware** | Auth (JWT/OAuth2), metrics (Prometheus), tracing (OpenTelemetry)       |
| **Modern Go 22**         | Context‑first APIs, generics‑powered type safety, dependency injection |
| **Hot‑reload Config**    | YAML or ENV, zero‑downtime reload via `SIGHUP`                         |
| **Batteries Included**   | Docker image, Helm chart, golden‑path integration tests                |

---

## 🚀 Quick Start

### From Source

```bash
go install github.com/TFMV/flight/cmd/flight@latest
flight serve --config ./config.yaml
```

### Sample Query

```go
client, _ := flightsql.NewClient("localhost:32010")
ctx := context.Background()

// Standard SQL
rdr, _ := client.DoGet(ctx, "SELECT 42 AS answer")
for rdr.Next() { fmt.Println(rdr.Record()) }
```

---

## 🛠️ Configuration

Create a file `config.yaml` (all fields optional):

```yaml
server:
  address: 0.0.0.0:32010
  max_connections: 100
  tls:
    cert_file: certs/server.pem
    key_file:  certs/server-key.pem

database:
  dsn: duckdb://:memory:
  max_open_conns: 32
  health_check_period: 30s

logging:
  level: info   # debug|info|warn|error
  format: json  # json|console

metrics:
  enabled: true
  endpoint: :9090

tracing:
  enabled: false
```

Then:

```bash
flight serve --config ./config.yaml
```

---

## 🧬 Architecture (bird’s‑eye)

```mermaid
flowchart LR
    subgraph Client
        A[Flight SQL Client]
    end
    subgraph Server
        B[FlightServer]\n(gRPC)
        B --> C[Middleware⟶Auth/Stats/Trace]
        C --> D[FlightSQL Handler]
        D --> E[Query Svc]
        D --> F[Metadata Svc]
        D --> G[Txn Svc]
        E & F & G --> H[DuckDB Connection Pool]
        H --> I[(DuckDB + Arrow Ext)]
    end
    A --> B
```

---

## 📚 Usage Patterns

* **Ad‑hoc Analytics:** Point Superset or Tableau at `grpc://host:32010` and run.
* **Streaming Extracts:** Pipe result sets directly into Arrow Flight streams for ML features.
* **Embedded Mode:** Link the library, embed DuckDB, and expose Flight in‑process.

---

## 📄 License

Released under the MIT License. See [LICENSE](LICENSE) for details.
