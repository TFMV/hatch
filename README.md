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

## License

Released under the MIT License. See [LICENSE](LICENSE) for details.

---

