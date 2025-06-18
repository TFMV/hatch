# 🐣 Hatch

*Zero‑copy analytics, delivered at Mach Arrow.*

[![Go Report Card](https://goreportcard.com/badge/github.com/TFMV/hatch)](https://goreportcard.com/report/github.com/TFMV/hatch)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build and Test](https://github.com/TFMV/hatch/actions/workflows/ci.yml/badge.svg)](https://github.com/TFMV/hatch/actions/workflows/ci.yml)

## 🧠 The Big Idea

A great engine shouldn't be gated by heavyweight infra.
Hatch keeps DuckDB's magic small, open, and composable so your data can fly wherever you need it.

---

Hatch is under active development.
It is experimental and **not yet ready for use.**

For a stable implementation, see GizmoSQL:
👉 <https://github.com/gizmodata/gizmosql>

*Please avoid submitting Hatch to package managers, registries, or third-party distributions until it reaches a stable release. Premature packaging may cause confusion or misrepresent the project's status.*

---

## Why Hatch Exists

### 1. Arrow-Native Networking Is Inevitable  

Flight SQL moves columnar data faster than REST or JDBC, with schemas baked in. DuckDB already "speaks Arrow" internally. Hatch lets it **broadcast**.

### 2. Self-Hosted ≠ Heavyweight  

Options today: embed DuckDB yourself, bolt it onto Python/Java web servers, or go proprietary.  
Hatch offers a **third way**: a small server that does one thing: serve SQL over Flight.

### 3. Pipelines Need Lightweight Nodes  

Modern data stacks are Lego bricks: Redis for cache, NATS for events, DuckDB for OLAP. Hatch slots into that ecosystem—just stream Arrow in, stream Arrow out.

### 4. A Playground for Arrow Minds  

Want column-level ACLs? Write a middleware.  
Need OpenTelemetry spans? Drop in an interceptor.  
Curious about WASM UDFs? Fork and go wild.  
Hatch is scaffolding, not a silo.

---

## License

Released under the MIT License. See [LICENSE](LICENSE) for details.
