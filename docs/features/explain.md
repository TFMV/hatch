# Query Plan Explain

Porter exposes a lightweight explain facility that allows inspecting how a SQL query would be executed without running it. The feature is available for DuckDB backed deployments.

```bash
porter query "SELECT * FROM events LIMIT 10" --explain
```

Typical output:

```
Backend: duckdb
Plan: sequential scan
Estimated Rows: 10
Execution Mode: streamed
Cache: cold
```

ClickHouse backends currently return a `UNIMPLEMENTED` error when requesting an explanation.
