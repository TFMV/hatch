# MotherDuck Backend

Porter can connect to [MotherDuck](https://motherduck.com) using DuckDB's remote connector. The connection string uses the `duckdb` scheme with the `motherduck` host.

```bash
porter serve --database "duckdb://motherduck/my_db" --token "$MOTHERDUCK_TOKEN"
```

## Authentication

MotherDuck requires an authentication token. Porter reads the token from the `--token` flag or the `MOTHERDUCK_TOKEN` environment variable. The token value is injected into the DSN as the `motherduck_token` query parameter.

Tokens are never printed in logs. Ensure TLS is enabled when connecting over the network.

## Examples

```bash
# Using environment variable
export MOTHERDUCK_TOKEN=your_token
porter serve --database "duckdb://motherduck/my_db"

# Explicit flag
porter serve --database "duckdb://motherduck/my_db" --token your_token
```

## Limitations

- No user-defined functions or local file access on the MotherDuck side.
- Hybrid execution optimisation is not yet implemented.
