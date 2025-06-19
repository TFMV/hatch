# Porter TPC-H Benchmarking

Porter includes a built-in TPC-H benchmarking capability that leverages DuckDB's native TPC-H module for reproducible performance testing. This feature allows you to evaluate Porter's performance against industry-standard analytical workloads without requiring pre-loaded datasets.

## Features

- **Dynamic Data Generation**: Uses DuckDB's built-in `tpch` extension to generate TPC-H schema and data on-demand
- **Standard TPC-H Queries**: Supports TPC-H queries q1-q10 with plans to extend to q22
- **Multiple Output Formats**: Results can be output in table, JSON, or Apache Arrow formats
- **Comprehensive Metrics**: Includes execution time, row counts, and optional query plan analysis
- **Configurable Scale Factors**: Supports any scale factor (0.01, 0.1, 1, 10, etc.)
- **Multiple Iterations**: Run queries multiple times for statistical analysis
- **Portable**: No external dependencies or pre-loaded data required

## Usage

### Basic Usage

Run a single query with default settings:

```bash
porter bench --query q1
```

Run multiple queries with a specific scale factor:

```bash
porter bench --query q1,q3,q5 --scale 0.1
```

Run all available queries:

```bash
porter bench --all --scale 1
```

### Output Formats

#### Table Format (Default)
Human-readable table output:

```bash
porter bench --query q1 --format table
```

Output:
```
TPC-H Benchmark Results
=======================

Configuration:
  Scale Factor: 1.00
  Iterations: 1
  Total Time: 2.345s
  Started: 2025-06-19T07:09:11-05:00

Environment:
  DuckDB Version: v1.3.1
  Go Version: go1.24
  OS/Arch: darwin/arm64

Results:
Query  Iter Time         Rows     Status
-----  ---- ----         ----     ------
q1     1    125.456ms    4        OK
```

#### JSON Format
Structured JSON output for programmatic processing:

```bash
porter bench --query q1 --format json
```

Output:
```json
{
  "config": {
    "queries": ["q1"],
    "scale_factor": 1.0,
    "iterations": 1,
    "timeout": 600000000000,
    "analyze": false,
    "database": ":memory:"
  },
  "results": [
    {
      "query": "q1",
      "iteration": 1,
      "execution_time_ns": 125456000,
      "row_count": 4
    }
  ],
  "total_time_ns": 2345000000,
  "start_time": "2025-06-19T07:09:11-05:00",
  "end_time": "2025-06-19T07:09:13-05:00",
  "environment": {
    "duckdb_version": "v1.3.1",
    "go_version": "go1.24",
    "os": "darwin",
    "arch": "arm64"
  }
}
```

#### Arrow Format
Apache Arrow IPC format for efficient data interchange:

```bash
porter bench --query q1 --format arrow --output results.arrow
```

### Advanced Options

#### Multiple Iterations
Run each query multiple times for statistical analysis:

```bash
porter bench --query q1 --iterations 5 --scale 1
```

#### Query Plan Analysis
Include query execution plans in the results:

```bash
porter bench --query q1 --analyze --format json
```

#### Custom Timeout
Set a custom timeout for long-running queries:

```bash
porter bench --all --scale 10 --timeout 30m
```

#### File Output
Save results to a file instead of stdout:

```bash
porter bench --all --scale 1 --format json --output benchmark_results.json
```

#### Custom Database Path
Use a persistent database instead of in-memory:

```bash
porter bench --query q1 --database ./benchmark.db
```

## Command Reference

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--query` | `-q` | | TPC-H query to run (e.g., 'q1' or 'q1,q5,q22') |
| `--all` | | `false` | Run all available TPC-H queries |
| `--scale` | `-s` | `1.0` | TPC-H scale factor (0.01, 0.1, 1, 10, etc.) |
| `--format` | `-f` | `table` | Output format: table, json, arrow |
| `--output` | `-o` | | Output file path (stdout if not specified) |
| `--iterations` | | `1` | Number of iterations per query |
| `--analyze` | | `false` | Include query plan analysis |
| `--timeout` | | `10m` | Query timeout |
| `--database` | | `:memory:` | DuckDB database path |
| `--log-level` | | `info` | Log level (debug, info, warn, error) |

### Available Queries

Currently supported TPC-H queries:

- **q1**: Pricing Summary Report
- **q2**: Minimum Cost Supplier
- **q3**: Shipping Priority
- **q4**: Order Priority Checking
- **q5**: Local Supplier Volume
- **q6**: Forecasting Revenue Change
- **q7**: Volume Shipping
- **q8**: National Market Share
- **q9**: Product Type Profit Measure
- **q10**: Returned Item Reporting

## Examples

### Performance Testing Workflow

1. **Quick Smoke Test** (small scale, fast):
```bash
porter bench --all --scale 0.01 --format table
```

2. **Development Benchmarking** (medium scale):
```bash
porter bench --all --scale 0.1 --iterations 3 --format json --output dev_benchmark.json
```

3. **Production Performance Testing** (full scale):
```bash
porter bench --all --scale 1 --iterations 5 --timeout 30m --format json --output prod_benchmark.json
```

4. **Query-Specific Analysis** (with plans):
```bash
porter bench --query q1,q3,q5 --scale 1 --analyze --format json --output query_analysis.json
```

### Continuous Integration

Use in CI/CD pipelines for performance regression testing:

```bash
# Run lightweight benchmark
porter bench --query q1,q6 --scale 0.01 --format json --output ci_benchmark.json

# Compare with baseline (pseudo-code)
python compare_benchmarks.py baseline.json ci_benchmark.json
```

## Implementation Details

### TPC-H Data Generation

Porter uses DuckDB's built-in TPC-H extension which:

1. **Installs the TPC-H extension**: `INSTALL tpch`
2. **Loads the extension**: `LOAD tpch`
3. **Generates data**: `CALL dbgen(sf=<scale_factor>)`

This approach ensures:
- **Reproducibility**: Same data every time for a given scale factor
- **Portability**: No external files or dependencies
- **Performance**: Optimized data generation directly in DuckDB
- **Flexibility**: Any scale factor supported by DuckDB

### Query Execution

Each query execution includes:

1. **Plan Analysis** (if requested): `EXPLAIN <query>`
2. **Timed Execution**: Measure wall-clock time
3. **Result Counting**: Count returned rows
4. **Error Handling**: Capture and report failures

### Output Formats

- **Table**: Human-readable console output
- **JSON**: Structured data with full metadata
- **Arrow**: Efficient binary format for data analysis tools

## Best Practices

### Scale Factor Selection

- **Development/Testing**: Use 0.01 or 0.1 for fast iteration
- **CI/CD**: Use 0.01 for quick regression testing
- **Performance Analysis**: Use 1.0 or higher for realistic workloads
- **Stress Testing**: Use 10+ for large-scale evaluation

### Iteration Guidelines

- **Single Run**: Good for development and quick checks
- **3-5 Iterations**: Better for performance analysis
- **10+ Iterations**: Statistical significance for benchmarking

### Timeout Considerations

- **Small Scale (≤0.1)**: Default 10m is usually sufficient
- **Medium Scale (1.0)**: Consider 30m for complex queries
- **Large Scale (≥10)**: May need 1h+ for some queries

## Troubleshooting

### Common Issues

1. **Out of Memory**: Reduce scale factor or use persistent database
2. **Timeout**: Increase timeout or reduce scale factor
3. **Missing TPC-H Extension**: Ensure DuckDB includes TPC-H support

### Performance Tips

1. **Use Persistent Database**: For multiple runs, avoid re-generating data
2. **Warm-up Runs**: First iteration may be slower due to cold start
3. **Resource Monitoring**: Monitor CPU/memory usage during benchmarks

## Future Enhancements

Planned improvements include:

- **Complete TPC-H Suite**: Support for all 22 TPC-H queries
- **TPC-DS Support**: Additional benchmark suite
- **Custom Queries**: Support for user-defined benchmark queries
- **Statistical Analysis**: Built-in statistical reporting
- **Comparison Tools**: Compare results across runs
- **Flight SQL Integration**: Benchmark over Flight SQL protocol
- **Visualization**: Generate performance charts and reports 