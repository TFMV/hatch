# ClickHouse Backend Demo

This demo shows how to use Porter with ClickHouse as the backend database.

## Prerequisites

1. **ClickHouse Server**: Install and run ClickHouse
```bash
# Using Docker
docker run -d --name clickhouse-server \
  -p 9000:9000 -p 8123:8123 \
  clickhouse/clickhouse-server

# Or install locally (macOS)
brew install clickhouse
clickhouse-server
```

2. **Porter**: Build Porter with ClickHouse support
```bash
cd porter
make build
```

## Setup

### 1. Start ClickHouse
```bash
# Using Docker
docker start clickhouse-server

# Or if running locally
clickhouse-server
```

### 2. Create Sample Data in ClickHouse
```bash
# Connect to ClickHouse
clickhouse-client

# Create a sample table
CREATE TABLE sales (
    id UInt32,
    product String,
    amount Float64,
    date Date
) ENGINE = MergeTree()
ORDER BY id;

# Insert sample data
INSERT INTO sales VALUES 
    (1, 'Laptop', 999.99, '2024-01-15'),
    (2, 'Mouse', 29.99, '2024-01-16'),
    (3, 'Keyboard', 79.99, '2024-01-17'),
    (4, 'Monitor', 299.99, '2024-01-18'),
    (5, 'Tablet', 499.99, '2024-01-19');
```

### 3. Start Porter with ClickHouse Backend
```bash
# Start Porter pointing to ClickHouse
./build/bin/porter serve --database "clickhouse://localhost:9000/default?username=default"

# Or with configuration file
cat > clickhouse_config.yaml << EOF
server:
  address: "0.0.0.0:32010"
  max_connections: 100

database:
  dsn: "clickhouse://localhost:9000/default?username=default"
  max_open_conns: 32
  max_idle_conns: 8

logging:
  level: "info"

metrics:
  enabled: true
  address: ":9090"
EOF

./build/bin/porter serve --config clickhouse_config.yaml
```

## Testing the Integration

### 1. Using Python with ADBC
```python
import adbc_driver_flightsql.dbapi

# Connect to Porter (which connects to ClickHouse)
conn = adbc_driver_flightsql.dbapi.connect("grpc://localhost:32010")
cursor = conn.cursor()

# Query ClickHouse data through Porter
cursor.execute("SELECT * FROM sales ORDER BY date")
results = cursor.fetchall()

print("Sales data from ClickHouse via Porter:")
for row in results:
    print(f"ID: {row[0]}, Product: {row[1]}, Amount: ${row[2]}, Date: {row[3]}")

# Aggregate query
cursor.execute("SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY total DESC")
results = cursor.fetchall()

print("\nSales by product:")
for row in results:
    print(f"{row[0]}: ${row[1]}")

conn.close()
```

### 2. Using PyArrow Flight
```python
from pyarrow import flight
import pyarrow as pa

# Connect to Porter Flight SQL server
client = flight.FlightClient("grpc://localhost:32010")

# Execute query and get results as Arrow table
query = "SELECT product, amount, date FROM sales WHERE amount > 100"
info = client.get_flight_info(flight.FlightDescriptor.for_command(query.encode()))

# Read the data
reader = client.do_get(info.endpoints[0].ticket)
table = reader.read_all()

print("High-value sales (Arrow format):")
print(table.to_pandas())
```

### 3. Metadata Discovery
```python
import adbc_driver_flightsql.dbapi

conn = adbc_driver_flightsql.dbapi.connect("grpc://localhost:32010")
cursor = conn.cursor()

# List tables
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
print("Available tables:", [t[0] for t in tables])

# Describe table structure
cursor.execute("DESCRIBE sales")
columns = cursor.fetchall()
print("\nSales table structure:")
for col in columns:
    print(f"  {col[0]}: {col[1]}")

conn.close()
```

## Performance Testing

### 1. Load Test Data
```sql
-- Create a larger table for performance testing
CREATE TABLE performance_test (
    id UInt64,
    timestamp DateTime,
    value Float64,
    category String
) ENGINE = MergeTree()
ORDER BY (timestamp, id);

-- Insert 1M rows
INSERT INTO performance_test 
SELECT 
    number as id,
    now() - INTERVAL number SECOND as timestamp,
    rand() / 1000000000 as value,
    'category_' || toString(number % 100) as category
FROM numbers(1000000);
```

### 2. Benchmark Queries
```python
import time
import adbc_driver_flightsql.dbapi

conn = adbc_driver_flightsql.dbapi.connect("grpc://localhost:32010")
cursor = conn.cursor()

# Benchmark aggregation query
start_time = time.time()
cursor.execute("""
    SELECT category, 
           COUNT(*) as count, 
           AVG(value) as avg_value,
           MAX(value) as max_value
    FROM performance_test 
    GROUP BY category 
    ORDER BY count DESC
""")
results = cursor.fetchall()
end_time = time.time()

print(f"Aggregation query: {len(results)} results in {end_time - start_time:.3f}s")

# Benchmark time-series query
start_time = time.time()
cursor.execute("""
    SELECT toStartOfHour(timestamp) as hour,
           COUNT(*) as events,
           AVG(value) as avg_value
    FROM performance_test 
    WHERE timestamp >= now() - INTERVAL 1 DAY
    GROUP BY hour
    ORDER BY hour
""")
results = cursor.fetchall()
end_time = time.time()

print(f"Time-series query: {len(results)} results in {end_time - start_time:.3f}s")

conn.close()
```

## Benefits of Porter + ClickHouse

1. **Arrow-Native Performance**: Zero-copy data transfer from ClickHouse to clients
2. **Standard Interface**: Use any Flight SQL client with ClickHouse
3. **Connection Pooling**: Efficient connection management to ClickHouse
4. **Monitoring**: Built-in metrics and observability
5. **Cross-Platform**: Unified interface across different database backends

## Troubleshooting

### Common Issues

1. **Connection Failed**: Ensure ClickHouse is running and accessible
```bash
# Test ClickHouse connectivity
clickhouse-client --query "SELECT 1"
```

2. **Authentication Error**: Check username/password in DSN
```bash
# Test with explicit credentials
./porter serve --database "clickhouse://localhost:9000/default?username=default&password=yourpassword"
```

3. **Table Not Found**: Verify table exists in ClickHouse
```bash
clickhouse-client --query "SHOW TABLES"
```

### Debug Mode
```bash
# Run Porter with debug logging
./porter serve --database "clickhouse://localhost:9000/default" --log-level debug
```

## Next Steps

- Try the [Flight SQL throughput benchmark](../bench/flight_throughput_benchmark.go) with ClickHouse
- Explore [advanced ClickHouse features](https://clickhouse.com/docs) through Porter
- Set up monitoring with [Prometheus metrics](http://localhost:9090/metrics)
- Test with your own ClickHouse datasets and queries 