#!/usr/bin/env python3
"""Simple Flight SQL example with Porter.

Connects to a running Porter instance and executes a few queries
using the ADBC Flight SQL driver.
"""

import adbc_driver_flightsql.dbapi

# Connect to Porter (default port 32010)
conn = adbc_driver_flightsql.dbapi.connect("grpc://localhost:32010")
cursor = conn.cursor()

# Run a basic query and fetch all rows
cursor.execute("SELECT 42 AS answer")
print("Rows:", cursor.fetchall())

# Fetch an Arrow table for zero-copy access
cursor.execute("SELECT 'hello' AS greeting")
print("Arrow:", cursor.fetch_arrow_table())

cursor.close()
conn.close()
