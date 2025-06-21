#!/usr/bin/env python3
"""Load data from Porter into a Polars DataFrame using Arrow RecordBatches."""

import adbc_driver_flightsql.dbapi
import pyarrow as pa
import polars as pl

# Connect to Porter (running on localhost:32010 by default)
conn = adbc_driver_flightsql.dbapi.connect("grpc://localhost:32010")
cursor = conn.cursor()

# Example query - modify as needed for your own schema
cursor.execute("SELECT 1 AS id, 'example' AS label")

# Collect all Arrow record batches
batches = []
batch = cursor.fetch_record_batch()
while batch is not None:
    batches.append(batch)
    batch = cursor.fetch_record_batch()

# Build a PyArrow table then convert to Polars
arrow_table = pa.Table.from_batches(batches)
df = pl.from_arrow(arrow_table)
print(df)

cursor.close()
conn.close()
