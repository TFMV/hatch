#!/usr/bin/env python3
"""Plot data queried through Porter using pandas and matplotlib."""

import adbc_driver_flightsql.dbapi
import pandas as pd
import matplotlib.pyplot as plt

# Connect to Porter and load some sample data
conn = adbc_driver_flightsql.dbapi.connect("grpc://localhost:32010")

# Example query: replace with your own table
query = "SELECT timestamp, value FROM performance_test LIMIT 1000"

df = pd.read_sql(query, conn)

df['timestamp'] = pd.to_datetime(df['timestamp'])

df.set_index('timestamp')['value'].plot(title='Value over time')
plt.xlabel('Timestamp')
plt.ylabel('Value')
plt.tight_layout()
plt.show()

conn.close()
