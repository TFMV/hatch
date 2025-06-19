#!/usr/bin/env python3

try:
    from pyarrow import flight
    from pyarrow.flight import sql as flightsql
    import pyarrow as pa
    
    client = flightsql.FlightSqlClient('grpc://localhost:32010')
    client._client.wait_for_ready(timeout=5)
    print('Connected to Porter Flight SQL server')
    
    # Test simple query
    client.execute_update('DROP TABLE IF EXISTS py_test')
    client.execute_update('CREATE TABLE py_test(id INTEGER, name TEXT)')
    client.execute_update("INSERT INTO py_test VALUES (1, 'hello')")
    print('Created table and inserted data')
    
    # Test query
    info = client.execute('SELECT * FROM py_test')
    print(f'Got flight info with {len(info.endpoints)} endpoints')
    
    table = client._client.get_stream(info.endpoints[0].ticket).read_all()
    print(f'Retrieved table with {table.num_rows} rows')
    print(table.to_pydict())
    print('Test completed successfully!')
    
except ImportError as e:
    print(f'pyarrow not installed: {e}')
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc() 