import subprocess
import time
import os
import duckdb

try:
    from pyarrow import flight
    from pyarrow.flight import sql as flightsql
    import pyarrow as pa
except ImportError as e:
    raise SystemExit(f"pyarrow not installed: {e}")


def start_server():
    env = os.environ.copy()
    proc = subprocess.Popen(
        ["go", "run", "./cmd/server", "serve", "--address", "localhost:32010", "--database", ":memory:", "--log-level", "error"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    time.sleep(2)
    return proc


def stop_server(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def roundtrip_query(client, query, params=None):
    if params is None:
        info = client.execute(query)
    else:
        prepared = client.prepare(query)
        prepared.execute_update(params)
        info = prepared.execute()
        prepared.close()
    return client._client.get_stream(info.endpoints[0].ticket).read_all()


def test_cross_client_roundtrip():
    proc = start_server()
    try:
        client = flightsql.FlightSqlClient("grpc://localhost:32010")
        client._client.wait_for_ready(timeout=5)
        client.execute_update("CREATE TABLE py_test(id INTEGER, name TEXT)")
        data = pa.Table.from_pydict({"id": [1], "name": ["flight"]})
        prep = client.prepare("INSERT INTO py_test VALUES (?, ?)")
        prep.execute_update(data)
        prep.close()
        table = roundtrip_query(client, "SELECT * FROM py_test")
        expected = duckdb.sql("SELECT 1 AS id, 'flight' AS name").arrow()
        assert table.equals(expected)

        # metadata roundtrips
        meta_tables = client.get_tables().read_all()
        meta_cols = client.get_columns(None, None, "py_test", None).read_all()
        sql_info = client.get_sql_info([flightsql.SqlInfo.FLIGHT_SQL_SERVER_NAME])
        assert meta_tables.num_rows >= 1
        assert meta_cols.num_rows == 1
        assert len(sql_info[0]) > 0
    finally:
        stop_server(proc)
