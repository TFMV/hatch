#!/usr/bin/env python3
"""
Run a trivial Flight SQL query and measure how long each step takes.

Usage
-----
    python test_client.py          # connects to grpc://localhost:32010
    python test_client.py --url grpc://my-host:port
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, List
import struct

import pyarrow as pa
import pyarrow.flight as fl


@contextmanager
def stopwatch(label: str, timings: Dict[str, float]) -> None:  # noqa: D401
    """Context manager that records the wall clock time for *label*."""
    start = time.perf_counter()
    yield
    timings[label] = time.perf_counter() - start


@dataclass
class Result:
    """Container for the Flight SQL result and its timings."""

    table: pa.Table
    timings: Dict[str, float]

    def __str__(self) -> str:  # pretty print
        timing_lines: List[str] = [
            f"  {k:<10s}: {v * 1_000:.3f} ms" for k, v in self.timings.items()
        ]
        total = sum(self.timings.values())
        timing_lines.append(f"  {'total':<10s}: {total * 1_000:.3f} ms")
        return f"{self.table.to_pylist()}\nTimings:\n" + "\n".join(timing_lines)


def create_flight_sql_command(query: str) -> bytes:
    """Create a Flight SQL CommandStatementQuery wrapped in Any."""
    # Create the CommandStatementQuery
    query_bytes = query.encode("utf-8")
    query_field_type = struct.pack("B", 10)  # field 1, wire type 2
    query_field_len = struct.pack("B", len(query_bytes))
    cmd_query = query_field_type + query_field_len + query_bytes

    # Create the Any wrapper
    type_url = "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
    type_url_bytes = type_url.encode("utf-8")

    # Field 1: type_url
    type_field_type = struct.pack("B", 10)  # field 1, wire type 2
    type_field_len = struct.pack("B", len(type_url_bytes))

    # Field 2: value
    value_field_type = struct.pack("B", 18)  # field 2, wire type 2
    value_field_len = struct.pack("B", len(cmd_query))

    return (
        type_field_type
        + type_field_len
        + type_url_bytes
        + value_field_type
        + value_field_len
        + cmd_query
    )


def run_query(url: str, sql: str) -> Result:
    """Connect to *url*, run *sql*, and return the table plus per‑step timings."""
    timings: Dict[str, float] = {}

    with stopwatch("connect", timings):
        client = fl.FlightClient(url)

    with stopwatch("prepare", timings):
        # Create proper Flight SQL command
        cmd = create_flight_sql_command(sql)
        descriptor = fl.FlightDescriptor.for_command(cmd)
        info = client.get_flight_info(descriptor)

    # Get the first endpoint
    endpoint = info.endpoints[0]
    ticket = endpoint.ticket

    with stopwatch("get", timings):
        reader = client.do_get(ticket)

    with stopwatch("fetch", timings):
        table = reader.read_all()

    client.close()
    return Result(table=table, timings=timings)


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flight SQL latency tester")
    parser.add_argument(
        "--url",
        default="grpc://localhost:32010",
        help="Flight SQL server location (default: grpc://localhost:32010)",
    )
    parser.add_argument(
        "--sql",
        default="SELECT 42 AS answer",
        help='SQL statement to run (default: "SELECT 42 AS answer")',
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug logging"
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s – %(message)s",
    )

    logging.info("Connecting to %s …", args.url)
    try:
        result = run_query(args.url, args.sql)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Flight SQL query failed: %s", exc)
        sys.exit(1)

    print(result)


if __name__ == "__main__":
    main()
