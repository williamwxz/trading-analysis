"""
ClickHouse Cloud client — single shared connection for all assets.

Uses clickhouse-connect (native HTTP/HTTPS) for ClickHouse Cloud.
Replaces the per-asset urllib HTTP code from falcon-lakehouse.
"""

import os
from typing import Dict, List, Optional, Tuple

import clickhouse_connect
from clickhouse_connect.driver.client import Client


def get_client() -> Client:
    """Create a ClickHouse Cloud client from env vars."""
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8443")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        secure=os.getenv("CLICKHOUSE_SECURE", "true").lower() == "true",
        connect_timeout=15,
        send_receive_timeout=600,
    )


def query_rows(sql: str, client: Optional[Client] = None) -> List[List]:
    """Execute a query, return rows as list of lists."""
    c = client or get_client()
    result = c.query(sql)
    return [list(row) for row in result.result_rows]


def query_dicts(sql: str, client: Optional[Client] = None) -> List[Dict]:
    """Execute a query, return rows as list of dicts."""
    c = client or get_client()
    result = c.query(sql)
    cols = result.column_names
    return [dict(zip(cols, row)) for row in result.result_rows]


def query_scalar(sql: str, client: Optional[Client] = None):
    """Execute a query, return the first value of the first row."""
    c = client or get_client()
    result = c.query(sql)
    if result.result_rows:
        return result.result_rows[0][0]
    return None


def execute(sql: str, client: Optional[Client] = None) -> None:
    """Execute a DDL/DML statement."""
    c = client or get_client()
    c.command(sql)


def insert_rows(
    table: str,
    columns: List[str],
    rows: List[List],
    client: Optional[Client] = None,
    batch_size: int = 200_000,
) -> int:
    """Bulk insert rows into a table in batches. Returns total rows inserted."""
    c = client or get_client()
    total = len(rows)
    for start in range(0, total, batch_size):
        batch = rows[start : start + batch_size]
        c.insert(table, batch, column_names=columns)
    return total
