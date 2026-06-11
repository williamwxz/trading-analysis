"""ClickHouse Cloud client for libs — no Dagster dependency.

Uses clickhouse-connect (native HTTP/HTTPS) for ClickHouse Cloud.
Required env vars: CLICKHOUSE_HOST, CLICKHOUSE_PORT (8443),
                   CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_SECURE (true).
"""

import os
from typing import Any, Dict, List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client

# Default per-query memory controls applied to EVERY client/session.
#
# ClickHouse Cloud leaves max_memory_usage at 0 (unlimited per query), so a
# single heavy consumer cold-start query (bootstrap seed + 2h walk across all
# strategies) can try to allocate the whole server's memory and trip
# `(total) memory limit exceeded`, OOM-crashing the service and competing with
# background merges — turning a transient spike into a crash loop.
#
# Capping per-query memory and lowering the external-aggregation/sort thresholds
# forces ClickHouse to spill GROUP BY / ORDER BY to local disk instead of holding
# everything in RAM (verified working on this Cloud instance/user). The consumer
# therefore cannot monopolize server memory and bootstraps fit alongside merges.
# Disk-spill trades RAM for I/O (slightly slower) — fine for a cold start. Spill
# thresholds sit below the per-query cap so spilling engages before the cap is hit.
# Callers may override any key via get_client(settings=...).
_DEFAULT_QUERY_SETTINGS: Dict[str, Any] = {
    "max_memory_usage": 3_000_000_000,  # 3 GiB hard cap per query
    "max_bytes_before_external_group_by": 1_500_000_000,  # spill GROUP BY at 1.5 GiB
    "max_bytes_before_external_sort": 1_500_000_000,  # spill ORDER BY at 1.5 GiB
}


def get_client(settings: Optional[Dict[str, Any]] = None) -> Client:
    """Create a ClickHouse Cloud client from env vars.

    Applies _DEFAULT_QUERY_SETTINGS (per-query memory cap + disk-spill) to every
    session so no single query can exhaust the Cloud server's memory. Any keys in
    ``settings`` are merged on top, taking precedence over the defaults.
    """
    merged_settings = {**_DEFAULT_QUERY_SETTINGS, **(settings or {})}
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8443")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        secure=os.getenv("CLICKHOUSE_SECURE", "true").lower() == "true",
        connect_timeout=15,
        send_receive_timeout=600,
        settings=merged_settings,
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
