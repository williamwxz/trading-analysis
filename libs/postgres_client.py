"""Postgres client for libs — used by streaming pipeline checkpointing.

Mirrors libs.clickhouse_client shape. Connections are not pooled by default;
callers pass `client=` for batched operations.

Required env vars (with defaults):
  SUPABASE_HOST        (localhost)
  SUPABASE_PORT        (5432)
  SUPABASE_USER        (postgres)
  SUPABASE_PASSWORD    (empty)
  SUPABASE_DATABASE    (postgres)
  SUPABASE_SSLMODE     (prefer)  -- use "require" against Supabase
"""

import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import Connection


def get_client() -> Connection:
    """Create a Postgres client from env vars. Caller owns the connection."""
    return psycopg.connect(
        host=os.getenv("SUPABASE_HOST", "localhost"),
        port=int(os.getenv("SUPABASE_PORT", "5432")),
        user=os.getenv("SUPABASE_USER", "postgres"),
        password=os.getenv("SUPABASE_PASSWORD", ""),
        dbname=os.getenv("SUPABASE_DATABASE", "postgres"),
        sslmode=os.getenv("SUPABASE_SSLMODE", "prefer"),
        connect_timeout=15,
    )


def execute(
    sql: str,
    params: Sequence[Any] | None = None,
    client: Connection | None = None,
) -> None:
    """Execute a single DDL/DML statement. Commits on owned connection."""
    own = client is None
    c = client or get_client()
    try:
        with c.cursor() as cur:
            cur.execute(sql, params)
        if own:
            c.commit()
    finally:
        if own:
            c.close()


def query_rows(
    sql: str,
    params: Sequence[Any] | None = None,
    client: Connection | None = None,
) -> list[list[Any]]:
    """Execute a query, return rows as list of lists."""
    own = client is None
    c = client or get_client()
    try:
        with c.cursor() as cur:
            cur.execute(sql, params)
            return [list(row) for row in cur.fetchall()]
    finally:
        if own:
            c.close()


def query_dicts(
    sql: str,
    params: Sequence[Any] | None = None,
    client: Connection | None = None,
) -> list[dict[str, Any]]:
    """Execute a query, return rows as list of dicts."""
    own = client is None
    c = client or get_client()
    try:
        with c.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description] if cur.description else []
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        if own:
            c.close()


@contextmanager
def transaction(client: Connection) -> Iterator[Connection]:
    """Context manager: commit on success, rollback on exception.

    Caller-supplied connection only (does not open one). Use with `get_client()`
    explicitly or with a long-lived connection.
    """
    try:
        yield client
        client.commit()
    except Exception:
        client.rollback()
        raise
