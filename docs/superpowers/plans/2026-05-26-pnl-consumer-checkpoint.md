# PnL Consumer Supabase Checkpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist `pnl_consumer` `AnchorState` to Supabase Postgres so cold start restores from the checkpoint instead of issuing a large ClickHouse query, while keeping the existing 48h bootstrap as a fallback safety net.

**Architecture:** Atomic checkpoint pattern (Flink/Kafka-Streams aligned). After each successful ClickHouse flush, best-effort UPSERT to Supabase (`streaming.pnl_checkpoint` + `streaming.pnl_commit_state` in one transaction). On cold start, run cheap sanity checks (schema version, freshness, Kafka offset match, state-hash, optional ClickHouse invariant cross-check); on any failure, fall back to the unchanged `_bootstrap_state` / `_bootstrap_bt_state`.

**Tech Stack:** Python 3.11, psycopg 3, Supabase Postgres, ClickHouse Cloud, Redpanda/Kafka, ECS Fargate, Terraform, pytest.

**Spec reference:** `docs/superpowers/specs/2026-05-26-pnl-consumer-checkpoint-design.md`

---

## File structure

**New files (production code):**
- `libs/postgres_client.py` — generic Postgres connection + helpers (mirrors `libs/clickhouse_client.py`)
- `libs/computation/checkpoint_store.py` — read/write/hash logic + `SCHEMA_VERSION`
- `services/pnl_consumer/pnl_consumer/cold_start.py` — checkpoint try / sanity checks / fallback orchestration

**New files (schema / infra):**
- `infra/schemas/streaming_supabase.sql` — Supabase schema DDL
- `infra/grafana/dashboards/streaming-checkpoint.json` — monitoring dashboard

**New files (tests):**
- `libs/tests/test_postgres_client.py`
- `libs/tests/test_checkpoint_store.py`
- `services/pnl_consumer/tests/test_cold_start_sanity_checks.py`
- `services/pnl_consumer/tests/test_cold_start_orchestration.py`
- `services/pnl_consumer/tests/test_real_trade_revision_restore.py`
- `services/pnl_consumer/tests/test_state_serialization_roundtrip.py`
- `services/pnl_consumer/tests/test_checkpoint_postgres_integration.py`
- `services/pnl_consumer/tests/test_cold_start_invariant_integration.py`
- `services/pnl_consumer/tests/test_cold_start_e2e.py`
- `services/pnl_consumer/tests/test_real_trade_recovery_e2e.py`

**Modified files:**
- `pyproject.toml` — add `psycopg[binary]>=3.1` dependency
- `services/pnl_consumer/pnl_consumer/pnl_consumer.py` — wire cold-start + flush checkpoint write
- `infra/terraform/main.tf` — desired_count for bt/real-trade, env vars, Supabase secret, sink flags

---

## Task ordering

- **Phase 0 (separate PR):** Task 1 — scale up bt/real-trade. Ships first; provides a known-good baseline.
- **Phase 1 (foundation):** Tasks 2–8 — deps, schema file, postgres_client, checkpoint_store.
- **Phase 2 (cold start):** Tasks 9–13 — serialization test, sanity checks, orchestration, invariant check.
- **Phase 3 (correctness):** Tasks 14, 18 — real-trade revision restore tests, crash-restart property test.
- **Phase 4 (wire-up):** Tasks 15–16 — modify pnl_consumer.py for flush write + cold-start entry.
- **Phase 5 (E2E):** Task 17 — Postgres-backed cold-start scenarios.
- **Phase 6 (infra):** Tasks 19–20 — Terraform env vars + Secrets Manager + Grafana dashboard.
- **Phase 7 (rollout):** Tasks 21–24 — apply schema, shadow deploy, per-mode read enablement, cleanup.

Tasks 1–20 are normal code/PR tasks. Tasks 21–24 are operational runbooks; they describe procedures and verifications, not code.

---

## Task 1: Scale up bt and real-trade ECS services (Phase 0)

**Files:**
- Modify: `infra/terraform/main.tf:1263,1272`

This is a standalone change that ships as its own PR before any checkpoint work begins. It enables the bt and real-trade sinks on the current code path (48h bootstrap) so we have a known-good baseline.

- [ ] **Step 1: Modify Terraform local for real-trade**

In `infra/terraform/main.tf` change line 1263:

```diff
     real-trade = {
       enable_price      = "false"
       enable_prod       = "false"
       enable_real_trade = "true"
       enable_bt         = "false"
       group_id          = "pnl-consumer-real-trade-3"
-      desired_count     = 0
+      desired_count     = 1
       clickhouse_user   = "streaming"
     }
```

- [ ] **Step 2: Modify Terraform local for bt**

In `infra/terraform/main.tf` change line 1272:

```diff
     bt = {
       enable_price      = "false"
       enable_prod       = "false"
       enable_real_trade = "false"
       enable_bt         = "true"
       group_id          = "pnl-consumer-bt"
-      desired_count     = 0
+      desired_count     = 1
       clickhouse_user   = "streaming"
     }
```

- [ ] **Step 3: Run terraform plan**

```bash
cd infra/terraform && terraform plan
```

Expected output includes two service updates:
```
  ~ resource "aws_ecs_service" "pnl_consumer" {
        ~ desired_count                      = 0 -> 1
        # (for each.key == "bt" and each.key == "real-trade")
    }
```

No other resources should change.

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "$(cat <<'EOF'
infra(terraform): enable pnl-consumer bt and real-trade sinks

Set desired_count=1 for bt and real-trade ECS services. Both will run
the existing 48h ClickHouse bootstrap on cold start. Provides a
known-good baseline before introducing Supabase checkpointing.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 5: Post-merge verification (manual, after deploy)**

After CI/CD applies the change:

```bash
aws ecs describe-services \
  --cluster trading-analysis \
  --services trading-analysis-pnl-consumer-bt trading-analysis-pnl-consumer-real-trade \
  --region ap-northeast-1 \
  --query 'services[].{name:serviceName,desired:desiredCount,running:runningCount,pending:pendingCount}'
```

Expected: both services show `desired:1, running:1, pending:0` within ~10 minutes. Check CloudWatch logs for both services — should see successful bootstrap completion log lines.

Watch 48h before starting any checkpoint work. If either service is unstable, fix that first.

---

## Task 2: Add psycopg dependency

**Files:**
- Modify: `pyproject.toml:24`

- [ ] **Step 1: Add psycopg to dependencies**

In `pyproject.toml`, modify the `dependencies` list (line 10-24) to add `psycopg[binary]>=3.1`:

```diff
 dependencies = [
     "dagster==1.13.2",
     "dagster-webserver==1.13.2",
     "dagster-postgres==0.29.2",
     "dagster-aws>=0.25,<1.0",
     "boto3>=1.34",
     "clickhouse-connect>=0.8",
     "confluent-kafka>=2.4",
     "requests>=2.31",
     "python-dateutil>=2.8",
     "pandas>=2.0",
     "ccxt>=4.2",
     "loguru>=0.7",
     "websockets>=12.0",
+    "psycopg[binary]>=3.1",
 ]
```

- [ ] **Step 2: Install and verify**

```bash
pip install -e ".[dev]"
python -c "import psycopg; print(psycopg.__version__)"
```

Expected: prints a version string >= 3.1.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "deps: add psycopg[binary] for Supabase Postgres client"
```

---

## Task 3: Create Supabase schema file

**Files:**
- Create: `infra/schemas/streaming_supabase.sql`

- [ ] **Step 1: Write the schema file**

Create `infra/schemas/streaming_supabase.sql` with this exact content:

```sql
-- Streaming pipeline state in Supabase Postgres.
-- Apply once manually: psql "<SUPABASE_CONN_STRING>" -f streaming_supabase.sql
-- Idempotent (all statements use IF NOT EXISTS).

CREATE SCHEMA IF NOT EXISTS streaming;

-- ─────────────────────────────────────────────────────────────────────────────
-- streaming.pnl_checkpoint
-- One row per (mode, strategy_table_name). Source of truth for AnchorState.
-- bar_ts and revision_ts are NOT NULL — the "no anchor yet" sentinel is
-- datetime.min (0001-01-01 00:00:00+00), matching AnchorRecord defaults.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS streaming.pnl_checkpoint (
    mode                  TEXT             NOT NULL,
    strategy_table_name   TEXT             NOT NULL,
    pnl                   DOUBLE PRECISION NOT NULL,
    price                 DOUBLE PRECISION NOT NULL,
    position              DOUBLE PRECISION NOT NULL,
    bar_ts                TIMESTAMPTZ      NOT NULL,
    revision_ts           TIMESTAMPTZ      NOT NULL,
    strategy_id           INTEGER          NOT NULL DEFAULT 0,
    strategy_name         TEXT             NOT NULL DEFAULT '',
    underlying            TEXT             NOT NULL DEFAULT '',
    config_timeframe      TEXT             NOT NULL DEFAULT '',
    weighting             DOUBLE PRECISION NOT NULL DEFAULT 0,
    strategy_instance_id  TEXT             NOT NULL DEFAULT '',
    final_signal          DOUBLE PRECISION NOT NULL DEFAULT 0,
    benchmark             DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (mode, strategy_table_name)
);

CREATE INDEX IF NOT EXISTS idx_pnl_checkpoint_mode_updated
    ON streaming.pnl_checkpoint (mode, updated_at);

-- ─────────────────────────────────────────────────────────────────────────────
-- streaming.pnl_commit_state
-- One row per mode. Atomic-commit anchor: Kafka offset + state_hash + version.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS streaming.pnl_commit_state (
    mode             TEXT        NOT NULL PRIMARY KEY,
    last_candle_ts   TIMESTAMPTZ NOT NULL,
    kafka_topic      TEXT        NOT NULL,
    kafka_partition  INTEGER     NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    state_hash       TEXT        NOT NULL,
    schema_version   INTEGER     NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- [ ] **Step 2: Validate SQL is syntactically valid**

```bash
python -c "import psycopg.sql; sql=open('infra/schemas/streaming_supabase.sql').read(); print('lines:', len(sql.splitlines()))"
```

Expected: prints a line count > 30 (no Python exception means the file was readable; syntax is validated later when applied to Postgres).

- [ ] **Step 3: Commit**

```bash
git add infra/schemas/streaming_supabase.sql
git commit -m "schemas(supabase): add streaming.pnl_checkpoint + pnl_commit_state"
```

---

## Task 4: Implement libs/postgres_client.py — get_client + execute

**Files:**
- Create: `libs/postgres_client.py`
- Create: `libs/tests/test_postgres_client.py`

Mirror the shape of `libs/clickhouse_client.py:1-71` for consistency.

- [ ] **Step 1: Write failing test for get_client()**

Create `libs/tests/test_postgres_client.py`:

```python
"""Unit tests for libs.postgres_client.

These tests do not require a live Postgres — they verify the client builds
the correct connection string from environment variables.
"""

from unittest import mock

import pytest


@pytest.mark.unit
def test_get_client_uses_env_vars(monkeypatch):
    monkeypatch.setenv("SUPABASE_HOST", "db.example.supabase.co")
    monkeypatch.setenv("SUPABASE_PORT", "6543")
    monkeypatch.setenv("SUPABASE_USER", "postgres")
    monkeypatch.setenv("SUPABASE_PASSWORD", "secret")
    monkeypatch.setenv("SUPABASE_DATABASE", "postgres")
    monkeypatch.setenv("SUPABASE_SSLMODE", "require")

    fake_conn = mock.MagicMock()
    with mock.patch("psycopg.connect", return_value=fake_conn) as mock_connect:
        from libs.postgres_client import get_client
        client = get_client()

    assert client is fake_conn
    mock_connect.assert_called_once()
    kwargs = mock_connect.call_args.kwargs
    assert kwargs["host"] == "db.example.supabase.co"
    assert kwargs["port"] == 6543
    assert kwargs["user"] == "postgres"
    assert kwargs["password"] == "secret"
    assert kwargs["dbname"] == "postgres"
    assert kwargs["sslmode"] == "require"


@pytest.mark.unit
def test_get_client_defaults(monkeypatch):
    for var in ["SUPABASE_HOST", "SUPABASE_PORT", "SUPABASE_USER",
                "SUPABASE_PASSWORD", "SUPABASE_DATABASE", "SUPABASE_SSLMODE"]:
        monkeypatch.delenv(var, raising=False)

    fake_conn = mock.MagicMock()
    with mock.patch("psycopg.connect", return_value=fake_conn) as mock_connect:
        from libs.postgres_client import get_client
        get_client()

    kwargs = mock_connect.call_args.kwargs
    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == 5432
    assert kwargs["user"] == "postgres"
    assert kwargs["dbname"] == "postgres"
    assert kwargs["sslmode"] == "prefer"
```

- [ ] **Step 2: Run the test — verify it fails**

```bash
pytest libs/tests/test_postgres_client.py -v
```

Expected: fails with `ModuleNotFoundError: No module named 'libs.postgres_client'`.

- [ ] **Step 3: Create libs/postgres_client.py with get_client() and execute()**

Create `libs/postgres_client.py`:

```python
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
from typing import Any, Dict, List, Optional, Sequence

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
    params: Optional[Sequence[Any]] = None,
    client: Optional[Connection] = None,
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
```

- [ ] **Step 4: Run the test — verify it passes**

```bash
pytest libs/tests/test_postgres_client.py -v
```

Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add libs/postgres_client.py libs/tests/test_postgres_client.py
git commit -m "libs: add postgres_client.get_client + execute"
```

---

## Task 5: Implement libs/postgres_client.py — query_rows, query_dicts, transaction

**Files:**
- Modify: `libs/postgres_client.py`
- Modify: `libs/tests/test_postgres_client.py`

- [ ] **Step 1: Append tests for query helpers and transaction**

Append to `libs/tests/test_postgres_client.py`:

```python
@pytest.mark.unit
def test_query_rows_returns_lists(monkeypatch):
    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_cur.fetchall.return_value = [(1, "a"), (2, "b")]
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    from libs.postgres_client import query_rows
    rows = query_rows("SELECT 1, 'a'", client=fake_conn)
    assert rows == [[1, "a"], [2, "b"]]


@pytest.mark.unit
def test_query_dicts_returns_dicts(monkeypatch):
    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_cur.description = [("id", None), ("name", None)]
    fake_cur.fetchall.return_value = [(1, "a"), (2, "b")]
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    from libs.postgres_client import query_dicts
    rows = query_dicts("SELECT id, name FROM t", client=fake_conn)
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


@pytest.mark.unit
def test_transaction_context_commits_on_success(monkeypatch):
    fake_conn = mock.MagicMock()
    from libs.postgres_client import transaction
    with transaction(fake_conn) as conn:
        assert conn is fake_conn
    fake_conn.commit.assert_called_once()
    fake_conn.rollback.assert_not_called()


@pytest.mark.unit
def test_transaction_context_rolls_back_on_error(monkeypatch):
    fake_conn = mock.MagicMock()
    from libs.postgres_client import transaction
    with pytest.raises(RuntimeError, match="boom"):
        with transaction(fake_conn):
            raise RuntimeError("boom")
    fake_conn.commit.assert_not_called()
    fake_conn.rollback.assert_called_once()
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest libs/tests/test_postgres_client.py -v
```

Expected: four new tests fail with `ImportError: cannot import name 'query_rows' / 'query_dicts' / 'transaction'`.

- [ ] **Step 3: Add helpers to libs/postgres_client.py**

Append to `libs/postgres_client.py`:

```python
from contextlib import contextmanager
from typing import Iterator


def query_rows(
    sql: str,
    params: Optional[Sequence[Any]] = None,
    client: Optional[Connection] = None,
) -> List[List[Any]]:
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
    params: Optional[Sequence[Any]] = None,
    client: Optional[Connection] = None,
) -> List[Dict[str, Any]]:
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
```

- [ ] **Step 4: Run tests — verify all pass**

```bash
pytest libs/tests/test_postgres_client.py -v
```

Expected: all six tests pass.

- [ ] **Step 5: Commit**

```bash
git add libs/postgres_client.py libs/tests/test_postgres_client.py
git commit -m "libs(postgres_client): add query_rows, query_dicts, transaction ctx"
```

---

## Task 6: Implement checkpoint_store — SCHEMA_VERSION + compute_state_hash

**Files:**
- Create: `libs/computation/checkpoint_store.py`
- Create: `libs/tests/test_checkpoint_store.py`

- [ ] **Step 1: Write failing tests for compute_state_hash**

Create `libs/tests/test_checkpoint_store.py`:

```python
"""Unit tests for libs.computation.checkpoint_store."""

from datetime import UTC, datetime

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState


def _make_state(records: dict[str, AnchorRecord]) -> AnchorState:
    state = AnchorState()
    for k, rec in records.items():
        state.set(k, rec)
    return state


def _rec(pnl=0.0, price=100.0, position=1.0, bar_ts=None, revision_ts=None) -> AnchorRecord:
    return AnchorRecord(
        pnl=pnl, price=price, position=position,
        bar_ts=bar_ts or datetime(2024, 1, 1, tzinfo=UTC),
        revision_ts=revision_ts or datetime(2024, 1, 1, tzinfo=UTC),
        strategy_id=1, strategy_name="s", underlying="BTC",
        config_timeframe="1m", weighting=1.0,
        strategy_instance_id="i", final_signal=0.0, benchmark=0.0,
    )


@pytest.mark.unit
def test_compute_state_hash_deterministic():
    from libs.computation.checkpoint_store import compute_state_hash
    state = _make_state({"a": _rec(pnl=1.5)})
    h1 = compute_state_hash(state)
    h2 = compute_state_hash(state)
    assert h1 == h2
    assert len(h1) == 64  # sha256 hex


@pytest.mark.unit
def test_compute_state_hash_insertion_order_independent():
    from libs.computation.checkpoint_store import compute_state_hash
    s1 = _make_state({"a": _rec(pnl=1.0), "b": _rec(pnl=2.0)})
    s2 = _make_state({"b": _rec(pnl=2.0), "a": _rec(pnl=1.0)})
    assert compute_state_hash(s1) == compute_state_hash(s2)


@pytest.mark.unit
def test_compute_state_hash_sensitive_to_pnl_change():
    from libs.computation.checkpoint_store import compute_state_hash
    s1 = _make_state({"a": _rec(pnl=1.5)})
    s2 = _make_state({"a": _rec(pnl=1.5000001)})
    assert compute_state_hash(s1) != compute_state_hash(s2)


@pytest.mark.unit
def test_compute_state_hash_insensitive_to_sub_picosecond_pnl_change():
    """Rounding to 12 decimal places means changes below that don't move the hash."""
    from libs.computation.checkpoint_store import compute_state_hash
    s1 = _make_state({"a": _rec(pnl=1.5)})
    s2 = _make_state({"a": _rec(pnl=1.5 + 1e-15)})
    assert compute_state_hash(s1) == compute_state_hash(s2)


@pytest.mark.unit
def test_compute_state_hash_sensitive_to_revision_ts():
    from libs.computation.checkpoint_store import compute_state_hash
    s1 = _make_state({"a": _rec(revision_ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC))})
    s2 = _make_state({"a": _rec(revision_ts=datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC))})
    assert compute_state_hash(s1) != compute_state_hash(s2)


@pytest.mark.unit
def test_schema_version_constant():
    from libs.computation import checkpoint_store
    assert isinstance(checkpoint_store.SCHEMA_VERSION, int)
    assert checkpoint_store.SCHEMA_VERSION >= 1
```

- [ ] **Step 2: Run tests — verify failure**

```bash
pytest libs/tests/test_checkpoint_store.py -v
```

Expected: all six fail with `ModuleNotFoundError`.

- [ ] **Step 3: Create checkpoint_store.py with SCHEMA_VERSION + hash**

Create `libs/computation/checkpoint_store.py`:

```python
"""Supabase Postgres checkpoint store for pnl_consumer AnchorState.

Reads and writes streaming.pnl_checkpoint and streaming.pnl_commit_state.

SCHEMA_VERSION bumps on incompatible AnchorRecord changes (field added/removed,
type change, semantics change). Cold-start sanity check rejects a checkpoint
whose stored schema_version != SCHEMA_VERSION; the consumer falls back to the
existing 48h bootstrap and writes a fresh checkpoint at the new version.
"""

import hashlib
import json
from typing import Any

from libs.computation.anchor_state import AnchorState

# Bump on any incompatible change to AnchorRecord fields or canonical encoding.
SCHEMA_VERSION = 1

# Float precision for hash + serialization. Below this is treated as noise.
_FLOAT_DECIMALS = 12


def _canonical_record(rec) -> dict[str, Any]:
    """One AnchorRecord → dict in canonical form (sorted keys, rounded floats)."""
    return {
        "pnl": round(rec.pnl, _FLOAT_DECIMALS),
        "price": round(rec.price, _FLOAT_DECIMALS),
        "position": round(rec.position, _FLOAT_DECIMALS),
        "bar_ts": rec.bar_ts.isoformat(),
        "revision_ts": rec.revision_ts.isoformat(),
        "strategy_id": int(rec.strategy_id),
        "strategy_name": rec.strategy_name,
        "underlying": rec.underlying,
        "config_timeframe": rec.config_timeframe,
        "weighting": round(rec.weighting, _FLOAT_DECIMALS),
        "strategy_instance_id": rec.strategy_instance_id,
        "final_signal": round(rec.final_signal, _FLOAT_DECIMALS),
        "benchmark": round(rec.benchmark, _FLOAT_DECIMALS),
    }


def compute_state_hash(state: AnchorState) -> str:
    """sha256 over the canonical, key-sorted encoding of all records in `state`.

    Hash is over state only — metadata fields (updated_at) are excluded.
    """
    canonical = [
        {"strategy_table_name": k, **_canonical_record(state.get(k))}
        for k in sorted(state.keys())
    ]
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
```

- [ ] **Step 4: Run tests — verify pass**

```bash
pytest libs/tests/test_checkpoint_store.py -v
```

Expected: all six tests pass.

- [ ] **Step 5: Commit**

```bash
git add libs/computation/checkpoint_store.py libs/tests/test_checkpoint_store.py
git commit -m "checkpoint_store: add SCHEMA_VERSION and compute_state_hash"
```

---

## Task 7: Implement checkpoint_store — write_checkpoint

**Files:**
- Modify: `libs/computation/checkpoint_store.py`
- Modify: `libs/tests/test_checkpoint_store.py`

- [ ] **Step 1: Append write_checkpoint tests (mock-based unit tests)**

Append to `libs/tests/test_checkpoint_store.py`:

```python
from unittest import mock


@pytest.mark.unit
def test_write_checkpoint_executes_two_upserts_in_transaction():
    from libs.computation.checkpoint_store import write_checkpoint
    state = _make_state({"strat_a": _rec(pnl=1.5, price=100.0, position=2.0)})

    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    write_checkpoint(
        mode="prod",
        anchor_state=state,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=42,
        last_candle_ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        client=fake_conn,
    )

    # Expect at least two executes: one for pnl_checkpoint UPSERT, one for pnl_commit_state UPSERT.
    assert fake_cur.execute.call_count >= 2
    fake_conn.commit.assert_called_once()


@pytest.mark.unit
def test_write_checkpoint_rolls_back_on_failure():
    from libs.computation.checkpoint_store import write_checkpoint
    state = _make_state({"strat_a": _rec()})

    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_cur.execute.side_effect = [None, RuntimeError("simulated postgres failure")]
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    with pytest.raises(RuntimeError, match="simulated"):
        write_checkpoint(
            mode="prod",
            anchor_state=state,
            kafka_topic="binance.price.ticks",
            kafka_partition=0,
            kafka_offset=42,
            last_candle_ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            client=fake_conn,
        )
    fake_conn.rollback.assert_called_once()
    fake_conn.commit.assert_not_called()


@pytest.mark.unit
def test_write_checkpoint_empty_state_writes_commit_state_only():
    """An empty AnchorState shouldn't crash — just no pnl_checkpoint rows."""
    from libs.computation.checkpoint_store import write_checkpoint
    state = AnchorState()

    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    write_checkpoint(
        mode="prod",
        anchor_state=state,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=42,
        last_candle_ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        client=fake_conn,
    )

    # Still writes pnl_commit_state, but the checkpoint rows insert is skipped.
    assert fake_cur.execute.call_count == 1
    fake_conn.commit.assert_called_once()
```

- [ ] **Step 2: Run tests — verify failure**

```bash
pytest libs/tests/test_checkpoint_store.py -v
```

Expected: three new tests fail with `ImportError: cannot import name 'write_checkpoint'`.

- [ ] **Step 3: Implement write_checkpoint**

Append to `libs/computation/checkpoint_store.py`:

```python
from datetime import datetime
from typing import Iterable

from psycopg import Connection
from psycopg import sql as pg_sql


_UPSERT_CHECKPOINT_SQL = """
INSERT INTO streaming.pnl_checkpoint (
    mode, strategy_table_name,
    pnl, price, position,
    bar_ts, revision_ts,
    strategy_id, strategy_name, underlying, config_timeframe,
    weighting, strategy_instance_id, final_signal, benchmark,
    updated_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (mode, strategy_table_name) DO UPDATE SET
    pnl = EXCLUDED.pnl,
    price = EXCLUDED.price,
    position = EXCLUDED.position,
    bar_ts = EXCLUDED.bar_ts,
    revision_ts = EXCLUDED.revision_ts,
    strategy_id = EXCLUDED.strategy_id,
    strategy_name = EXCLUDED.strategy_name,
    underlying = EXCLUDED.underlying,
    config_timeframe = EXCLUDED.config_timeframe,
    weighting = EXCLUDED.weighting,
    strategy_instance_id = EXCLUDED.strategy_instance_id,
    final_signal = EXCLUDED.final_signal,
    benchmark = EXCLUDED.benchmark,
    updated_at = NOW();
"""

_UPSERT_COMMIT_STATE_SQL = """
INSERT INTO streaming.pnl_commit_state (
    mode, last_candle_ts, kafka_topic, kafka_partition,
    kafka_offset, state_hash, schema_version, updated_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (mode) DO UPDATE SET
    last_candle_ts  = EXCLUDED.last_candle_ts,
    kafka_topic     = EXCLUDED.kafka_topic,
    kafka_partition = EXCLUDED.kafka_partition,
    kafka_offset    = EXCLUDED.kafka_offset,
    state_hash      = EXCLUDED.state_hash,
    schema_version  = EXCLUDED.schema_version,
    updated_at      = NOW();
"""


def _checkpoint_row_params(mode: str, strategy_table_name: str, rec) -> tuple:
    return (
        mode, strategy_table_name,
        rec.pnl, rec.price, rec.position,
        rec.bar_ts, rec.revision_ts,
        int(rec.strategy_id), rec.strategy_name, rec.underlying, rec.config_timeframe,
        rec.weighting, rec.strategy_instance_id, rec.final_signal, rec.benchmark,
    )


def write_checkpoint(
    *,
    mode: str,
    anchor_state: AnchorState,
    kafka_topic: str,
    kafka_partition: int,
    kafka_offset: int,
    last_candle_ts: datetime,
    client: Connection,
) -> None:
    """Atomic UPSERT of checkpoint + commit_state for one mode in a single transaction.

    On any database error, rolls back and re-raises. Caller wraps in try/except
    for best-effort behavior — checkpoint_store does not silently swallow errors.

    The full AnchorState is written every call (UPSERT semantics), so a single
    successful call after an outage fully restores the checkpoint.
    """
    state_hash = compute_state_hash(anchor_state)
    try:
        with client.cursor() as cur:
            keys = sorted(anchor_state.keys())
            if keys:
                params_list: list[tuple] = [
                    _checkpoint_row_params(mode, k, anchor_state.get(k))
                    for k in keys
                ]
                cur.executemany(_UPSERT_CHECKPOINT_SQL, params_list)
            cur.execute(
                _UPSERT_COMMIT_STATE_SQL,
                (mode, last_candle_ts, kafka_topic, kafka_partition,
                 kafka_offset, state_hash, SCHEMA_VERSION),
            )
        client.commit()
    except Exception:
        client.rollback()
        raise
```

Note on `cur.executemany` vs `cur.execute` count: when called with `executemany`, psycopg counts that as one `.execute` call on the cursor mock. The test asserts `>= 2` which holds (1 from executemany + 1 from explicit execute). For the empty-state case, no executemany is called; only one explicit execute happens.

- [ ] **Step 4: Run tests — verify pass**

```bash
pytest libs/tests/test_checkpoint_store.py -v
```

Expected: all nine tests pass.

- [ ] **Step 5: Commit**

```bash
git add libs/computation/checkpoint_store.py libs/tests/test_checkpoint_store.py
git commit -m "checkpoint_store: implement write_checkpoint (atomic UPSERT in tx)"
```

---

## Task 8: Implement checkpoint_store — read_checkpoint

**Files:**
- Modify: `libs/computation/checkpoint_store.py`
- Modify: `libs/tests/test_checkpoint_store.py`

- [ ] **Step 1: Append read_checkpoint tests**

Append to `libs/tests/test_checkpoint_store.py`:

```python
@pytest.mark.unit
def test_read_checkpoint_returns_none_when_no_commit_state():
    from libs.computation.checkpoint_store import read_checkpoint

    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_cur.description = [
        ("mode",), ("last_candle_ts",), ("kafka_topic",), ("kafka_partition",),
        ("kafka_offset",), ("state_hash",), ("schema_version",),
    ]
    fake_cur.fetchall.return_value = []
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    result = read_checkpoint(mode="prod", client=fake_conn)
    assert result is None


@pytest.mark.unit
def test_read_checkpoint_returns_load_result():
    from libs.computation.checkpoint_store import CheckpointLoadResult, read_checkpoint

    # Two separate cursor uses: one for pnl_commit_state, one for pnl_checkpoint.
    cs_cur = mock.MagicMock()
    cs_cur.__enter__ = mock.MagicMock(return_value=cs_cur)
    cs_cur.__exit__ = mock.MagicMock(return_value=False)
    cs_cur.description = [
        ("mode",), ("last_candle_ts",), ("kafka_topic",), ("kafka_partition",),
        ("kafka_offset",), ("state_hash",), ("schema_version",),
    ]
    cs_cur.fetchall.return_value = [(
        "prod",
        datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        "binance.price.ticks", 0, 100,
        "deadbeef", 1,
    )]

    cp_cur = mock.MagicMock()
    cp_cur.__enter__ = mock.MagicMock(return_value=cp_cur)
    cp_cur.__exit__ = mock.MagicMock(return_value=False)
    cp_cur.description = [
        ("strategy_table_name",), ("pnl",), ("price",), ("position",),
        ("bar_ts",), ("revision_ts",), ("strategy_id",), ("strategy_name",),
        ("underlying",), ("config_timeframe",), ("weighting",),
        ("strategy_instance_id",), ("final_signal",), ("benchmark",),
    ]
    cp_cur.fetchall.return_value = [(
        "strat_a", 1.5, 100.0, 2.0,
        datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC),
        1, "s", "BTC", "1m", 1.0, "i", 0.0, 0.0,
    )]

    fake_conn = mock.MagicMock()
    fake_conn.cursor.side_effect = [cs_cur, cp_cur]

    result = read_checkpoint(mode="prod", client=fake_conn)
    assert isinstance(result, CheckpointLoadResult)
    assert result.commit_state.kafka_offset == 100
    assert result.commit_state.state_hash == "deadbeef"
    assert result.commit_state.schema_version == 1
    assert "strat_a" in result.anchor_state.keys()
    assert result.anchor_state.get("strat_a").pnl == 1.5
```

- [ ] **Step 2: Run tests — verify failure**

```bash
pytest libs/tests/test_checkpoint_store.py -v
```

Expected: two new tests fail with `ImportError`.

- [ ] **Step 3: Implement read_checkpoint + CheckpointLoadResult**

Append to `libs/computation/checkpoint_store.py`:

```python
from dataclasses import dataclass

from libs.computation.anchor_state import AnchorRecord


@dataclass(frozen=True)
class CommitStateRow:
    mode: str
    last_candle_ts: datetime
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    state_hash: str
    schema_version: int


@dataclass(frozen=True)
class CheckpointLoadResult:
    commit_state: CommitStateRow
    anchor_state: AnchorState


_SELECT_COMMIT_STATE = """
SELECT mode, last_candle_ts, kafka_topic, kafka_partition,
       kafka_offset, state_hash, schema_version
FROM streaming.pnl_commit_state
WHERE mode = %s
"""

_SELECT_CHECKPOINT_ROWS = """
SELECT strategy_table_name, pnl, price, position,
       bar_ts, revision_ts,
       strategy_id, strategy_name, underlying, config_timeframe,
       weighting, strategy_instance_id, final_signal, benchmark
FROM streaming.pnl_checkpoint
WHERE mode = %s
"""


def read_checkpoint(*, mode: str, client: Connection) -> "CheckpointLoadResult | None":
    """Return the persisted checkpoint for `mode`, or None if no commit_state row exists.

    Reads both tables in the same connection but separate cursors. The two reads
    are not in a single transaction — the writer guarantees consistency via its
    own transaction; readers see whatever the last successful write produced.
    """
    with client.cursor() as cur:
        cur.execute(_SELECT_COMMIT_STATE, (mode,))
        commit_rows = cur.fetchall()
    if not commit_rows:
        return None
    row = commit_rows[0]
    commit_state = CommitStateRow(
        mode=row[0],
        last_candle_ts=row[1],
        kafka_topic=row[2],
        kafka_partition=row[3],
        kafka_offset=row[4],
        state_hash=row[5],
        schema_version=row[6],
    )

    with client.cursor() as cur:
        cur.execute(_SELECT_CHECKPOINT_ROWS, (mode,))
        checkpoint_rows = cur.fetchall()

    anchor_state = AnchorState()
    for r in checkpoint_rows:
        anchor_state.set(r[0], AnchorRecord(
            pnl=r[1], price=r[2], position=r[3],
            bar_ts=r[4], revision_ts=r[5],
            strategy_id=r[6], strategy_name=r[7], underlying=r[8],
            config_timeframe=r[9], weighting=r[10],
            strategy_instance_id=r[11], final_signal=r[12], benchmark=r[13],
        ))
    return CheckpointLoadResult(commit_state=commit_state, anchor_state=anchor_state)
```

- [ ] **Step 4: Run tests — verify pass**

```bash
pytest libs/tests/test_checkpoint_store.py -v
```

Expected: all eleven tests pass.

- [ ] **Step 5: Commit**

```bash
git add libs/computation/checkpoint_store.py libs/tests/test_checkpoint_store.py
git commit -m "checkpoint_store: implement read_checkpoint + CheckpointLoadResult"
```

---

## Task 9: Add serialization roundtrip property test

**Files:**
- Create: `services/pnl_consumer/tests/test_state_serialization_roundtrip.py`

This is a focused property test that the hash is stable across write→read using mock cursors that round-trip values.

- [ ] **Step 1: Write the roundtrip test**

Create `services/pnl_consumer/tests/test_state_serialization_roundtrip.py`:

```python
"""Property test: write_checkpoint → read_checkpoint preserves AnchorState semantics.

Avoids needing a live Postgres by simulating the round-trip via in-memory
storage in mock cursors. Validates that compute_state_hash before write equals
compute_state_hash after read.
"""

from datetime import UTC, datetime
from unittest import mock

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.checkpoint_store import (
    compute_state_hash,
    read_checkpoint,
    write_checkpoint,
)


class _InMemoryCheckpointStore:
    """Tiny fake Postgres-shaped store. Stores tuples; replays them on SELECT."""

    def __init__(self):
        self.pnl_checkpoint: dict[tuple[str, str], tuple] = {}
        self.pnl_commit_state: dict[str, tuple] = {}

    def make_conn(self):
        store = self
        conn = mock.MagicMock()

        def make_cursor():
            cur = mock.MagicMock()
            cur.__enter__ = mock.MagicMock(return_value=cur)
            cur.__exit__ = mock.MagicMock(return_value=False)

            def execute(sql, params=None):
                cur._last_sql = sql
                cur._last_params = params
                if "FROM streaming.pnl_commit_state" in sql:
                    mode = params[0]
                    cur.description = [(c,) for c in [
                        "mode", "last_candle_ts", "kafka_topic", "kafka_partition",
                        "kafka_offset", "state_hash", "schema_version",
                    ]]
                    cur._rows = [store.pnl_commit_state[mode]] if mode in store.pnl_commit_state else []
                elif "FROM streaming.pnl_checkpoint" in sql:
                    mode = params[0]
                    cur.description = [(c,) for c in [
                        "strategy_table_name", "pnl", "price", "position",
                        "bar_ts", "revision_ts",
                        "strategy_id", "strategy_name", "underlying", "config_timeframe",
                        "weighting", "strategy_instance_id", "final_signal", "benchmark",
                    ]]
                    cur._rows = [
                        # drop the first element (mode) and the updated_at (last col)
                        v[1:-1]
                        for k, v in store.pnl_checkpoint.items() if k[0] == mode
                    ]
                elif "INTO streaming.pnl_commit_state" in sql:
                    # params: (mode, last_candle_ts, kafka_topic, kafka_partition,
                    #          kafka_offset, state_hash, schema_version)
                    store.pnl_commit_state[params[0]] = params

            def executemany(sql, params_list):
                # INSERT INTO streaming.pnl_checkpoint
                for p in params_list:
                    # (mode, strategy_table_name, pnl, price, position, bar_ts, revision_ts,
                    #  strategy_id, strategy_name, underlying, config_timeframe,
                    #  weighting, strategy_instance_id, final_signal, benchmark)
                    # We store with an appended `updated_at` slot (None) for symmetry.
                    store.pnl_checkpoint[(p[0], p[1])] = p + (None,)

            def fetchall():
                return cur._rows

            cur.execute = execute
            cur.executemany = executemany
            cur.fetchall = fetchall
            return cur

        conn.cursor.side_effect = make_cursor
        return conn


@pytest.mark.unit
def test_write_then_read_preserves_hash():
    state = AnchorState()
    state.set("s_a", AnchorRecord(
        pnl=1.5, price=100.25, position=2.0,
        bar_ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        revision_ts=datetime(2024, 1, 1, 12, 0, 30, tzinfo=UTC),
        strategy_id=7, strategy_name="alpha", underlying="BTC",
        config_timeframe="1m", weighting=0.5,
        strategy_instance_id="alpha-btc-1m", final_signal=1.0, benchmark=0.0,
    ))
    state.set("s_b", AnchorRecord(
        pnl=-2.5, price=50.5, position=-1.0,
        bar_ts=datetime(2024, 1, 1, 11, 0, tzinfo=UTC),
        revision_ts=datetime(2024, 1, 1, 11, 0, 45, tzinfo=UTC),
        strategy_id=9, strategy_name="beta", underlying="ETH",
        config_timeframe="5m", weighting=1.0,
        strategy_instance_id="beta-eth-5m", final_signal=-1.0, benchmark=0.0,
    ))
    hash_before = compute_state_hash(state)

    fake = _InMemoryCheckpointStore()
    conn = fake.make_conn()

    write_checkpoint(
        mode="prod", anchor_state=state,
        kafka_topic="binance.price.ticks", kafka_partition=0, kafka_offset=99,
        last_candle_ts=datetime(2024, 1, 1, 12, 1, tzinfo=UTC),
        client=conn,
    )

    loaded = read_checkpoint(mode="prod", client=conn)
    assert loaded is not None
    assert loaded.commit_state.kafka_offset == 99
    hash_after = compute_state_hash(loaded.anchor_state)
    assert hash_before == hash_after
```

- [ ] **Step 2: Run test — verify pass**

```bash
pytest services/pnl_consumer/tests/test_state_serialization_roundtrip.py -v
```

Expected: test passes (the implementations already exist from Tasks 7–8).

- [ ] **Step 3: Commit**

```bash
git add services/pnl_consumer/tests/test_state_serialization_roundtrip.py
git commit -m "test: write→read checkpoint roundtrip preserves state hash"
```

---

## Task 10: Implement cold-start sanity checks

**Files:**
- Create: `services/pnl_consumer/pnl_consumer/cold_start.py`
- Create: `services/pnl_consumer/tests/test_cold_start_sanity_checks.py`

- [ ] **Step 1: Write failing tests for each sanity check**

Create `services/pnl_consumer/tests/test_cold_start_sanity_checks.py`:

```python
"""Unit tests for the cold-start sanity check functions.

Each check is tested in isolation with fixture data. The orchestration of all
checks together is covered in test_cold_start_orchestration.py.
"""

from datetime import UTC, datetime, timedelta

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.checkpoint_store import (
    SCHEMA_VERSION,
    CheckpointLoadResult,
    CommitStateRow,
    compute_state_hash,
)


_NOW = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)


def _make_state_with_one() -> AnchorState:
    s = AnchorState()
    s.set("strat_a", AnchorRecord(
        pnl=1.0, price=100.0, position=1.0,
        bar_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
        revision_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
        strategy_id=1, strategy_name="s", underlying="BTC",
        config_timeframe="1m", weighting=1.0,
        strategy_instance_id="i", final_signal=0.0, benchmark=0.0,
    ))
    return s


def _make_result(
    *, schema_version=SCHEMA_VERSION, last_candle_ts=_NOW - timedelta(minutes=1),
    kafka_offset=100, state_hash_override=None,
) -> CheckpointLoadResult:
    state = _make_state_with_one()
    h = state_hash_override or compute_state_hash(state)
    cs = CommitStateRow(
        mode="prod", last_candle_ts=last_candle_ts,
        kafka_topic="binance.price.ticks", kafka_partition=0,
        kafka_offset=kafka_offset, state_hash=h, schema_version=schema_version,
    )
    return CheckpointLoadResult(commit_state=cs, anchor_state=state)


@pytest.mark.unit
def test_check_schema_version_passes_when_match():
    from pnl_consumer.cold_start import check_schema_version
    assert check_schema_version(_make_result()) is None


@pytest.mark.unit
def test_check_schema_version_fails_when_mismatch():
    from pnl_consumer.cold_start import FallbackReason, check_schema_version
    bad = _make_result(schema_version=SCHEMA_VERSION + 1)
    reason = check_schema_version(bad)
    assert reason == FallbackReason.SCHEMA_VERSION_MISMATCH


@pytest.mark.unit
def test_check_freshness_passes_within_window():
    from pnl_consumer.cold_start import check_freshness
    result = _make_result(last_candle_ts=_NOW - timedelta(hours=1))
    assert check_freshness(result, now=_NOW, max_age_seconds=86400) is None


@pytest.mark.unit
def test_check_freshness_fails_outside_window():
    from pnl_consumer.cold_start import FallbackReason, check_freshness
    result = _make_result(last_candle_ts=_NOW - timedelta(hours=25))
    reason = check_freshness(result, now=_NOW, max_age_seconds=86400)
    assert reason == FallbackReason.STALE


@pytest.mark.unit
def test_check_offset_match_passes_when_supabase_is_one_behind_committed():
    """Kafka stores offset+1 as next-to-read; checkpoint stores offset of last processed.
    So supabase.kafka_offset == kafka_committed - 1 is the match condition."""
    from pnl_consumer.cold_start import check_offset_match
    result = _make_result(kafka_offset=100)
    assert check_offset_match(result, kafka_committed_offset=101) is None


@pytest.mark.unit
def test_check_offset_match_fails_when_supabase_lags():
    from pnl_consumer.cold_start import FallbackReason, check_offset_match
    result = _make_result(kafka_offset=100)
    reason = check_offset_match(result, kafka_committed_offset=105)
    assert reason == FallbackReason.OFFSET_LAG


@pytest.mark.unit
def test_check_offset_match_fails_when_supabase_ahead():
    from pnl_consumer.cold_start import FallbackReason, check_offset_match
    result = _make_result(kafka_offset=100)
    reason = check_offset_match(result, kafka_committed_offset=99)
    assert reason == FallbackReason.OFFSET_LAG


@pytest.mark.unit
def test_check_hash_passes_on_clean_state():
    from pnl_consumer.cold_start import check_hash
    assert check_hash(_make_result()) is None


@pytest.mark.unit
def test_check_hash_fails_on_tampered_state():
    from pnl_consumer.cold_start import FallbackReason, check_hash
    result = _make_result(state_hash_override="0" * 64)
    reason = check_hash(result)
    assert reason == FallbackReason.HASH_MISMATCH


@pytest.mark.unit
def test_check_non_empty_strategy_set_fails_when_empty():
    from pnl_consumer.cold_start import FallbackReason, check_non_empty
    empty = CheckpointLoadResult(
        commit_state=_make_result().commit_state,
        anchor_state=AnchorState(),
    )
    reason = check_non_empty(empty)
    assert reason == FallbackReason.NO_STRATEGIES


@pytest.mark.unit
def test_real_trade_check_passes_with_real_revision_guard():
    from pnl_consumer.cold_start import check_real_trade_revision_guards
    result = _make_result()
    assert check_real_trade_revision_guards(result) is None


@pytest.mark.unit
def test_real_trade_check_fails_when_bar_ts_is_min():
    from pnl_consumer.cold_start import (
        FallbackReason,
        check_real_trade_revision_guards,
    )
    state = AnchorState()
    state.set("strat_a", AnchorRecord())  # all defaults — bar_ts == datetime.min
    cs = CommitStateRow(
        mode="real_trade", last_candle_ts=_NOW - timedelta(minutes=1),
        kafka_topic="binance.price.ticks", kafka_partition=0, kafka_offset=100,
        state_hash=compute_state_hash(state), schema_version=SCHEMA_VERSION,
    )
    result = CheckpointLoadResult(commit_state=cs, anchor_state=state)
    reason = check_real_trade_revision_guards(result)
    assert reason == FallbackReason.REAL_TRADE_GUARD_UNINITIALIZED
```

- [ ] **Step 2: Run tests — verify failure**

```bash
pytest services/pnl_consumer/tests/test_cold_start_sanity_checks.py -v
```

Expected: all fail with `ModuleNotFoundError: No module named 'pnl_consumer.cold_start'`.

- [ ] **Step 3: Implement cold_start.py sanity checks**

Create `services/pnl_consumer/pnl_consumer/cold_start.py`:

```python
"""Cold-start orchestration for pnl_consumer.

On startup, attempts to restore AnchorState from streaming.pnl_checkpoint.
Runs cheap sanity checks; on any failure, falls back to the existing
_bootstrap_state / _bootstrap_bt_state code in pnl_consumer.py.

This module is the only place that ties together Supabase reads, Kafka offset
queries, ClickHouse invariant checks, and bootstrap fallback. Everything else
(checkpoint_store, postgres_client) is pure I/O / serialization.
"""

from datetime import UTC, datetime
from enum import Enum

from libs.computation.anchor_state import AnchorState
from libs.computation.checkpoint_store import (
    SCHEMA_VERSION,
    CheckpointLoadResult,
    compute_state_hash,
)

_DATETIME_MIN_UTC = datetime.min.replace(tzinfo=UTC)


class FallbackReason(Enum):
    """Why cold-start fell back to bootstrap. Logged on every fallback."""

    NO_CHECKPOINT = "no_checkpoint"
    SCHEMA_VERSION_MISMATCH = "schema_version_mismatch"
    STALE = "stale"
    OFFSET_LAG = "offset_lag"
    HASH_MISMATCH = "hash_mismatch"
    NO_STRATEGIES = "no_strategies"
    REAL_TRADE_GUARD_UNINITIALIZED = "real_trade_guard_uninitialized"
    INVARIANT_MISMATCH = "invariant_mismatch"
    READ_DISABLED = "read_disabled"
    CHECKPOINT_READ_ERROR = "checkpoint_read_error"


def check_schema_version(result: CheckpointLoadResult) -> FallbackReason | None:
    if result.commit_state.schema_version != SCHEMA_VERSION:
        return FallbackReason.SCHEMA_VERSION_MISMATCH
    return None


def check_freshness(
    result: CheckpointLoadResult,
    *,
    now: datetime,
    max_age_seconds: int,
) -> FallbackReason | None:
    age = (now - result.commit_state.last_candle_ts).total_seconds()
    if age > max_age_seconds:
        return FallbackReason.STALE
    return None


def check_offset_match(
    result: CheckpointLoadResult,
    *,
    kafka_committed_offset: int,
) -> FallbackReason | None:
    """checkpoint.kafka_offset is the offset of the *last processed* message.
    Kafka's committed offset is the *next-to-read*, i.e. last_processed + 1.
    They match iff checkpoint.kafka_offset == kafka_committed_offset - 1.
    """
    if result.commit_state.kafka_offset != kafka_committed_offset - 1:
        return FallbackReason.OFFSET_LAG
    return None


def check_hash(result: CheckpointLoadResult) -> FallbackReason | None:
    recomputed = compute_state_hash(result.anchor_state)
    if recomputed != result.commit_state.state_hash:
        return FallbackReason.HASH_MISMATCH
    return None


def check_non_empty(result: CheckpointLoadResult) -> FallbackReason | None:
    if len(result.anchor_state) == 0:
        return FallbackReason.NO_STRATEGIES
    return None


def check_real_trade_revision_guards(
    result: CheckpointLoadResult,
) -> FallbackReason | None:
    """For real-trade mode: every record must have populated bar_ts and revision_ts.

    `_DATETIME_MIN_UTC` means the revision guard was never initialized; restoring
    from such a checkpoint would let any revision through, defeating the guard.
    """
    for key in result.anchor_state.keys():
        rec = result.anchor_state.get(key)
        if rec.bar_ts <= _DATETIME_MIN_UTC or rec.revision_ts <= _DATETIME_MIN_UTC:
            return FallbackReason.REAL_TRADE_GUARD_UNINITIALIZED
    return None
```

- [ ] **Step 4: Run tests — verify pass**

```bash
pytest services/pnl_consumer/tests/test_cold_start_sanity_checks.py -v
```

Expected: all twelve tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/pnl_consumer/pnl_consumer/cold_start.py services/pnl_consumer/tests/test_cold_start_sanity_checks.py
git commit -m "cold_start: implement sanity check functions + FallbackReason enum"
```

---

## Task 11: Implement cold-start orchestration (load_or_bootstrap)

**Files:**
- Modify: `services/pnl_consumer/pnl_consumer/cold_start.py`
- Create: `services/pnl_consumer/tests/test_cold_start_orchestration.py`

- [ ] **Step 1: Write tests for orchestration**

Create `services/pnl_consumer/tests/test_cold_start_orchestration.py`:

```python
"""Integration-style unit tests for load_or_bootstrap orchestration.

Mocks out checkpoint_store + bootstrap entry points; verifies the decision tree.
"""

from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest

from libs.computation.anchor_state import AnchorState
from libs.computation.checkpoint_store import (
    SCHEMA_VERSION,
    CheckpointLoadResult,
    CommitStateRow,
    compute_state_hash,
)


_NOW = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)


@pytest.mark.unit
def test_load_or_bootstrap_no_checkpoint_falls_back():
    """When read_checkpoint returns None, falls back to bootstrap."""
    from pnl_consumer.cold_start import load_or_bootstrap

    fake_bootstrap = mock.MagicMock(return_value=AnchorState())
    fake_read = mock.MagicMock(return_value=None)

    state, reason = load_or_bootstrap(
        mode="prod",
        pg_client=mock.MagicMock(),
        kafka_committed_offset=101,
        now=_NOW,
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=fake_read,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    fake_bootstrap.assert_called_once()
    assert reason.value == "no_checkpoint"


@pytest.mark.unit
def test_load_or_bootstrap_happy_path_returns_checkpoint_state():
    from pnl_consumer.cold_start import load_or_bootstrap

    state = AnchorState()
    from libs.computation.anchor_state import AnchorRecord
    state.set("a", AnchorRecord(
        pnl=1.0, price=100.0, position=1.0,
        bar_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
        revision_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
        strategy_id=1, strategy_name="s", underlying="BTC",
        config_timeframe="1m", weighting=1.0,
        strategy_instance_id="i", final_signal=0.0, benchmark=0.0,
    ))
    cs = CommitStateRow(
        mode="prod",
        last_candle_ts=_NOW - timedelta(minutes=1),
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=100,
        state_hash=compute_state_hash(state),
        schema_version=SCHEMA_VERSION,
    )
    result = CheckpointLoadResult(commit_state=cs, anchor_state=state)

    fake_bootstrap = mock.MagicMock()
    fake_read = mock.MagicMock(return_value=result)

    loaded_state, reason = load_or_bootstrap(
        mode="prod",
        pg_client=mock.MagicMock(),
        kafka_committed_offset=101,
        now=_NOW,
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=fake_read,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    fake_bootstrap.assert_not_called()
    assert reason is None
    assert loaded_state is state


@pytest.mark.unit
def test_load_or_bootstrap_stale_falls_back():
    from pnl_consumer.cold_start import load_or_bootstrap

    from libs.computation.anchor_state import AnchorRecord
    state = AnchorState()
    state.set("a", AnchorRecord(
        pnl=1.0, price=100.0, position=1.0,
        bar_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
        revision_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
        strategy_id=1, strategy_name="s", underlying="BTC",
        config_timeframe="1m", weighting=1.0,
        strategy_instance_id="i", final_signal=0.0, benchmark=0.0,
    ))
    cs = CommitStateRow(
        mode="prod",
        last_candle_ts=_NOW - timedelta(hours=25),  # stale
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=100,
        state_hash=compute_state_hash(state),
        schema_version=SCHEMA_VERSION,
    )
    result = CheckpointLoadResult(commit_state=cs, anchor_state=state)

    fake_bootstrap = mock.MagicMock(return_value=AnchorState())
    fake_read = mock.MagicMock(return_value=result)

    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=mock.MagicMock(),
        kafka_committed_offset=101,
        now=_NOW,
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=fake_read,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    fake_bootstrap.assert_called_once()
    assert reason.value == "stale"


@pytest.mark.unit
def test_load_or_bootstrap_invariant_failure_falls_back():
    """If invariant_check_fn returns INVARIANT_MISMATCH, fall back."""
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap

    from libs.computation.anchor_state import AnchorRecord
    state = AnchorState()
    state.set("a", AnchorRecord(
        pnl=1.0, price=100.0, position=1.0,
        bar_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
        revision_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
        strategy_id=1, strategy_name="s", underlying="BTC",
        config_timeframe="1m", weighting=1.0,
        strategy_instance_id="i", final_signal=0.0, benchmark=0.0,
    ))
    cs = CommitStateRow(
        mode="prod",
        last_candle_ts=_NOW - timedelta(minutes=1),
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=100,
        state_hash=compute_state_hash(state),
        schema_version=SCHEMA_VERSION,
    )
    result = CheckpointLoadResult(commit_state=cs, anchor_state=state)

    fake_bootstrap = mock.MagicMock(return_value=AnchorState())
    fake_read = mock.MagicMock(return_value=result)
    fake_invariant = mock.MagicMock(return_value=FallbackReason.INVARIANT_MISMATCH)

    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=mock.MagicMock(),
        kafka_committed_offset=101,
        now=_NOW,
        max_age_seconds=86400,
        invariant_check_enabled=True,
        read_checkpoint_fn=fake_read,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=fake_invariant,
    )
    fake_invariant.assert_called_once()
    fake_bootstrap.assert_called_once()
    assert reason == FallbackReason.INVARIANT_MISMATCH


@pytest.mark.unit
def test_load_or_bootstrap_read_exception_falls_back():
    """If read_checkpoint raises (e.g., Postgres down), fall back."""
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap

    fake_bootstrap = mock.MagicMock(return_value=AnchorState())
    fake_read = mock.MagicMock(side_effect=RuntimeError("postgres down"))

    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=mock.MagicMock(),
        kafka_committed_offset=101,
        now=_NOW,
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=fake_read,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason == FallbackReason.CHECKPOINT_READ_ERROR
    fake_bootstrap.assert_called_once()
```

- [ ] **Step 2: Run tests — verify failure**

```bash
pytest services/pnl_consumer/tests/test_cold_start_orchestration.py -v
```

Expected: fail with `ImportError: cannot import name 'load_or_bootstrap'`.

- [ ] **Step 3: Implement load_or_bootstrap in cold_start.py**

Append to `services/pnl_consumer/pnl_consumer/cold_start.py`:

```python
import logging
from typing import Callable

from libs.computation import checkpoint_store as _checkpoint_store_module

logger = logging.getLogger(__name__)


def load_or_bootstrap(
    *,
    mode: str,
    pg_client,
    kafka_committed_offset: int,
    now: datetime,
    max_age_seconds: int,
    invariant_check_enabled: bool,
    read_checkpoint_fn: Callable | None = None,
    bootstrap_fn: Callable[[], AnchorState] | None = None,
    invariant_check_fn: Callable | None = None,
) -> tuple[AnchorState, FallbackReason | None]:
    """Try to restore AnchorState from Supabase; fall back to bootstrap on any failure.

    Returns (state, reason). reason is None on success, the FallbackReason enum
    value otherwise. Caller logs the reason and proceeds either way.

    Function injection points (read_checkpoint_fn, bootstrap_fn, invariant_check_fn)
    exist for testability. In production, defaults are wired in pnl_consumer.run().
    """
    if read_checkpoint_fn is None or bootstrap_fn is None:
        raise ValueError(
            "load_or_bootstrap requires read_checkpoint_fn and bootstrap_fn "
            "(wired in pnl_consumer.run); they are injected for testing."
        )

    # 1. Read checkpoint.
    try:
        result = read_checkpoint_fn(mode=mode, client=pg_client)
    except Exception as exc:
        logger.warning(
            "cold-start: checkpoint read raised %s; falling back to bootstrap. mode=%s",
            type(exc).__name__, mode,
        )
        return bootstrap_fn(), FallbackReason.CHECKPOINT_READ_ERROR

    if result is None:
        logger.info("cold-start: no checkpoint row; falling back. mode=%s", mode)
        return bootstrap_fn(), FallbackReason.NO_CHECKPOINT

    # 2. Sanity checks (cheap, no ClickHouse).
    checks = [
        check_schema_version(result),
        check_non_empty(result),
        check_freshness(result, now=now, max_age_seconds=max_age_seconds),
        check_offset_match(result, kafka_committed_offset=kafka_committed_offset),
        check_hash(result),
    ]
    if mode == "real_trade":
        checks.append(check_real_trade_revision_guards(result))

    for reason in checks:
        if reason is not None:
            logger.warning(
                "cold-start: sanity check failed, falling back. mode=%s reason=%s",
                mode, reason.value,
            )
            return bootstrap_fn(), reason

    # 3. Optional ClickHouse invariant cross-check.
    if invariant_check_enabled and invariant_check_fn is not None:
        inv_reason = invariant_check_fn(mode=mode, result=result)
        if inv_reason is not None:
            logger.warning(
                "cold-start: invariant check failed, falling back. mode=%s reason=%s",
                mode, inv_reason.value,
            )
            return bootstrap_fn(), inv_reason

    logger.info(
        "cold-start: checkpoint restored. mode=%s strategies=%d offset=%d last_candle_ts=%s",
        mode, len(result.anchor_state),
        result.commit_state.kafka_offset, result.commit_state.last_candle_ts,
    )
    return result.anchor_state, None
```

- [ ] **Step 4: Run tests — verify pass**

```bash
pytest services/pnl_consumer/tests/test_cold_start_orchestration.py -v
```

Expected: all five tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/pnl_consumer/pnl_consumer/cold_start.py services/pnl_consumer/tests/test_cold_start_orchestration.py
git commit -m "cold_start: implement load_or_bootstrap orchestration"
```

---

## Task 12: Implement ClickHouse invariant cross-check

**Files:**
- Modify: `services/pnl_consumer/pnl_consumer/cold_start.py`

This check queries `strategy_output_history_*` for the latest position per strategy and compares to checkpoint.position. The integration test for this lives in `test_cold_start_invariant_integration.py` (Task 13) since it touches ClickHouse.

- [ ] **Step 1: Implement clickhouse_invariant_check**

Append to `services/pnl_consumer/pnl_consumer/cold_start.py`:

```python
_POSITION_TOLERANCE = 1e-9


def clickhouse_invariant_check(
    *,
    mode: str,
    result: CheckpointLoadResult,
    ch_client,
) -> FallbackReason | None:
    """Verify checkpoint.position agrees with the latest position in strategy_output_history_*.

    Single ClickHouse query per mode. Compares per-strategy_instance_id.

    Prod/bt: latest revision in [strategy_output_history_v2 | _bt_v2] as of
    result.commit_state.last_candle_ts.

    Real-trade: the *specific* (ts, revision_ts) pair stored on each checkpoint
    record. If that exact revision is missing, fall back.

    Returns None on agreement, FallbackReason.INVARIANT_MISMATCH otherwise.
    """
    history_table = {
        "prod": "analytics.strategy_output_history_v2",
        "bt": "analytics.strategy_output_history_bt_v2",
        "real_trade": "analytics.strategy_output_history_v2",
    }[mode]

    expected_by_iid: dict[str, float] = {}
    for key in result.anchor_state.keys():
        rec = result.anchor_state.get(key)
        if rec.strategy_instance_id:
            expected_by_iid[rec.strategy_instance_id] = rec.position

    if not expected_by_iid:
        # No instance_ids to check — caller should have caught NO_STRATEGIES already.
        return None

    if mode == "real_trade":
        # Verify the exact (ts, revision_ts) revisions referenced by checkpoint still exist
        # in history with matching position. Returns one row per checkpoint record.
        # Build a VALUES list of (strategy_instance_id, ts, revision_ts) tuples.
        tuples = []
        for key in result.anchor_state.keys():
            rec = result.anchor_state.get(key)
            if not rec.strategy_instance_id:
                continue
            tuples.append((rec.strategy_instance_id,
                           rec.bar_ts.isoformat(),
                           rec.revision_ts.isoformat()))
        if not tuples:
            return None
        # Use a VALUES join. JSON_EXTRACT_FLOAT for position field name in row_json.
        values_clause = ", ".join(
            f"('{iid}', '{ts}', '{rts}')" for iid, ts, rts in tuples
        )
        sql = f"""
            SELECT
                v.iid AS strategy_instance_id,
                JSONExtractFloat(h.row_json, 'position') AS position
            FROM (
                SELECT iid, ts, revision_ts FROM (
                    VALUES (
                        'iid String, ts DateTime, revision_ts DateTime',
                        {values_clause}
                    )
                )
            ) v
            INNER JOIN {history_table} h
                ON v.iid = h.strategy_instance_id
               AND v.ts = h.ts
               AND v.revision_ts = h.revision_ts
        """
    else:
        # Latest revision per strategy_instance_id, asof last_candle_ts.
        as_of = result.commit_state.last_candle_ts.isoformat()
        iid_list = ",".join(f"'{i}'" for i in expected_by_iid.keys())
        sql = f"""
            SELECT
                strategy_instance_id,
                JSONExtractFloat(argMin(row_json, revision_ts), 'position') AS position
            FROM {history_table}
            WHERE strategy_instance_id IN ({iid_list})
              AND ts <= toDateTime('{as_of}')
            GROUP BY strategy_instance_id
        """

    from libs.clickhouse_client import query_rows
    rows = query_rows(sql, client=ch_client)
    actual_by_iid = {r[0]: float(r[1]) for r in rows}

    for iid, expected_position in expected_by_iid.items():
        actual = actual_by_iid.get(iid)
        if actual is None or abs(actual - expected_position) > _POSITION_TOLERANCE:
            logger.warning(
                "invariant mismatch: iid=%s expected_position=%s actual_position=%s",
                iid, expected_position, actual,
            )
            return FallbackReason.INVARIANT_MISMATCH
    return None
```

- [ ] **Step 2: Validate file imports cleanly**

```bash
python -c "from pnl_consumer.cold_start import clickhouse_invariant_check; print('ok')"
```

Expected: prints `ok` (the function is importable; integration behavior tested in Task 13).

- [ ] **Step 3: Commit**

```bash
git add services/pnl_consumer/pnl_consumer/cold_start.py
git commit -m "cold_start: implement clickhouse_invariant_check (configurable)"
```

---

## Task 13: Integration test — ClickHouse invariant check

**Files:**
- Create: `services/pnl_consumer/tests/test_cold_start_invariant_integration.py`

Requires live ClickHouse. Marker: `integration`.

- [ ] **Step 1: Write the integration test**

Create `services/pnl_consumer/tests/test_cold_start_invariant_integration.py`:

```python
"""Integration test for cold_start.clickhouse_invariant_check.

Requires CLICKHOUSE_* env vars pointing to a live ClickHouse with
analytics.strategy_output_history_v2 populated.

The test seeds a synthetic strategy_instance_id (uses a unique uuid suffix)
so it doesn't interfere with production data, then queries through the
invariant check function.
"""

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from libs.clickhouse_client import execute as ch_execute
from libs.clickhouse_client import get_client as ch_get_client
from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.checkpoint_store import (
    SCHEMA_VERSION,
    CheckpointLoadResult,
    CommitStateRow,
    compute_state_hash,
)


pytestmark = pytest.mark.integration


@pytest.fixture
def ch_client():
    if not os.getenv("CLICKHOUSE_HOST"):
        pytest.skip("CLICKHOUSE_HOST not set — skipping integration test")
    client = ch_get_client()
    yield client
    client.close()


def _make_result(strategy_instance_id: str, position: float, as_of: datetime) -> CheckpointLoadResult:
    state = AnchorState()
    state.set("strat_a", AnchorRecord(
        pnl=1.0, price=100.0, position=position,
        bar_ts=as_of, revision_ts=as_of,
        strategy_id=1, strategy_name="invariant-test", underlying="BTC",
        config_timeframe="1m", weighting=1.0,
        strategy_instance_id=strategy_instance_id,
        final_signal=0.0, benchmark=0.0,
    ))
    cs = CommitStateRow(
        mode="prod", last_candle_ts=as_of,
        kafka_topic="binance.price.ticks", kafka_partition=0,
        kafka_offset=100, state_hash=compute_state_hash(state),
        schema_version=SCHEMA_VERSION,
    )
    return CheckpointLoadResult(commit_state=cs, anchor_state=state)


def test_invariant_passes_when_position_matches(ch_client):
    from pnl_consumer.cold_start import clickhouse_invariant_check

    iid = f"invariant-test-{uuid.uuid4().hex[:8]}"
    as_of = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=10)
    rev_ts = as_of

    # Seed a single row in strategy_output_history_v2.
    row_json = '{"position": 2.5, "cumulative_pnl": 0, "price": 100}'
    ch_execute(
        f"""
        INSERT INTO analytics.strategy_output_history_v2
            (strategy_id, strategy_name, strategy_instance_id, underlying,
             config_timeframe, weighting, ts, revision_ts, row_json,
             source_label, final_signal, benchmark)
        VALUES (1, 'invariant-test', '{iid}', 'BTC', '1m', 1.0,
                toDateTime('{as_of.isoformat()}'),
                toDateTime('{rev_ts.isoformat()}'),
                '{row_json}', 'production', 0, 0)
        """,
        client=ch_client,
    )

    result = _make_result(iid, position=2.5, as_of=as_of)
    reason = clickhouse_invariant_check(mode="prod", result=result, ch_client=ch_client)
    assert reason is None


def test_invariant_fails_when_position_diverges(ch_client):
    from pnl_consumer.cold_start import FallbackReason, clickhouse_invariant_check

    iid = f"invariant-test-{uuid.uuid4().hex[:8]}"
    as_of = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=10)
    rev_ts = as_of

    row_json = '{"position": 2.5, "cumulative_pnl": 0, "price": 100}'
    ch_execute(
        f"""
        INSERT INTO analytics.strategy_output_history_v2
            (strategy_id, strategy_name, strategy_instance_id, underlying,
             config_timeframe, weighting, ts, revision_ts, row_json,
             source_label, final_signal, benchmark)
        VALUES (1, 'invariant-test', '{iid}', 'BTC', '1m', 1.0,
                toDateTime('{as_of.isoformat()}'),
                toDateTime('{rev_ts.isoformat()}'),
                '{row_json}', 'production', 0, 0)
        """,
        client=ch_client,
    )

    # Checkpoint claims position=3.0 but history has 2.5.
    result = _make_result(iid, position=3.0, as_of=as_of)
    reason = clickhouse_invariant_check(mode="prod", result=result, ch_client=ch_client)
    assert reason == FallbackReason.INVARIANT_MISMATCH
```

- [ ] **Step 2: Run the integration test (requires CLICKHOUSE_HOST)**

```bash
pytest services/pnl_consumer/tests/test_cold_start_invariant_integration.py -v -m integration
```

Expected: two tests pass against a live ClickHouse. If `CLICKHOUSE_HOST` is unset, both tests skip with a clear reason.

- [ ] **Step 3: Commit**

```bash
git add services/pnl_consumer/tests/test_cold_start_invariant_integration.py
git commit -m "test(integration): clickhouse_invariant_check pass/fail cases"
```

---

## Task 14: Real-trade revision restore tests

**Files:**
- Create: `services/pnl_consumer/tests/test_real_trade_revision_restore.py`

The seven explicit cases from the spec's Real-trade section. These exercise the AnchorState.should_apply_revision logic after a restore (no actual restore round-trip — just verify the tuple-comparison invariant holds with restored timestamp values).

- [ ] **Step 1: Write the seven explicit cases**

Create `services/pnl_consumer/tests/test_real_trade_revision_restore.py`:

```python
"""Real-trade revision-guard correctness after restore.

After restoring an AnchorState from checkpoint, the revision guard
(bar_ts, revision_ts) tuple comparison must continue to work exactly as
in-memory. These tests construct an AnchorState as if just restored and feed
new revisions into should_apply_revision, asserting accept/reject decisions.
"""

from datetime import UTC, datetime

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState

pytestmark = pytest.mark.unit


_BAR_10 = datetime(2024, 6, 1, 10, 0, tzinfo=UTC)
_BAR_11 = datetime(2024, 6, 1, 11, 0, tzinfo=UTC)
_BAR_9 = datetime(2024, 6, 1, 9, 0, tzinfo=UTC)

_REV_T0 = datetime(2024, 6, 1, 10, 0, 0, tzinfo=UTC)
_REV_T1 = datetime(2024, 6, 1, 10, 0, 30, tzinfo=UTC)
_REV_TM1 = datetime(2024, 6, 1, 9, 59, 30, tzinfo=UTC)


def _restored_state_at(bar_ts, revision_ts) -> AnchorState:
    state = AnchorState()
    state.set("strat_a", AnchorRecord(
        pnl=1.0, price=100.0, position=1.0,
        bar_ts=bar_ts, revision_ts=revision_ts,
        strategy_id=1, strategy_name="s", underlying="BTC",
        config_timeframe="1m", weighting=1.0,
        strategy_instance_id="i", final_signal=0.0, benchmark=0.0,
    ))
    return state


def test_case_1_older_revision_same_bar_rejected():
    state = _restored_state_at(_BAR_10, _REV_T0)
    # Old revision_ts (T-1) for the same bar
    assert state.should_apply_revision("strat_a", _BAR_10, _REV_TM1) is False


def test_case_2_same_revision_rejected():
    state = _restored_state_at(_BAR_10, _REV_T0)
    assert state.should_apply_revision("strat_a", _BAR_10, _REV_T0) is False


def test_case_3_newer_revision_same_bar_accepted():
    state = _restored_state_at(_BAR_10, _REV_T0)
    assert state.should_apply_revision("strat_a", _BAR_10, _REV_T1) is True


def test_case_4_newer_bar_older_revision_ts_accepted():
    """Tuple comparison: (bar=11, rev=09:59:30) > (bar=10, rev=10:00:00). Accept."""
    state = _restored_state_at(_BAR_10, _REV_T0)
    assert state.should_apply_revision("strat_a", _BAR_11, _REV_TM1) is True


def test_case_5_older_bar_newer_revision_ts_rejected():
    """(bar=9, rev=10:00:30) < (bar=10, rev=10:00:00). Reject."""
    state = _restored_state_at(_BAR_10, _REV_T0)
    assert state.should_apply_revision("strat_a", _BAR_9, _REV_T1) is False


def test_case_6_uninitialized_guard_accepts_first_revision():
    """Sanity-check would reject a real-trade checkpoint with min sentinels,
    but in-memory the tuple comparison correctly accepts the first revision."""
    state = AnchorState()
    # AnchorRecord defaults: bar_ts = revision_ts = datetime.min (naive).
    state.set("strat_a", AnchorRecord(strategy_instance_id="i"))
    assert state.should_apply_revision("strat_a", _BAR_10, _REV_T0) is True


def test_case_7_restore_then_serialize_then_compare_tuple():
    """Restore a state, serialize via compute_state_hash, restore another with same
    values — the tuple comparison invariant still holds for both."""
    from libs.computation.checkpoint_store import compute_state_hash
    s1 = _restored_state_at(_BAR_10, _REV_T0)
    s2 = _restored_state_at(_BAR_10, _REV_T0)
    assert compute_state_hash(s1) == compute_state_hash(s2)
    # Both reject the same older revision.
    assert s1.should_apply_revision("strat_a", _BAR_10, _REV_TM1) is False
    assert s2.should_apply_revision("strat_a", _BAR_10, _REV_TM1) is False
```

- [ ] **Step 2: Run tests — verify pass**

```bash
pytest services/pnl_consumer/tests/test_real_trade_revision_restore.py -v
```

Expected: all seven tests pass (using the existing should_apply_revision logic).

- [ ] **Step 3: Commit**

```bash
git add services/pnl_consumer/tests/test_real_trade_revision_restore.py
git commit -m "test: real-trade revision guard preserves correctness after restore"
```

---

## Task 15: Wire checkpoint write into pnl_consumer flush path

**Files:**
- Modify: `services/pnl_consumer/pnl_consumer/pnl_consumer.py`

The flush site is around lines 720–745 (`insert_rows(...)` calls followed by `consumer.commit(asynchronous=False)`). We add a best-effort checkpoint write between the ClickHouse inserts and the Kafka commit.

- [ ] **Step 1: Read the current flush block to confirm the insertion point**

```bash
sed -n '720,745p' services/pnl_consumer/pnl_consumer/pnl_consumer.py
```

Expected: see the `insert_rows("analytics.futures_price_1min", ...)` ... `consumer.commit(asynchronous=False)` sequence.

- [ ] **Step 2: Add imports**

Near the top of `services/pnl_consumer/pnl_consumer/pnl_consumer.py` (with the other `from libs.computation import ...` block, around line 32), add:

```python
from libs.computation.checkpoint_store import write_checkpoint
from libs.postgres_client import get_client as pg_get_client
```

- [ ] **Step 3: Add a helper at module scope**

Add near the top of the file (after the `_MODE_CONFIG` dict, around line 94), the following helper. Replace `<msg>` with the actual variable name for the Kafka message in scope (in the flush path, it's the candle's source message — confirm by reading the flush block).

```python
_CHECKPOINT_ENABLED = os.getenv("STREAMING_CHECKPOINT_ENABLED", "true").lower() == "true"
_CHECKPOINT_WRITE_ENABLED = (
    _CHECKPOINT_ENABLED
    and os.getenv("STREAMING_CHECKPOINT_WRITE_ENABLED", "true").lower() == "true"
)


def _try_write_checkpoint(
    *,
    mode: str,
    anchor_state: AnchorState,
    pg_client,
    kafka_topic: str,
    kafka_partition: int,
    kafka_offset: int,
    last_candle_ts: datetime,
) -> None:
    """Best-effort checkpoint write. Logs and continues on any failure.

    Never raises. Failures increment a future Prometheus counter and write a
    log line — callers must not assume success.
    """
    if not _CHECKPOINT_WRITE_ENABLED:
        return
    try:
        write_checkpoint(
            mode=mode,
            anchor_state=anchor_state,
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            last_candle_ts=last_candle_ts,
            client=pg_client,
        )
    except Exception as exc:
        logger.warning(
            "checkpoint write failed (continuing). mode=%s offset=%d err=%s",
            mode, kafka_offset, type(exc).__name__,
        )
```

- [ ] **Step 4: Call _try_write_checkpoint after ClickHouse inserts, before consumer.commit**

In the flush block (around line 738 — verify by reading), modify to look like:

```python
# ... existing insert_rows() calls for futures_price_1min, prod, real_trade, bt ...

# Best-effort checkpoint writes per active mode, before Kafka commit.
if sink_config.prod and pg_client_prod is not None:
    _try_write_checkpoint(
        mode="prod",
        anchor_state=state_prod,
        pg_client=pg_client_prod,
        kafka_topic=msg.topic(),
        kafka_partition=msg.partition(),
        kafka_offset=msg.offset(),
        last_candle_ts=candle_ts,
    )
if sink_config.bt and pg_client_bt is not None:
    _try_write_checkpoint(
        mode="bt",
        anchor_state=state_bt,
        pg_client=pg_client_bt,
        kafka_topic=msg.topic(),
        kafka_partition=msg.partition(),
        kafka_offset=msg.offset(),
        last_candle_ts=candle_ts,
    )
if sink_config.real_trade and pg_client_real_trade is not None:
    _try_write_checkpoint(
        mode="real_trade",
        anchor_state=state_real_trade,
        pg_client=pg_client_real_trade,
        kafka_topic=msg.topic(),
        kafka_partition=msg.partition(),
        kafka_offset=msg.offset(),
        last_candle_ts=candle_ts,
    )

consumer.commit(asynchronous=False)
```

Note: the actual variable name for the Kafka message in scope must be confirmed. The existing flush path uses `msg` in the broader handler — verify against the surrounding context. The `candle_ts` variable should already exist (it's the candle's timestamp used elsewhere in the flush).

Variables `pg_client_prod`, `pg_client_bt`, `pg_client_real_trade` are wired in the next step (Task 16) — they are passed into the flush block from `run()`.

- [ ] **Step 5: Verify the file still parses**

```bash
python -c "import ast; ast.parse(open('services/pnl_consumer/pnl_consumer/pnl_consumer.py').read()); print('ok')"
```

Expected: prints `ok`.

- [ ] **Step 6: Commit**

```bash
git add services/pnl_consumer/pnl_consumer/pnl_consumer.py
git commit -m "pnl_consumer: best-effort checkpoint write between CH insert and Kafka commit"
```

---

## Task 16: Wire cold-start entry point + Postgres connection lifecycle

**Files:**
- Modify: `services/pnl_consumer/pnl_consumer/pnl_consumer.py`

Replace the direct `_bootstrap_state` / `_bootstrap_bt_state` calls in `run()` (around lines 832-836) with calls to `cold_start.load_or_bootstrap`. Open per-mode Postgres connections at the top of `run()`, pass them in, close on shutdown.

- [ ] **Step 1: Add imports**

Near the top of `pnl_consumer.py` add:

```python
from libs.computation.checkpoint_store import read_checkpoint
from pnl_consumer.cold_start import (
    FallbackReason,
    clickhouse_invariant_check,
    load_or_bootstrap,
)
```

- [ ] **Step 2: Read the current run() bootstrap block**

```bash
sed -n '809,860p' services/pnl_consumer/pnl_consumer/pnl_consumer.py
```

- [ ] **Step 3: Open Postgres connections per active mode**

Modify `run()` to add Postgres connection setup at the top, before the bootstrap block. Place after the existing consumer setup and before the `state_prod = ...` lines:

```python
# Cold-start config from env.
_CHECKPOINT_READ_ENABLED = (
    _CHECKPOINT_ENABLED
    and os.getenv("STREAMING_CHECKPOINT_READ_ENABLED", "true").lower() == "true"
)
_INVARIANT_CHECK_ENABLED = (
    os.getenv("STREAMING_CHECKPOINT_INVARIANT_CHECK_ENABLED", "true").lower() == "true"
)
_CHECKPOINT_FRESHNESS_SECONDS = int(
    os.getenv("STREAMING_CHECKPOINT_FRESHNESS_SECONDS", "86400")
)

# Open one Postgres connection per active sink mode (None if mode disabled).
pg_client_prod = None
pg_client_bt = None
pg_client_real_trade = None
if _CHECKPOINT_ENABLED and sink_config.prod:
    pg_client_prod = pg_get_client()
if _CHECKPOINT_ENABLED and sink_config.bt:
    pg_client_bt = pg_get_client()
if _CHECKPOINT_ENABLED and sink_config.real_trade:
    pg_client_real_trade = pg_get_client()
```

- [ ] **Step 4: Replace the bootstrap calls with load_or_bootstrap**

In `run()`, replace the existing bootstrap block (lines 832-836):

```python
# OLD:
if sink_config.prod:
    state_prod = _bootstrap_state("prod", reference_ts)
if sink_config.bt:
    state_bt = _bootstrap_bt_state(reference_ts)
if sink_config.real_trade:
    state_real_trade = _bootstrap_state("real_trade", reference_ts)
```

with:

```python
# NEW: try checkpoint first per mode; fall back to existing bootstrap on any failure.
now_utc = datetime.now(UTC)

def _committed_offset(topic_partition_list) -> int:
    """Helper: return the committed offset for partition 0 (single-partition topic).
    If multi-partition, picks the maximum committed offset across partitions."""
    committed = consumer.committed(topic_partition_list, timeout=5.0)
    offsets = [tp.offset for tp in committed if tp.offset != OFFSET_INVALID]
    return max(offsets) if offsets else 0

_partitions_for_offset = [TopicPartition(TOPIC, 0)]  # single-partition assumption

if sink_config.prod:
    if _CHECKPOINT_READ_ENABLED and pg_client_prod is not None:
        ch_for_invariant = ch_get_client() if _INVARIANT_CHECK_ENABLED else None
        state_prod, reason = load_or_bootstrap(
            mode="prod",
            pg_client=pg_client_prod,
            kafka_committed_offset=_committed_offset(_partitions_for_offset),
            now=now_utc,
            max_age_seconds=_CHECKPOINT_FRESHNESS_SECONDS,
            invariant_check_enabled=_INVARIANT_CHECK_ENABLED,
            read_checkpoint_fn=read_checkpoint,
            bootstrap_fn=lambda: _bootstrap_state("prod", reference_ts),
            invariant_check_fn=(
                lambda *, mode, result: clickhouse_invariant_check(
                    mode=mode, result=result, ch_client=ch_for_invariant
                )
            ) if ch_for_invariant else None,
        )
        if ch_for_invariant:
            ch_for_invariant.close()
    else:
        state_prod = _bootstrap_state("prod", reference_ts)

if sink_config.bt:
    if _CHECKPOINT_READ_ENABLED and pg_client_bt is not None:
        ch_for_invariant = ch_get_client() if _INVARIANT_CHECK_ENABLED else None
        state_bt, reason = load_or_bootstrap(
            mode="bt",
            pg_client=pg_client_bt,
            kafka_committed_offset=_committed_offset(_partitions_for_offset),
            now=now_utc,
            max_age_seconds=_CHECKPOINT_FRESHNESS_SECONDS,
            invariant_check_enabled=_INVARIANT_CHECK_ENABLED,
            read_checkpoint_fn=read_checkpoint,
            bootstrap_fn=lambda: _bootstrap_bt_state(reference_ts),
            invariant_check_fn=(
                lambda *, mode, result: clickhouse_invariant_check(
                    mode=mode, result=result, ch_client=ch_for_invariant
                )
            ) if ch_for_invariant else None,
        )
        if ch_for_invariant:
            ch_for_invariant.close()
    else:
        state_bt = _bootstrap_bt_state(reference_ts)

if sink_config.real_trade:
    if _CHECKPOINT_READ_ENABLED and pg_client_real_trade is not None:
        ch_for_invariant = ch_get_client() if _INVARIANT_CHECK_ENABLED else None
        state_real_trade, reason = load_or_bootstrap(
            mode="real_trade",
            pg_client=pg_client_real_trade,
            kafka_committed_offset=_committed_offset(_partitions_for_offset),
            now=now_utc,
            max_age_seconds=_CHECKPOINT_FRESHNESS_SECONDS,
            invariant_check_enabled=_INVARIANT_CHECK_ENABLED,
            read_checkpoint_fn=read_checkpoint,
            bootstrap_fn=lambda: _bootstrap_state("real_trade", reference_ts),
            invariant_check_fn=(
                lambda *, mode, result: clickhouse_invariant_check(
                    mode=mode, result=result, ch_client=ch_for_invariant
                )
            ) if ch_for_invariant else None,
        )
        if ch_for_invariant:
            ch_for_invariant.close()
    else:
        state_real_trade = _bootstrap_state("real_trade", reference_ts)
```

(The `from libs.clickhouse_client import get_client as ch_get_client` may already be implicitly imported; if not, add the explicit alias near the top.)

- [ ] **Step 5: Ensure pg_client_* are closed on shutdown**

Locate the existing shutdown path in `run()` (look for `consumer.close()`). Add directly before or after that:

```python
for pg in (pg_client_prod, pg_client_bt, pg_client_real_trade):
    if pg is not None:
        try:
            pg.close()
        except Exception:
            pass
```

- [ ] **Step 6: Verify the file still parses**

```bash
python -c "import ast; ast.parse(open('services/pnl_consumer/pnl_consumer/pnl_consumer.py').read()); print('ok')"
```

Expected: prints `ok`.

- [ ] **Step 7: Run the full unit test suite to confirm nothing broke**

```bash
pytest -m unit -v
```

Expected: all unit tests pass. No new failures.

- [ ] **Step 8: Commit**

```bash
git add services/pnl_consumer/pnl_consumer/pnl_consumer.py
git commit -m "pnl_consumer: cold-start uses checkpoint with bootstrap fallback"
```

---

## Task 17: End-to-end test — cold-start scenarios

**Files:**
- Create: `services/pnl_consumer/tests/test_cold_start_e2e.py`

These tests use a real Postgres (testcontainers, already in dev deps). Marker: `streaming_integration`. They do not require ClickHouse — the bootstrap function is mocked, so the test focuses on the cold-start decision tree against a real Postgres backend.

- [ ] **Step 1: Write the E2E tests**

Create `services/pnl_consumer/tests/test_cold_start_e2e.py`:

```python
"""E2E tests for cold-start against a real Postgres (testcontainers).

These verify the full write → read → sanity-check → fallback decision tree
against an actual Postgres backend. ClickHouse and Kafka are mocked out —
this test exercises Supabase semantics end-to-end.
"""

from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest

pytestmark = pytest.mark.streaming_integration


@pytest.fixture(scope="module")
def pg_container():
    from testcontainers.postgres import PostgresContainer
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture
def pg_env(pg_container, monkeypatch):
    monkeypatch.setenv("SUPABASE_HOST", pg_container.get_container_host_ip())
    monkeypatch.setenv("SUPABASE_PORT", str(pg_container.get_exposed_port(5432)))
    monkeypatch.setenv("SUPABASE_USER", "test")
    monkeypatch.setenv("SUPABASE_PASSWORD", "test")
    monkeypatch.setenv("SUPABASE_DATABASE", "test")
    monkeypatch.setenv("SUPABASE_SSLMODE", "disable")
    yield


@pytest.fixture
def pg_client(pg_env):
    from libs.postgres_client import get_client
    client = get_client()
    # Apply schema.
    with open("infra/schemas/streaming_supabase.sql") as f:
        ddl = f.read()
    with client.cursor() as cur:
        cur.execute(ddl)
    client.commit()
    yield client
    client.close()


def _make_anchor_state():
    from libs.computation.anchor_state import AnchorRecord, AnchorState
    s = AnchorState()
    s.set("strat_a", AnchorRecord(
        pnl=1.0, price=100.0, position=1.0,
        bar_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
        revision_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
        strategy_id=1, strategy_name="s", underlying="BTC",
        config_timeframe="1m", weighting=1.0,
        strategy_instance_id="i", final_signal=0.0, benchmark=0.0,
    ))
    return s


def test_first_start_falls_back_to_bootstrap(pg_client):
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap
    from libs.computation.checkpoint_store import read_checkpoint

    bootstrap_state = _make_anchor_state()
    fake_bootstrap = mock.MagicMock(return_value=bootstrap_state)

    state, reason = load_or_bootstrap(
        mode="prod",
        pg_client=pg_client,
        kafka_committed_offset=101,
        now=datetime.now(UTC),
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=read_checkpoint,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason == FallbackReason.NO_CHECKPOINT
    fake_bootstrap.assert_called_once()
    assert state is bootstrap_state


def test_warm_restart_uses_checkpoint(pg_client):
    from libs.computation.checkpoint_store import read_checkpoint, write_checkpoint
    from pnl_consumer.cold_start import load_or_bootstrap

    state = _make_anchor_state()
    last_candle_ts = datetime.now(UTC) - timedelta(minutes=1)
    write_checkpoint(
        mode="prod", anchor_state=state,
        kafka_topic="binance.price.ticks", kafka_partition=0, kafka_offset=100,
        last_candle_ts=last_candle_ts, client=pg_client,
    )

    fake_bootstrap = mock.MagicMock()  # must not be called
    loaded, reason = load_or_bootstrap(
        mode="prod",
        pg_client=pg_client,
        kafka_committed_offset=101,  # one ahead, matches semantics
        now=datetime.now(UTC),
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=read_checkpoint,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason is None
    fake_bootstrap.assert_not_called()
    assert "strat_a" in loaded.keys()
    assert loaded.get("strat_a").pnl == 1.0


def test_offset_lag_after_write_failure_triggers_fallback(pg_client):
    """Simulate Supabase outage: checkpoint reflects offset 50, but Kafka committed offset is 100."""
    from libs.computation.checkpoint_store import read_checkpoint, write_checkpoint
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap

    state = _make_anchor_state()
    write_checkpoint(
        mode="prod", anchor_state=state,
        kafka_topic="binance.price.ticks", kafka_partition=0, kafka_offset=50,
        last_candle_ts=datetime.now(UTC) - timedelta(minutes=1),
        client=pg_client,
    )

    fake_bootstrap = mock.MagicMock(return_value=_make_anchor_state())
    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=pg_client,
        kafka_committed_offset=101,
        now=datetime.now(UTC),
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=read_checkpoint,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason == FallbackReason.OFFSET_LAG
    fake_bootstrap.assert_called_once()


def test_schema_version_bump_falls_back(pg_client):
    """Manually write a row with version=99, then load with current SCHEMA_VERSION."""
    from libs.computation.checkpoint_store import (
        SCHEMA_VERSION,
        read_checkpoint,
        write_checkpoint,
    )
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap

    state = _make_anchor_state()
    write_checkpoint(
        mode="prod", anchor_state=state,
        kafka_topic="binance.price.ticks", kafka_partition=0, kafka_offset=100,
        last_candle_ts=datetime.now(UTC) - timedelta(minutes=1),
        client=pg_client,
    )
    # Tamper schema_version after the legitimate write.
    with pg_client.cursor() as cur:
        cur.execute(
            "UPDATE streaming.pnl_commit_state SET schema_version = %s WHERE mode = 'prod'",
            (SCHEMA_VERSION + 99,),
        )
    pg_client.commit()

    fake_bootstrap = mock.MagicMock(return_value=_make_anchor_state())
    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=pg_client,
        kafka_committed_offset=101,
        now=datetime.now(UTC),
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=read_checkpoint,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason == FallbackReason.SCHEMA_VERSION_MISMATCH


def test_hash_tamper_falls_back(pg_client):
    """Tamper one pnl value after write; verify hash check rejects the read."""
    from libs.computation.checkpoint_store import read_checkpoint, write_checkpoint
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap

    state = _make_anchor_state()
    write_checkpoint(
        mode="prod", anchor_state=state,
        kafka_topic="binance.price.ticks", kafka_partition=0, kafka_offset=100,
        last_candle_ts=datetime.now(UTC) - timedelta(minutes=1),
        client=pg_client,
    )
    with pg_client.cursor() as cur:
        cur.execute(
            "UPDATE streaming.pnl_checkpoint SET pnl = pnl + 999.0 WHERE mode = 'prod' AND strategy_table_name = 'strat_a'"
        )
    pg_client.commit()

    fake_bootstrap = mock.MagicMock(return_value=_make_anchor_state())
    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=pg_client,
        kafka_committed_offset=101,
        now=datetime.now(UTC),
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=read_checkpoint,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason == FallbackReason.HASH_MISMATCH
```

- [ ] **Step 2: Run the E2E tests**

```bash
pytest services/pnl_consumer/tests/test_cold_start_e2e.py -v -m streaming_integration
```

Expected: all five tests pass. Each takes 1–3s for the Postgres testcontainer startup.

- [ ] **Step 3: Commit**

```bash
git add services/pnl_consumer/tests/test_cold_start_e2e.py
git commit -m "test(e2e): cold-start scenarios against real Postgres"
```

---

## Task 18: Real-trade crash-restart property test

**Files:**
- Create: `services/pnl_consumer/tests/test_real_trade_recovery_property.py`

The spec's strongest correctness guarantee: "Same input fed to (a) consumer that never crashed and (b) consumer killed mid-stream then restored produces identical final AnchorState." This task implements that property at the AnchorState + checkpoint_store layer (no Kafka subprocess required) — sufficient to catch any restore-correctness bugs in the revision guard or serialization.

A full subprocess-based E2E (separate Kafka + ClickHouse containers driving the actual consumer binary) is out of scope; the spec acknowledges the per-layer tests provide strong defense in depth.

- [ ] **Step 1: Write the property test**

Create `services/pnl_consumer/tests/test_real_trade_recovery_property.py`:

```python
"""Property test: restoring AnchorState from checkpoint mid-stream produces the
same final state as running through without a restart.

Drives `AnchorState.should_apply_revision` + `compute_pnl` directly, mirroring
what the consumer does per Kafka message. The crash point is varied; the final
states (and final state hashes) must match for every crash point.
"""

from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.checkpoint_store import (
    compute_state_hash,
    read_checkpoint,
    write_checkpoint,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def fake_pg_conn():
    """In-memory shim that satisfies write_checkpoint/read_checkpoint."""
    store = {"checkpoint": {}, "commit_state": {}}

    def make_cursor():
        cur = mock.MagicMock()
        cur.__enter__ = mock.MagicMock(return_value=cur)
        cur.__exit__ = mock.MagicMock(return_value=False)

        def execute(sql, params=None):
            cur._last_sql = sql
            if params is None:
                return
            if "FROM streaming.pnl_commit_state" in sql:
                mode = params[0]
                cur.description = [(c,) for c in [
                    "mode", "last_candle_ts", "kafka_topic", "kafka_partition",
                    "kafka_offset", "state_hash", "schema_version",
                ]]
                cur._rows = [store["commit_state"][mode]] if mode in store["commit_state"] else []
            elif "FROM streaming.pnl_checkpoint" in sql:
                mode = params[0]
                cur.description = [(c,) for c in [
                    "strategy_table_name", "pnl", "price", "position",
                    "bar_ts", "revision_ts",
                    "strategy_id", "strategy_name", "underlying", "config_timeframe",
                    "weighting", "strategy_instance_id", "final_signal", "benchmark",
                ]]
                cur._rows = [v[1:-1] for (m, _stn), v in store["checkpoint"].items() if m == mode]
            elif "INTO streaming.pnl_commit_state" in sql:
                store["commit_state"][params[0]] = params

        def executemany(sql, params_list):
            for p in params_list:
                store["checkpoint"][(p[0], p[1])] = p + (None,)

        def fetchall():
            return cur._rows

        cur.execute = execute
        cur.executemany = executemany
        cur.fetchall = fetchall
        return cur

    conn = mock.MagicMock()
    conn.cursor.side_effect = make_cursor
    return conn


def _seed_state() -> AnchorState:
    s = AnchorState()
    s.set("strat_a", AnchorRecord(
        pnl=0.0, price=100.0, position=0.0,
        bar_ts=datetime.min.replace(tzinfo=UTC),
        revision_ts=datetime.min.replace(tzinfo=UTC),
        strategy_id=1, strategy_name="s", underlying="BTC",
        config_timeframe="1m", weighting=1.0,
        strategy_instance_id="i", final_signal=0.0, benchmark=0.0,
    ))
    return s


def _make_revision_stream():
    """Deterministic real-trade revision stream: 6 revisions across 3 bars."""
    base_bar = datetime(2024, 6, 1, 10, 0, tzinfo=UTC)
    base_rev = base_bar
    return [
        # (bar_ts, revision_ts, position, price)
        (base_bar,                   base_rev,                   1.0, 101.0),
        (base_bar,                   base_rev + timedelta(s=30), 1.5, 102.0),
        (base_bar + timedelta(s=60), base_rev + timedelta(s=60), 2.0, 103.0),
        # Older bar arrives late — must be rejected.
        (base_bar,                   base_rev - timedelta(s=10), 9.9, 999.0),
        (base_bar + timedelta(s=60), base_rev + timedelta(s=90), 2.5, 104.0),
        (base_bar + timedelta(s=120), base_rev + timedelta(s=120), 3.0, 105.0),
    ]


# timedelta accepts seconds=... not s=... — alias for readability above doesn't work.
# Inline construction below replaces the helper.
def _stream():
    base_bar = datetime(2024, 6, 1, 10, 0, tzinfo=UTC)
    base_rev = base_bar
    td = lambda s: timedelta(seconds=s)  # noqa: E731
    return [
        (base_bar,             base_rev,           1.0, 101.0),
        (base_bar,             base_rev + td(30),  1.5, 102.0),
        (base_bar + td(60),    base_rev + td(60),  2.0, 103.0),
        (base_bar,             base_rev - td(10),  9.9, 999.0),  # late, rejected
        (base_bar + td(60),    base_rev + td(90),  2.5, 104.0),
        (base_bar + td(120),   base_rev + td(120), 3.0, 105.0),
    ]


def _apply_revision(state: AnchorState, bar_ts, revision_ts, position, price):
    if not state.should_apply_revision("strat_a", bar_ts, revision_ts):
        return
    state.compute_pnl("strat_a", current_price=price, position=position,
                      bar_ts=bar_ts, revision_ts=revision_ts)


@pytest.mark.parametrize("crash_after", [1, 2, 3, 4, 5])
def test_crash_at_each_step_matches_fresh_run(fake_pg_conn, crash_after):
    """For each possible crash point, restoring matches a fresh run."""
    stream = _stream()

    # Reference run: no crash.
    fresh = _seed_state()
    for rev in stream:
        _apply_revision(fresh, *rev)
    fresh_hash = compute_state_hash(fresh)

    # Crash run: apply first crash_after revisions, checkpoint, restore, finish.
    crashed = _seed_state()
    for rev in stream[:crash_after]:
        _apply_revision(crashed, *rev)

    write_checkpoint(
        mode="real_trade",
        anchor_state=crashed,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=crash_after,
        last_candle_ts=stream[crash_after - 1][0],  # bar_ts of last applied
        client=fake_pg_conn,
    )

    loaded = read_checkpoint(mode="real_trade", client=fake_pg_conn)
    assert loaded is not None
    restored = loaded.anchor_state

    for rev in stream[crash_after:]:
        _apply_revision(restored, *rev)

    assert compute_state_hash(restored) == fresh_hash, (
        f"crash_after={crash_after}: restored state diverged from fresh run"
    )
```

- [ ] **Step 2: Run the test**

```bash
pytest services/pnl_consumer/tests/test_real_trade_recovery_property.py -v
```

Expected: five parametrized cases all pass (crash_after = 1..5).

- [ ] **Step 3: Commit**

```bash
git add services/pnl_consumer/tests/test_real_trade_recovery_property.py
git commit -m "test: crash-restart at any point produces same AnchorState as fresh run"
```

---

## Task 19: Terraform — env vars and Secrets Manager

**Files:**
- Modify: `infra/terraform/main.tf` (multiple sections)

- [ ] **Step 1: Add Supabase Secrets Manager entry**

Append to `infra/terraform/main.tf` (a section that logically groups with the existing `aws_secretsmanager_secret.clickhouse` resource):

```hcl
resource "aws_secretsmanager_secret" "supabase" {
  name                    = "${local.name_prefix}/supabase"
  description             = "Supabase Postgres credentials for streaming pipeline checkpointing"
  recovery_window_in_days = 0

  tags = local.common_tags
}

resource "aws_secretsmanager_secret_version" "supabase" {
  secret_id = aws_secretsmanager_secret.supabase.id
  secret_string = jsonencode({
    host     = var.supabase_host
    port     = "6543"
    user     = var.supabase_user
    password = var.supabase_password
    database = var.supabase_database
    sslmode  = "require"
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}
```

- [ ] **Step 2: Add input variables**

In `infra/terraform/main.tf` (variables section near the top, or wherever existing `variable "..."` blocks are), append:

```hcl
variable "supabase_host" {
  type        = string
  default     = ""
  description = "Supabase Postgres host (set via TF_VAR_supabase_host)"
}

variable "supabase_user" {
  type        = string
  default     = "postgres"
  description = "Supabase Postgres user"
}

variable "supabase_password" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Supabase Postgres password (set via TF_VAR_supabase_password)"
}

variable "supabase_database" {
  type        = string
  default     = "postgres"
  description = "Supabase Postgres database name"
}
```

- [ ] **Step 3: Inject Supabase env vars + checkpoint flags into pnl_consumer task definitions**

In `infra/terraform/main.tf` around line 1288–1322 (the `aws_ecs_task_definition.pnl_consumer.container_definitions`), modify the `secrets` and `environment` lists:

```hcl
  container_definitions = jsonencode([{
    name      = "pnl-consumer"
    image     = "${aws_ecr_repository.pnl_consumer.repository_url}:latest"
    essential = true
    secrets = [
      { name = "CLICKHOUSE_HOST",     valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
      { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
      { name = "SUPABASE_HOST",       valueFrom = "${aws_secretsmanager_secret.supabase.arn}:host::" },
      { name = "SUPABASE_PORT",       valueFrom = "${aws_secretsmanager_secret.supabase.arn}:port::" },
      { name = "SUPABASE_USER",       valueFrom = "${aws_secretsmanager_secret.supabase.arn}:user::" },
      { name = "SUPABASE_PASSWORD",   valueFrom = "${aws_secretsmanager_secret.supabase.arn}:password::" },
      { name = "SUPABASE_DATABASE",   valueFrom = "${aws_secretsmanager_secret.supabase.arn}:database::" },
      { name = "SUPABASE_SSLMODE",    valueFrom = "${aws_secretsmanager_secret.supabase.arn}:sslmode::" },
    ]
    environment = [
      { name = "CLICKHOUSE_PORT",         value = "8443" },
      { name = "CLICKHOUSE_USER",         value = each.value.clickhouse_user },
      { name = "CLICKHOUSE_SECURE",       value = "true" },
      { name = "REDPANDA_BROKERS",        value = "redpanda.${local.name_prefix}.local:9092" },
      { name = "KAFKA_GROUP_ID",          value = each.value.group_id },
      { name = "ENABLE_PRICE_SINK",       value = each.value.enable_price },
      { name = "ENABLE_PROD_SINK",        value = each.value.enable_prod },
      { name = "ENABLE_REAL_TRADE_SINK",  value = each.value.enable_real_trade },
      { name = "ENABLE_BT_SINK",          value = each.value.enable_bt },
      # Checkpoint flags — defaults match Phase 2 (shadow mode: writes on, reads off).
      { name = "STREAMING_CHECKPOINT_ENABLED",                  value = "true" },
      { name = "STREAMING_CHECKPOINT_WRITE_ENABLED",            value = "true" },
      { name = "STREAMING_CHECKPOINT_READ_ENABLED",             value = "false" },
      { name = "STREAMING_CHECKPOINT_INVARIANT_CHECK_ENABLED",  value = "true" },
      { name = "STREAMING_CHECKPOINT_FRESHNESS_SECONDS",        value = "86400" },
    ]
    ...
  }])
```

- [ ] **Step 4: Grant the ECS task IAM role read access to the Supabase secret**

Find the IAM policy that grants the pnl_consumer task role access to Secrets Manager. Add the Supabase secret ARN to that policy's `Resource` list. Search the file for `aws_secretsmanager_secret.clickhouse.arn` — the same locations need a parallel entry for `aws_secretsmanager_secret.supabase.arn`.

- [ ] **Step 5: Validate Terraform**

```bash
cd infra/terraform && terraform fmt && terraform validate
```

Expected: no errors. Run `terraform plan` to see what would change — review carefully before applying.

- [ ] **Step 6: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "infra(terraform): add Supabase secret + checkpoint env vars to pnl_consumer"
```

---

## Task 20: Grafana dashboard for checkpoint health

**Files:**
- Create: `infra/grafana/dashboards/streaming-checkpoint.json`

The pipeline's monitoring is Grafana Cloud. The minimal dashboard tracks the most actionable signals from the spec.

- [ ] **Step 1: Inspect an existing dashboard for shape**

```bash
ls infra/grafana/dashboards/
cat infra/grafana/dashboards/*.json | head -80
```

Identify the JSON schema version and existing variable conventions used in the project's dashboards.

- [ ] **Step 2: Create the dashboard JSON**

Create `infra/grafana/dashboards/streaming-checkpoint.json`. The exact panel shape depends on what metrics emitter the consumer uses (CloudWatch via awslogs, or another). For now, log-based panels are sufficient:

```json
{
  "title": "Streaming Checkpoint Health",
  "uid": "streaming-checkpoint",
  "schemaVersion": 39,
  "version": 1,
  "tags": ["streaming", "pnl-consumer", "checkpoint"],
  "time": { "from": "now-6h", "to": "now" },
  "refresh": "1m",
  "panels": [
    {
      "id": 1,
      "title": "Cold-start fallback reasons (last 24h)",
      "type": "logs",
      "datasource": { "type": "cloudwatch", "uid": "cloudwatch" },
      "targets": [{
        "logGroups": [{ "name": "/ecs/trading-analysis-streaming" }],
        "queryString": "fields @timestamp, @message | filter @message like /cold-start: (sanity check failed|invariant check failed|no checkpoint row)/ | sort @timestamp desc | limit 200",
        "region": "ap-northeast-1"
      }],
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 0 }
    },
    {
      "id": 2,
      "title": "Checkpoint write failures (last 24h)",
      "type": "logs",
      "datasource": { "type": "cloudwatch", "uid": "cloudwatch" },
      "targets": [{
        "logGroups": [{ "name": "/ecs/trading-analysis-streaming" }],
        "queryString": "fields @timestamp, @message | filter @message like /checkpoint write failed/ | stats count() by bin(5m)",
        "region": "ap-northeast-1"
      }],
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 8 }
    },
    {
      "id": 3,
      "title": "Cold-start: checkpoint-restored vs fallback (last 7d)",
      "type": "logs",
      "datasource": { "type": "cloudwatch", "uid": "cloudwatch" },
      "targets": [{
        "logGroups": [{ "name": "/ecs/trading-analysis-streaming" }],
        "queryString": "fields @timestamp, @message | filter @message like /cold-start: (checkpoint restored|sanity check failed|no checkpoint row|invariant check failed)/ | sort @timestamp desc",
        "region": "ap-northeast-1"
      }],
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 16 }
    }
  ]
}
```

The CI/CD `deploy-grafana-cloud` job will push this on next merge to main (see project CLAUDE.md).

- [ ] **Step 3: Commit**

```bash
git add infra/grafana/dashboards/streaming-checkpoint.json
git commit -m "grafana: dashboard for checkpoint health (fallback reasons + write failures)"
```

---

## Task 21: Apply schema to Supabase (one-off manual procedure)

This is an operational task, not a code change. It runs **once** against the live Supabase project, before any consumer deployment that has checkpointing enabled.

- [ ] **Step 1: Confirm Supabase connection credentials**

```bash
# Retrieve from password manager / Supabase console.
# Set env vars locally — do not commit.
export SUPABASE_HOST="db.<projectref>.supabase.co"
export SUPABASE_PORT="6543"
export SUPABASE_USER="postgres"
export SUPABASE_PASSWORD="<from-supabase-console>"
export SUPABASE_DATABASE="postgres"
```

- [ ] **Step 2: Apply the schema**

```bash
PGPASSWORD="$SUPABASE_PASSWORD" psql \
  -h "$SUPABASE_HOST" \
  -p "$SUPABASE_PORT" \
  -U "$SUPABASE_USER" \
  -d "$SUPABASE_DATABASE" \
  --set sslmode=require \
  -f infra/schemas/streaming_supabase.sql
```

Expected output: `CREATE SCHEMA`, `CREATE TABLE` (twice), `CREATE INDEX`. If the schema or tables already exist (`IF NOT EXISTS`), the statements are no-ops.

- [ ] **Step 3: Verify tables exist**

```bash
PGPASSWORD="$SUPABASE_PASSWORD" psql \
  -h "$SUPABASE_HOST" -p "$SUPABASE_PORT" -U "$SUPABASE_USER" -d "$SUPABASE_DATABASE" \
  -c "\dt streaming.*"
```

Expected: two rows — `pnl_checkpoint` and `pnl_commit_state`.

- [ ] **Step 4: Write the AWS Secrets Manager entry**

```bash
aws secretsmanager create-secret \
  --name trading-analysis/supabase \
  --secret-string "{\"host\":\"$SUPABASE_HOST\",\"port\":\"$SUPABASE_PORT\",\"user\":\"$SUPABASE_USER\",\"password\":\"$SUPABASE_PASSWORD\",\"database\":\"$SUPABASE_DATABASE\",\"sslmode\":\"require\"}" \
  --region ap-northeast-1 \
  --profile AdministratorAccess-068704208855
```

If the secret already exists from a prior Terraform apply, use `update-secret-version` instead.

---

## Task 22: Phase 2 deploy — shadow mode (writes only)

Operational task. Assumes Tasks 1–21 are merged to main and the schema has been applied.

- [ ] **Step 1: Confirm Terraform defaults are write-on, read-off**

```bash
grep -A 6 "STREAMING_CHECKPOINT_" infra/terraform/main.tf
```

Expected: `STREAMING_CHECKPOINT_WRITE_ENABLED=true`, `STREAMING_CHECKPOINT_READ_ENABLED=false`.

- [ ] **Step 2: Trigger CI/CD deploy**

Merge the integrated branch to main. The `ci-cd.yml` workflow rebuilds the pnl-consumer image and rolls all four ECS services.

- [ ] **Step 3: Verify rollout health**

```bash
aws ecs describe-services \
  --cluster trading-analysis \
  --services trading-analysis-pnl-consumer-price trading-analysis-pnl-consumer-prod \
             trading-analysis-pnl-consumer-bt trading-analysis-pnl-consumer-real-trade \
  --region ap-northeast-1 \
  --query 'services[].{name:serviceName,desired:desiredCount,running:runningCount}'
```

Expected: all four services show desired=running. Wait ~10 min for ECS rolling deployment.

- [ ] **Step 4: Confirm checkpoint rows are being written**

After ~10 min of runtime (long enough for at least one flush per mode):

```bash
PGPASSWORD=... psql -h ... -c "
  SELECT mode, COUNT(*) AS rows, MAX(updated_at) AS last_update
  FROM streaming.pnl_checkpoint
  GROUP BY mode;
"
```

Expected: rows for prod, bt, real_trade. `last_update` within last few minutes.

```bash
PGPASSWORD=... psql -h ... -c "
  SELECT mode, last_candle_ts, kafka_offset, state_hash, schema_version
  FROM streaming.pnl_commit_state;
"
```

Expected: one row per active mode with recent `last_candle_ts` and a non-empty `state_hash`.

- [ ] **Step 5: Validate state correctness (sample 5 strategies per mode)**

For 5 random rows per mode, query the corresponding ClickHouse pnl table for the latest row and confirm the checkpoint matches within tolerance:

```bash
# Pick a strategy_table_name from streaming.pnl_checkpoint, then:
clickhouse-client -h $CLICKHOUSE_HOST --secure --password $CLICKHOUSE_PASSWORD --query "
  SELECT strategy_table_name, cumulative_pnl, price, position, ts
  FROM analytics.strategy_pnl_1min_prod_v2
  WHERE strategy_table_name = '<picked>'
  ORDER BY ts DESC LIMIT 1
"
```

Compare to the corresponding `streaming.pnl_checkpoint` row's `(pnl, price, position)`. Should match within float tolerance.

- [ ] **Step 6: Watch for 24h**

Watch the Grafana dashboard from Task 19. No `checkpoint write failed` log lines should appear. If any do, investigate before proceeding to Task 22.

Roll back if anything looks wrong: set `STREAMING_CHECKPOINT_WRITE_ENABLED=false` in Terraform, apply.

---

## Task 23: Phase 3+4 — enable reads, one mode at a time

Operational task. Run only after Task 22 has been clean for 24h.

- [ ] **Step 1: Enable reads for prod**

Modify `infra/terraform/main.tf` to make `STREAMING_CHECKPOINT_READ_ENABLED` per-mode via the `pnl_consumer_sinks` local, or override at the env-var level for the prod service only.

Simplest implementation: in `locals.pnl_consumer_sinks`, add a `read_checkpoint` field; then in the `environment` block reference `each.value.read_checkpoint`:

```hcl
locals {
  pnl_consumer_sinks = {
    price = { ..., read_checkpoint = "false" }
    prod  = { ..., read_checkpoint = "true" }    # only prod for now
    real-trade = { ..., read_checkpoint = "false" }
    bt    = { ..., read_checkpoint = "false" }
  }
}

# In environment block:
{ name = "STREAMING_CHECKPOINT_READ_ENABLED", value = each.value.read_checkpoint },
```

Commit, push to main, wait for CI/CD to roll the prod service.

- [ ] **Step 2: Force a cold start on the prod service**

```bash
aws ecs update-service \
  --cluster trading-analysis \
  --service trading-analysis-pnl-consumer-prod \
  --force-new-deployment \
  --region ap-northeast-1
```

- [ ] **Step 3: Verify checkpoint-restored on cold start**

Within 5 min of the new task starting, check logs:

```bash
aws logs tail /ecs/trading-analysis-streaming \
  --since 10m \
  --filter-pattern '"cold-start"' \
  --region ap-northeast-1
```

Expected: a log line `cold-start: checkpoint restored. mode=prod strategies=N offset=M`. No `sanity check failed` or `falling back` lines for prod.

If a fallback occurred, examine the reason and resolve before continuing.

- [ ] **Step 4: Confirm no ClickHouse 48h-scan after restore**

Check ClickHouse query log for queries against `strategy_pnl_1min_prod_v2` within 30s of the prod task starting. With checkpoint restore, there should be at most one small query (the invariant check), not a full bootstrap.

- [ ] **Step 5: Watch 48h**

Same as Task 21 Step 6 — watch for any anomalies. PnL output should be unchanged relative to baseline.

- [ ] **Step 6: Enable reads for bt**

Repeat Steps 1–5 for the bt service. Set `read_checkpoint = "true"` in the bt sink local.

- [ ] **Step 7: Enable reads for real-trade**

Repeat for the real-trade service. Watch 72h (longer than the others). Additional verification: spot-check a few real-trade strategies in the logs for `should_apply_revision` decisions on a restored anchor — confirm late revisions are correctly accepted/rejected.

---

## Task 24: Phase 5 — cleanup

Run after all three modes have been on reads for at least a week without incident.

- [ ] **Step 1: Remove STREAMING_CHECKPOINT_READ_ENABLED special-case in code**

In `services/pnl_consumer/pnl_consumer/pnl_consumer.py`, the `_CHECKPOINT_READ_ENABLED` derivation can fold into `_CHECKPOINT_ENABLED`. The dedicated flag is no longer needed for rollout. Remove from the env-vars block:

```diff
-_CHECKPOINT_READ_ENABLED = (
-    _CHECKPOINT_ENABLED
-    and os.getenv("STREAMING_CHECKPOINT_READ_ENABLED", "true").lower() == "true"
-)
```

Update all references from `_CHECKPOINT_READ_ENABLED` to `_CHECKPOINT_ENABLED`.

In `infra/terraform/main.tf`, remove the `read_checkpoint` field from `pnl_consumer_sinks` and remove the env-var entry.

- [ ] **Step 2: Run unit tests**

```bash
pytest -m unit -v
```

Expected: still pass.

- [ ] **Step 3: Commit**

```bash
git add services/pnl_consumer/pnl_consumer/pnl_consumer.py infra/terraform/main.tf
git commit -m "pnl_consumer: collapse READ_ENABLED into ENABLED flag (post-rollout cleanup)"
```

- [ ] **Step 4: Update project CLAUDE.md**

Add a brief section to `CLAUDE.md` documenting the checkpoint feature:

```markdown
### Streaming PnL Consumer Checkpoint (Supabase Postgres)

`pnl_consumer` persists its `AnchorState` to Supabase Postgres after each successful ClickHouse flush. On cold start, the consumer reads the checkpoint, runs sanity checks (schema version, freshness, Kafka offset match, state-hash, optional ClickHouse invariant), and seeds AnchorState directly — skipping the 48h ClickHouse bootstrap.

Tables in Supabase schema `streaming`:
- `pnl_checkpoint` — per-(mode, strategy) state
- `pnl_commit_state` — per-mode atomic-commit anchor with state_hash + Kafka offset

Best-effort writes: Supabase outages never block Kafka commit. The next successful write self-heals the checkpoint. Stale checkpoints trigger fallback to the existing `_bootstrap_state` / `_bootstrap_bt_state` code.

Kill switch: `STREAMING_CHECKPOINT_ENABLED=false` disables both read and write paths; consumer reverts to today's bootstrap-only behavior.

Design doc: `docs/superpowers/specs/2026-05-26-pnl-consumer-checkpoint-design.md`
```

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: document Supabase checkpoint feature in project CLAUDE.md"
```

---

## Final notes

- **Periodic reconciliation** is explicitly out of scope here. Dagster will own it as a separate workstream — `libs/computation/checkpoint_store.read_checkpoint` is the API surface it will use.
- **The 48h bootstrap fallback path** (`_bootstrap_state` / `_bootstrap_bt_state`) is intentionally left untouched. It is our safety net and will remain in the codebase indefinitely.
- **`STREAMING_CHECKPOINT_ENABLED=false`** is the emergency kill switch at every phase. Total time from "decide to roll back" to "back on old behavior" is ~5 min (ECS rolling update).
