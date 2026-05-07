# Full Assets 3-Day Delete + Recompute Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change `pnl_prod_v2_full`, `pnl_real_trade_v2_full`, and `pnl_bt_v2_full` assets to pause the relevant ECS pnl-consumer service, delete the last 3 days from the target table, recompute only that window using anchors from before the window, then resume the consumer.

**Architecture:** Add `fetch_anchors(before_ts=)` param to scope anchor reads. Add `_process_underlying_recent` thread worker (load anchors → DELETE → recompute). Add `_recompute_pnl_recent` top-level orchestrator (pause ECS → fan-out threads → resume ECS in finally). Wire each `*_full` asset to call `_recompute_pnl_recent` with its service name.

**Tech Stack:** Python 3.11, Dagster, ClickHouse-connect, boto3 (ECS), pytest, unittest.mock

---

## File Map

| File | Change |
|---|---|
| `trading_dagster/utils/pnl_compute.py` | Add `before_ts` param to `fetch_anchors` |
| `trading_dagster/assets/pnl_strategy_v2.py` | Add `_pause_ecs_service`, `_resume_ecs_service`, `_process_underlying_recent`, `_recompute_pnl_recent`; update 3 asset functions |
| `tests/test_pnl_recent_recompute.py` | New test file for all new logic |

---

## Task 1: Add `before_ts` to `fetch_anchors`

**Files:**
- Modify: `trading_dagster/utils/pnl_compute.py:48-76`
- Test: `tests/test_pnl_recent_recompute.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_pnl_recent_recompute.py`:

```python
"""Unit tests for the 3-day recent recompute additions."""
import pytest
from unittest.mock import patch, MagicMock

from trading_dagster.utils.pnl_compute import fetch_anchors


class TestFetchAnchorsBeforeTs:

    @patch("trading_dagster.utils.pnl_compute.query_dicts")
    def test_no_before_ts_omits_time_filter(self, mock_qd):
        """Without before_ts, no ts filter is added to the SQL."""
        mock_qd.return_value = []
        fetch_anchors("strategy_pnl_1min_prod_v2", "btc")
        sql = mock_qd.call_args[0][0]
        assert "ts <" not in sql

    @patch("trading_dagster.utils.pnl_compute.query_dicts")
    def test_before_ts_adds_time_filter(self, mock_qd):
        """With before_ts, SQL must contain AND ts < toDateTime('...')."""
        mock_qd.return_value = []
        from datetime import datetime, UTC
        before = datetime(2026, 5, 3, 0, 0, 0, tzinfo=UTC)
        fetch_anchors("strategy_pnl_1min_prod_v2", "btc", before_ts=before)
        sql = mock_qd.call_args[0][0]
        assert "ts < toDateTime('2026-05-03 00:00:00')" in sql

    @patch("trading_dagster.utils.pnl_compute.query_dicts")
    def test_before_ts_returns_correct_anchors(self, mock_qd):
        """Rows returned by the filtered query are parsed into the anchor tuple."""
        mock_qd.return_value = [
            {"strategy_table_name": "s1", "anchor_pnl": 1.5, "anchor_price": 200.0, "anchor_position": 1.0}
        ]
        from datetime import datetime, UTC
        before = datetime(2026, 5, 3, tzinfo=UTC)
        result = fetch_anchors("strategy_pnl_1min_prod_v2", "btc", before_ts=before)
        assert result == {"s1": (1.5, 200.0, 1.0)}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/wzhang/Desktop/trading-analysis
source .venv/bin/activate
pytest tests/test_pnl_recent_recompute.py::TestFetchAnchorsBeforeTs -v
```

Expected: FAIL — `fetch_anchors() got an unexpected keyword argument 'before_ts'`

- [ ] **Step 3: Implement `before_ts` in `fetch_anchors`**

In `trading_dagster/utils/pnl_compute.py`, replace the `fetch_anchors` function (lines 48–76):

```python
def fetch_anchors(
    target_table: str, underlying: str, before_ts=None,
) -> Dict[str, Tuple[float, float, float]]:
    """Read last committed PnL per strategy from target.

    Returns: {strategy_table_name: (anchor_pnl, anchor_price, anchor_position)}
    before_ts: if provided, only rows with ts < before_ts are considered.
    """
    ts_filter = ""
    if before_ts is not None:
        ts_str = before_ts.strftime("%Y-%m-%d %H:%M:%S")
        ts_filter = f"  AND ts < toDateTime('{ts_str}')\n"
    sql = f"""\
SELECT
    strategy_table_name,
    cumulative_pnl AS anchor_pnl,
    price          AS anchor_price,
    position       AS anchor_position
FROM analytics.{target_table}
WHERE underlying = '{underlying}'
{ts_filter}ORDER BY strategy_table_name, ts DESC, updated_at DESC
LIMIT 1 BY strategy_table_name
"""
    rows = query_dicts(sql)
    result = {}
    for r in rows:
        pnl = float(r["anchor_pnl"]) if r["anchor_pnl"] is not None else 0.0
        price = float(r["anchor_price"]) if r["anchor_price"] is not None else 0.0
        pos = float(r["anchor_position"]) if r["anchor_position"] is not None else 0.0
        result[r["strategy_table_name"]] = (pnl, price, pos)
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_pnl_recent_recompute.py::TestFetchAnchorsBeforeTs -v
```

Expected: 3 PASSED

- [ ] **Step 5: Commit**

```bash
git add trading_dagster/utils/pnl_compute.py tests/test_pnl_recent_recompute.py
git commit -m "feat: add before_ts param to fetch_anchors for scoped anchor reads"
```

---

## Task 2: Add ECS pause/resume helpers

**Files:**
- Modify: `trading_dagster/assets/pnl_strategy_v2.py` (add after imports, before `_CHUNK_DAYS`)
- Test: `tests/test_pnl_recent_recompute.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_pnl_recent_recompute.py`:

```python
class TestEcsPauseResume:

    def test_pause_sets_desired_count_zero(self):
        """_pause_ecs_service calls update_service with desiredCount=0."""
        from trading_dagster.assets.pnl_strategy_v2 import _pause_ecs_service
        mock_client = MagicMock()
        mock_client.get_waiter.return_value = MagicMock()
        _pause_ecs_service("trading-analysis-pnl-consumer-prod", "trading-analysis", "ap-northeast-1", mock_client)
        mock_client.update_service.assert_called_once_with(
            cluster="trading-analysis",
            service="trading-analysis-pnl-consumer-prod",
            desiredCount=0,
        )

    def test_pause_waits_for_stable(self):
        """_pause_ecs_service waits using the services_stable waiter."""
        from trading_dagster.assets.pnl_strategy_v2 import _pause_ecs_service
        mock_client = MagicMock()
        waiter = MagicMock()
        mock_client.get_waiter.return_value = waiter
        _pause_ecs_service("trading-analysis-pnl-consumer-prod", "trading-analysis", "ap-northeast-1", mock_client)
        mock_client.get_waiter.assert_called_once_with("services_stable")
        waiter.wait.assert_called_once_with(
            cluster="trading-analysis",
            services=["trading-analysis-pnl-consumer-prod"],
        )

    def test_resume_sets_desired_count_one(self):
        """_resume_ecs_service calls update_service with desiredCount=1."""
        from trading_dagster.assets.pnl_strategy_v2 import _resume_ecs_service
        mock_client = MagicMock()
        _resume_ecs_service("trading-analysis-pnl-consumer-prod", "trading-analysis", "ap-northeast-1", mock_client)
        mock_client.update_service.assert_called_once_with(
            cluster="trading-analysis",
            service="trading-analysis-pnl-consumer-prod",
            desiredCount=1,
        )
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_pnl_recent_recompute.py::TestEcsPauseResume -v
```

Expected: FAIL — `cannot import name '_pause_ecs_service'`

- [ ] **Step 3: Add `import boto3` and the helpers to `pnl_strategy_v2.py`**

Add `import boto3` to the imports block at the top of `trading_dagster/assets/pnl_strategy_v2.py` (after `from datetime import UTC, datetime, timedelta`):

```python
import boto3
```

Add these two functions immediately before the `_CHUNK_DAYS` constant (around line 86):

```python
_ECS_CLUSTER = "trading-analysis"
_ECS_REGION = "ap-northeast-1"


def _pause_ecs_service(service_name: str, cluster: str, region: str, boto_client) -> None:
    boto_client.update_service(cluster=cluster, service=service_name, desiredCount=0)
    waiter = boto_client.get_waiter("services_stable")
    waiter.wait(cluster=cluster, services=[service_name])


def _resume_ecs_service(service_name: str, cluster: str, region: str, boto_client) -> None:
    boto_client.update_service(cluster=cluster, service=service_name, desiredCount=1)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_pnl_recent_recompute.py::TestEcsPauseResume -v
```

Expected: 3 PASSED

- [ ] **Step 5: Commit**

```bash
git add trading_dagster/assets/pnl_strategy_v2.py tests/test_pnl_recent_recompute.py
git commit -m "feat: add ECS pause/resume helpers for pnl consumer"
```

---

## Task 3: Add `_process_underlying_recent`

**Files:**
- Modify: `trading_dagster/assets/pnl_strategy_v2.py` (add after `_process_underlying`)
- Test: `tests/test_pnl_recent_recompute.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_pnl_recent_recompute.py`:

```python
class TestProcessUnderlyingRecent:

    def _make_bar(self, stn="strat_a", ts="2026-05-04 00:00:00"):
        return {
            "strategy_table_name": stn,
            "strategy_id": 1,
            "strategy_name": "Test",
            "underlying": "btc",
            "config_timeframe": "5m",
            "weighting": 1.0,
            "ts": ts,
            "position": 1.0,
            "bar_price": 100.0,
            "final_signal": 1.0,
            "bar_benchmark": 100.0,
        }

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2.execute")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_anchors_loaded_before_delete(self, mock_gc, mock_fa, mock_exec, mock_qd, mock_prices, mock_insert):
        """fetch_anchors must be called before the DELETE execute call."""
        from datetime import datetime, UTC, timedelta
        from trading_dagster.assets.pnl_strategy_v2 import _process_underlying_recent
        from trading_dagster.utils.pnl_compute import PROD_INSERT_COLUMNS

        call_order = []
        mock_fa.side_effect = lambda *a, **kw: call_order.append("anchor") or {}
        mock_exec.side_effect = lambda *a, **kw: call_order.append("delete")
        mock_qd.return_value = []
        mock_prices.return_value = {"btc": {}}
        mock_insert.return_value = 0

        window_start = datetime(2026, 5, 3, 0, 0, 0, tzinfo=UTC)
        end_dt = datetime(2026, 5, 6, 0, 0, 0, tzinfo=UTC)
        _process_underlying_recent(
            "btc", "strategy_pnl_1min_prod_v2", "strategy_output_history_v2",
            "production", PROD_INSERT_COLUMNS, "prod", window_start, end_dt,
        )
        assert call_order.index("anchor") < call_order.index("delete")

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2.execute")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_delete_uses_correct_table_and_window(self, mock_gc, mock_fa, mock_exec, mock_qd, mock_prices, mock_insert):
        """DELETE statement must reference the target table and window_start."""
        from datetime import datetime, UTC
        from trading_dagster.assets.pnl_strategy_v2 import _process_underlying_recent
        from trading_dagster.utils.pnl_compute import PROD_INSERT_COLUMNS

        mock_fa.return_value = {}
        mock_qd.return_value = []
        mock_prices.return_value = {"btc": {}}
        mock_insert.return_value = 0

        window_start = datetime(2026, 5, 3, 0, 0, 0, tzinfo=UTC)
        end_dt = datetime(2026, 5, 6, 0, 0, 0, tzinfo=UTC)
        _process_underlying_recent(
            "btc", "strategy_pnl_1min_prod_v2", "strategy_output_history_v2",
            "production", PROD_INSERT_COLUMNS, "prod", window_start, end_dt,
        )
        delete_sql = mock_exec.call_args[0][0]
        assert "strategy_pnl_1min_prod_v2" in delete_sql
        assert "btc" in delete_sql
        assert "2026-05-03 00:00:00" in delete_sql

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2.execute")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_recompute_only_covers_window(self, mock_gc, mock_fa, mock_exec, mock_qd, mock_prices, mock_insert):
        """Bars query must be scoped to [window_start, end_dt), not full history."""
        from datetime import datetime, UTC
        from trading_dagster.assets.pnl_strategy_v2 import _process_underlying_recent
        from trading_dagster.utils.pnl_compute import PROD_INSERT_COLUMNS

        mock_fa.return_value = {}
        mock_qd.return_value = [self._make_bar()]
        mock_prices.return_value = {"btc": {"2026-05-04 00:00:00": 100.0}}
        mock_insert.return_value = 5

        window_start = datetime(2026, 5, 3, 0, 0, 0, tzinfo=UTC)
        end_dt = datetime(2026, 5, 6, 0, 0, 0, tzinfo=UTC)
        _process_underlying_recent(
            "btc", "strategy_pnl_1min_prod_v2", "strategy_output_history_v2",
            "production", PROD_INSERT_COLUMNS, "prod", window_start, end_dt,
        )
        first_bar_sql = mock_qd.call_args_list[0][0][0]
        assert "2026-05-03 00:00:00" in first_bar_sql
        assert "strategy_output_history_v2" in first_bar_sql
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_pnl_recent_recompute.py::TestProcessUnderlyingRecent -v
```

Expected: FAIL — `cannot import name '_process_underlying_recent'`

- [ ] **Step 3: Implement `_process_underlying_recent`**

Add the following function to `trading_dagster/assets/pnl_strategy_v2.py`, immediately after the closing of `_process_underlying` (after line ~330, before `_recompute_pnl_full`):

```python
def _process_underlying_recent(
    underlying: str,
    target_table: str,
    source_table: str,
    label: str,
    insert_columns: list,
    mode: str,
    window_start: datetime,
    end_dt: datetime,
    log_fn=None,
) -> int:
    """Delete the last 3 days and recompute for one underlying. Returns rows inserted.

    Order: load anchors (before window) → DELETE → recompute [window_start, end_dt).
    Designed to run in a thread — owns its own ClickHouse client.
    """
    _emit = log_fn or _log.info
    client = get_client()
    window_start_ts = window_start.strftime("%Y-%m-%d %H:%M:%S")

    anchors = fetch_anchors(target_table, underlying, before_ts=window_start)
    _emit(f"[{underlying}] loaded {len(anchors)} anchors before {window_start_ts}")

    execute(
        f"ALTER TABLE analytics.{target_table} DELETE "
        f"WHERE underlying = '{underlying}' AND ts >= toDateTime('{window_start_ts}')",
        client=client,
    )
    _emit(f"[{underlying}] deleted rows >= {window_start_ts}")

    total_rows = 0
    chunk_count = math.ceil((end_dt - window_start).total_seconds() / 86400 / _CHUNK_DAYS)
    chunks_done = 0
    _emit(f"[{underlying}] recomputing {chunk_count} chunks from {window_start_ts}")

    chunk_start = window_start
    while chunk_start < end_dt:
        chunk_end = min(chunk_start + timedelta(days=_CHUNK_DAYS), end_dt)
        chunk_start_ts = chunk_start.strftime("%Y-%m-%d %H:%M:%S")
        chunk_end_ts = chunk_end.strftime("%Y-%m-%d %H:%M:%S")

        if mode == "prod":
            sql = f"""\
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe,
    argMin(weighting, revision_ts) AS weighting,
    toString(ts) AS ts,
    JSONExtractFloat(argMin(row_json, revision_ts), 'position') AS position,
    JSONExtractFloat(argMin(row_json, revision_ts), 'price') AS bar_price,
    JSONExtractFloat(argMin(row_json, revision_ts), 'final_signal') AS final_signal,
    JSONExtractFloat(argMin(row_json, revision_ts), 'benchmark') AS bar_benchmark
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(ts) >= toDateTime('{chunk_start_ts}') AND toDateTime(ts) < toDateTime('{chunk_end_ts}')
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
ORDER BY strategy_table_name, ts
"""
        elif mode == "bt":
            sql = f"""\
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe,
    argMin(weighting, revision_ts) AS weighting,
    toString(ts) AS ts,
    JSONExtractFloat(argMin(row_json, revision_ts), 'position') AS position,
    JSONExtractFloat(argMin(row_json, revision_ts), 'price') AS bar_price,
    JSONExtractFloat(argMin(row_json, revision_ts), 'final_signal') AS final_signal,
    JSONExtractFloat(argMin(row_json, revision_ts), 'benchmark') AS bar_benchmark,
    JSONExtractFloat(argMin(row_json, revision_ts), 'cumulative_pnl') AS cumulative_pnl
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(ts) >= toDateTime('{chunk_start_ts}') AND toDateTime(ts) < toDateTime('{chunk_end_ts}')
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
ORDER BY strategy_table_name, ts
"""
        else:  # real_trade
            tf_expr = "multiIf(config_timeframe = '5m', 5, config_timeframe = '10m', 10, config_timeframe = '15m', 15, config_timeframe = '30m', 30, config_timeframe = '1h', 60, config_timeframe = '4h', 240, config_timeframe = '1d', 1440, 5)"
            sql = f"""\
SELECT
    r.strategy_table_name,
    r.strategy_id, r.strategy_name, r.underlying, r.config_timeframe, r.weighting,
    toString(r.ts) AS ts,
    toString(r.ts + toIntervalMinute({tf_expr})) AS closing_ts,
    toString(toStartOfMinute(r.revision_ts + INTERVAL 59 SECOND)) AS execution_ts,
    toString(r.revision_ts) AS revision_ts,
    toString(nb.next_bar_ts + toIntervalMinute({tf_expr})) AS next_bar_closing_ts,
    JSONExtractFloat(r.row_json, 'position') AS position,
    JSONExtractFloat(r.row_json, 'price') AS bar_price,
    JSONExtractFloat(r.row_json, 'final_signal') AS final_signal,
    JSONExtractFloat(r.row_json, 'benchmark') AS bar_benchmark
FROM analytics.{source_table} r
LEFT JOIN (
    SELECT
        strategy_table_name,
        ts,
        leadInFrame(ts, 1, ts) OVER (
            PARTITION BY strategy_table_name ORDER BY ts
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_bar_ts
    FROM (
        SELECT strategy_table_name, ts
        FROM analytics.{source_table}
        WHERE underlying = '{underlying}'
          AND strategy_table_name NOT LIKE 'manual_probe%'
          AND toDateTime(ts) >= toDateTime('{chunk_start_ts}')
          AND toDateTime(ts) < toDateTime('{chunk_end_ts}')
        GROUP BY strategy_table_name, ts
    )
) nb ON r.strategy_table_name = nb.strategy_table_name AND r.ts = nb.ts
WHERE r.underlying = '{underlying}'
  AND r.strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(r.ts) >= toDateTime('{chunk_start_ts}') AND toDateTime(r.ts) < toDateTime('{chunk_end_ts}')
ORDER BY r.strategy_table_name, r.ts, r.revision_ts
"""

        raw_rows = query_dicts(sql, client)
        chunks_done += 1

        if mode == "real_trade":
            rows_dict = [
                {
                    "strategy_table_name": r["strategy_table_name"],
                    "strategy_id": int(r["strategy_id"]),
                    "strategy_name": r["strategy_name"],
                    "underlying": r["underlying"],
                    "config_timeframe": r["config_timeframe"],
                    "weighting": float(r["weighting"]),
                    "ts": str(r["ts"]),
                    "closing_ts": str(r["closing_ts"]),
                    "execution_ts": str(r["execution_ts"]),
                    "revision_ts": str(r["revision_ts"]),
                    "next_bar_closing_ts": str(r["next_bar_closing_ts"]),
                    "position": float(r["position"]),
                    "bar_price": float(r["bar_price"]),
                    "final_signal": float(r["final_signal"]),
                    "bar_benchmark": float(r["bar_benchmark"]),
                }
                for r in raw_rows
            ]
        else:
            rows_dict = raw_rows

        if not rows_dict:
            chunk_start = chunk_end
            continue

        all_prices = fetch_prices_multi(
            underlyings=[underlying],
            ts_min=chunk_start_ts,
            ts_max=chunk_end_ts,
            client=client,
            extend_minutes=0,
        )
        prices = all_prices.pop(underlying, {})

        if mode == "prod":
            for _stn, strategy_rows in iter_compute_prod_pnl(rows_dict, anchors, prices, source_label=label):
                _prepare_rows_for_clickhouse(strategy_rows)
                n = insert_rows(f"analytics.{target_table}", insert_columns, strategy_rows, client)
                total_rows += n
                if strategy_rows:
                    last = strategy_rows[-1]
                    anchors[_stn] = (float(last[8]), float(last[11]), float(last[10]))
        elif mode == "bt":
            for bar in rows_dict:
                tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
                bar["execution_ts"] = (
                    _parse_ts(bar["ts"]) + timedelta(minutes=tf_minutes)
                ).strftime("%Y-%m-%d %H:%M:%S")
            by_stn: dict[str, list] = defaultdict(list)
            for bar in rows_dict:
                by_stn[bar["strategy_table_name"]].append(bar)
            for _stn, stn_bars in by_stn.items():
                strategy_rows = compute_bt_pnl(stn_bars, prices, anchors=anchors)
                _prepare_rows_for_clickhouse(strategy_rows)
                n = insert_rows(f"analytics.{target_table}", insert_columns, strategy_rows, client)
                total_rows += n
                if strategy_rows:
                    last = strategy_rows[-1]
                    anchors[_stn] = (float(last[8]), float(last[11]), float(last[10]))
        elif mode == "real_trade":
            by_strategy: dict[str, list] = defaultdict(list)
            for bar in rows_dict:
                by_strategy[bar["strategy_table_name"]].append(bar)
            for stn, strategy_bars in by_strategy.items():
                strategy_rows = compute_real_trade_pnl(strategy_bars, anchors, prices)
                _prepare_rows_for_clickhouse(strategy_rows)
                n = insert_rows(f"analytics.{target_table}", insert_columns, strategy_rows, client)
                total_rows += n
                if strategy_rows:
                    last = strategy_rows[-1]
                    anchors[stn] = (float(last[8]), float(last[11]), float(last[10]))
        else:
            raise ValueError(f"Unknown mode: {mode!r}")

        del rows_dict, prices
        chunk_start = chunk_end

    _emit(f"[{underlying}] recent recompute complete: {total_rows:,} rows inserted")
    return total_rows
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_pnl_recent_recompute.py::TestProcessUnderlyingRecent -v
```

Expected: 3 PASSED

- [ ] **Step 5: Commit**

```bash
git add trading_dagster/assets/pnl_strategy_v2.py tests/test_pnl_recent_recompute.py
git commit -m "feat: add _process_underlying_recent with delete+recompute window logic"
```

---

## Task 4: Add `_recompute_pnl_recent` and wire up assets

**Files:**
- Modify: `trading_dagster/assets/pnl_strategy_v2.py`
- Test: `tests/test_pnl_recent_recompute.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_pnl_recent_recompute.py`:

```python
class TestRecomputePnlRecent:

    def _make_bar(self, stn="strat_a", ts="2026-05-04 00:00:00"):
        return {
            "strategy_table_name": stn,
            "strategy_id": 1,
            "strategy_name": "Test",
            "underlying": "btc",
            "config_timeframe": "5m",
            "weighting": 1.0,
            "ts": ts,
            "position": 1.0,
            "bar_price": 100.0,
            "final_signal": 1.0,
            "bar_benchmark": 100.0,
        }

    def _make_context(self):
        ctx = MagicMock()
        ctx.log = MagicMock()
        ctx.log.info = MagicMock()
        return ctx

    @patch("trading_dagster.assets.pnl_strategy_v2.boto3")
    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2.execute")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_consumer_paused_before_recompute(
        self, mock_gc, mock_get_und, mock_fa, mock_exec, mock_qd, mock_prices, mock_insert, mock_boto3
    ):
        """ECS update_service(desiredCount=0) must be called before any DELETE or INSERT."""
        from trading_dagster.assets.pnl_strategy_v2 import _recompute_pnl_recent
        from trading_dagster.utils.pnl_compute import PROD_INSERT_COLUMNS

        call_order = []
        mock_ecs = MagicMock()
        mock_ecs.get_waiter.return_value = MagicMock()
        mock_boto3.client.return_value = mock_ecs
        mock_ecs.update_service.side_effect = lambda **kw: call_order.append(("ecs", kw.get("desiredCount")))
        mock_get_und.return_value = ["btc"]
        mock_fa.return_value = {}
        mock_exec.side_effect = lambda *a, **kw: call_order.append("delete")
        mock_qd.return_value = []
        mock_prices.return_value = {"btc": {}}
        mock_insert.return_value = 0

        _recompute_pnl_recent(
            self._make_context(),
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            label="production",
            insert_columns=PROD_INSERT_COLUMNS,
            mode="prod",
            ecs_service="trading-analysis-pnl-consumer-prod",
        )

        pause_idx = next(i for i, x in enumerate(call_order) if x == ("ecs", 0))
        delete_idx = call_order.index("delete")
        assert pause_idx < delete_idx

    @patch("trading_dagster.assets.pnl_strategy_v2.boto3")
    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2.execute")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_consumer_resumed_even_on_failure(
        self, mock_gc, mock_get_und, mock_fa, mock_exec, mock_qd, mock_prices, mock_insert, mock_boto3
    ):
        """ECS update_service(desiredCount=1) must be called even when recompute raises."""
        from trading_dagster.assets.pnl_strategy_v2 import _recompute_pnl_recent
        from trading_dagster.utils.pnl_compute import PROD_INSERT_COLUMNS

        mock_ecs = MagicMock()
        mock_ecs.get_waiter.return_value = MagicMock()
        mock_boto3.client.return_value = mock_ecs
        mock_get_und.return_value = ["btc"]
        mock_fa.return_value = {}
        mock_exec.side_effect = RuntimeError("ClickHouse unavailable")
        mock_qd.return_value = []
        mock_prices.return_value = {"btc": {}}
        mock_insert.return_value = 0

        with pytest.raises(RuntimeError, match="ClickHouse unavailable"):
            _recompute_pnl_recent(
                self._make_context(),
                target_table="strategy_pnl_1min_prod_v2",
                source_table="strategy_output_history_v2",
                label="production",
                insert_columns=PROD_INSERT_COLUMNS,
                mode="prod",
                ecs_service="trading-analysis-pnl-consumer-prod",
            )

        resume_calls = [c for c in mock_ecs.update_service.call_args_list if c.kwargs.get("desiredCount") == 1]
        assert len(resume_calls) == 1

    @patch("trading_dagster.assets.pnl_strategy_v2.boto3")
    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2.execute")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_window_start_is_3_days_ago_midnight(
        self, mock_gc, mock_get_und, mock_fa, mock_exec, mock_qd, mock_prices, mock_insert, mock_boto3
    ):
        """window_start passed to fetch_anchors must be (today - 3 days) at UTC midnight."""
        from datetime import datetime, UTC, timedelta
        from trading_dagster.assets.pnl_strategy_v2 import _recompute_pnl_recent
        from trading_dagster.utils.pnl_compute import PROD_INSERT_COLUMNS

        mock_ecs = MagicMock()
        mock_ecs.get_waiter.return_value = MagicMock()
        mock_boto3.client.return_value = mock_ecs
        mock_get_und.return_value = ["btc"]
        mock_fa.return_value = {}
        mock_qd.return_value = []
        mock_prices.return_value = {"btc": {}}
        mock_insert.return_value = 0

        _recompute_pnl_recent(
            self._make_context(),
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            label="production",
            insert_columns=PROD_INSERT_COLUMNS,
            mode="prod",
            ecs_service="trading-analysis-pnl-consumer-prod",
        )

        expected_window = (
            datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
            - timedelta(days=3)
        )
        actual_before_ts = mock_fa.call_args.kwargs.get("before_ts") or mock_fa.call_args[1].get("before_ts")
        assert actual_before_ts.date() == expected_window.date()
        assert actual_before_ts.hour == 0
        assert actual_before_ts.minute == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_pnl_recent_recompute.py::TestRecomputePnlRecent -v
```

Expected: FAIL — `cannot import name '_recompute_pnl_recent'`

- [ ] **Step 3: Implement `_recompute_pnl_recent`**

Add the following function to `trading_dagster/assets/pnl_strategy_v2.py`, immediately after `_recompute_pnl_full`:

```python
def _recompute_pnl_recent(
    context: AssetExecutionContext,
    target_table: str,
    source_table: str,
    label: str,
    insert_columns: list,
    mode: str,
    ecs_service: str,
) -> MaterializeResult:
    """Delete last 3 days from target and recompute, seeded from anchors before the window.

    Pauses the named ECS consumer service before any writes and resumes it in a finally
    block so it always restarts even if the recompute fails.
    """
    window_start = (
        datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        - timedelta(days=3)
    )
    end_dt = datetime.now(tz=UTC)

    underlyings = _get_underlyings(source_table)
    context.log.info(
        f"Recent recompute {label}: {len(underlyings)} underlyings, "
        f"{window_start.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d %H:%M:%S')}, "
        f"ecs_service={ecs_service}"
    )

    ecs = boto3.client("ecs", region_name=_ECS_REGION)
    _pause_ecs_service(ecs_service, _ECS_CLUSTER, _ECS_REGION, ecs)
    context.log.info(f"Paused ECS service {ecs_service}")

    total_rows = 0
    try:
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            futures = {
                pool.submit(
                    _process_underlying_recent,
                    underlying,
                    target_table,
                    source_table,
                    label,
                    insert_columns,
                    mode,
                    window_start,
                    end_dt,
                    context.log.info,
                ): underlying
                for underlying in underlyings
            }
            for future in as_completed(futures):
                underlying = futures[future]
                rows = future.result()
                total_rows += rows
                context.log.info(f"[{underlying}] complete: {rows:,} rows inserted")
    finally:
        _resume_ecs_service(ecs_service, _ECS_CLUSTER, _ECS_REGION, ecs)
        context.log.info(f"Resumed ECS service {ecs_service}")

    context.log.info(f"Recent recompute {label} complete: {total_rows:,} total rows inserted")
    return MaterializeResult(metadata={
        "rows_inserted": total_rows,
        "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_ts": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    })
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_pnl_recent_recompute.py::TestRecomputePnlRecent -v
```

Expected: 3 PASSED

- [ ] **Step 5: Update the three `*_full` asset functions**

In `trading_dagster/assets/pnl_strategy_v2.py`, replace the body of each `*_full` asset:

**`pnl_prod_v2_full_asset`** — replace docstring and body:
```python
def pnl_prod_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Delete last 3 days and recompute prod PnL from anchors, pausing the prod consumer."""
    return _recompute_pnl_recent(
        context,
        target_table="strategy_pnl_1min_prod_v2",
        source_table="strategy_output_history_v2",
        label="production",
        insert_columns=PROD_INSERT_COLUMNS,
        mode="prod",
        ecs_service="trading-analysis-pnl-consumer-prod",
    )
```

**`pnl_real_trade_v2_full_asset`** — replace docstring and body:
```python
def pnl_real_trade_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Delete last 3 days and recompute real_trade PnL from anchors, pausing the real-trade consumer."""
    return _recompute_pnl_recent(
        context,
        target_table="strategy_pnl_1min_real_trade_v2",
        source_table="strategy_output_history_v2",
        label="real_trade",
        insert_columns=REAL_TRADE_INSERT_COLUMNS,
        mode="real_trade",
        ecs_service="trading-analysis-pnl-consumer-real-trade",
    )
```

**`pnl_bt_v2_full_asset`** — replace docstring and body:
```python
def pnl_bt_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Delete last 3 days and recompute BT PnL from anchors. bt consumer is already stopped (desired_count=0)."""
    return _recompute_pnl_recent(
        context,
        target_table="strategy_pnl_1min_bt_v2",
        source_table="strategy_output_history_bt_v2",
        label="backtest",
        insert_columns=PROD_INSERT_COLUMNS,
        mode="bt",
        ecs_service="trading-analysis-pnl-consumer-bt",
    )
```

- [ ] **Step 6: Run all new tests**

```bash
pytest tests/test_pnl_recent_recompute.py -v
```

Expected: All 12 tests PASSED

- [ ] **Step 7: Run full test suite to check no regressions**

```bash
pytest tests/ -m "not integration and not streaming_integration" -v
```

Expected: All tests PASSED (same count as before this feature)

- [ ] **Step 8: Commit**

```bash
git add trading_dagster/assets/pnl_strategy_v2.py tests/test_pnl_recent_recompute.py
git commit -m "feat: wire *_full assets to 3-day delete+recompute with ECS pause/resume"
```
