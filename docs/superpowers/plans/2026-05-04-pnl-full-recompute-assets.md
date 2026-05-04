# PnL Full Recompute Assets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two unpartitioned Dagster assets (`pnl_prod_v2_full`, `pnl_real_trade_v2_full`) that recompute PnL sequentially from the start date to now in a single long-running job, eliminating the implicit inter-partition anchor dependency.

**Architecture:** Each asset calls a shared `_recompute_pnl_full()` function that iterates all strategy bars chronologically per underlying, chains anchors in memory, and batch-inserts rows into ClickHouse. `ReplacingMergeTree(updated_at)` handles deduplication on reruns — no DELETE needed. Existing partitioned assets are untouched.

**Tech Stack:** Python 3.11, Dagster asset decorators, ClickHouse-connect via `trading_dagster/utils/clickhouse_client.py`, existing `pnl_compute.py` utilities.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `trading_dagster/assets/pnl_strategy_v2.py` | Modify | Add `_recompute_pnl_full()`, `pnl_prod_v2_full_asset`, `pnl_real_trade_v2_full_asset` |
| `trading_dagster/definitions/__init__.py` | Modify | Register the two new assets |
| `tests/test_pnl_full_recompute.py` | Create | Unit tests for `_recompute_pnl_full()` logic (mocked ClickHouse) |

---

## Task 1: Write failing tests for `_recompute_pnl_full` prod path

**Files:**
- Create: `tests/test_pnl_full_recompute.py`

- [ ] **Step 1: Create the test file**

```python
# tests/test_pnl_full_recompute.py
"""
Unit tests for _recompute_pnl_full — the shared compute function used by
pnl_prod_v2_full_asset and pnl_real_trade_v2_full_asset.

All ClickHouse calls are mocked. Tests verify:
- bars fetched with correct time range (start_date to now)
- rows inserted to correct target table
- correct columns used for prod vs real_trade
- progress logging occurs per batch
- empty underlying (no bars) is skipped gracefully
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

# Import will fail until Task 2 adds the function — that's expected.
from trading_dagster.assets.pnl_strategy_v2 import _recompute_pnl_full
from trading_dagster.utils.pnl_compute import (
    PROD_INSERT_COLUMNS,
    REAL_TRADE_INSERT_COLUMNS,
    PROD_REAL_TRADE_START_DATE,
)


def _make_context():
    ctx = MagicMock()
    ctx.log = MagicMock()
    ctx.log.info = MagicMock()
    return ctx


def _make_bar(stn="strat_a", ts="2026-02-27 00:00:00", tf="5m", pos=1.0):
    return {
        "strategy_table_name": stn,
        "strategy_id": 1,
        "strategy_name": "Test",
        "underlying": "btc",
        "config_timeframe": tf,
        "weighting": 1.0,
        "ts": ts,
        "position": pos,
        "bar_price": 100.0,
        "final_signal": 1.0,
        "bar_benchmark": 100.0,
    }


def _make_rt_bar(stn="strat_a", ts="2026-02-27 00:00:00", tf="5m", pos=1.0):
    return {
        **_make_bar(stn, ts, tf, pos),
        "closing_ts": "2026-02-27 00:05:00",
        "execution_ts": "2026-02-27 00:01:00",
    }


class TestRecomputePnlFullProd:

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_prod_fetches_bars_for_full_range(
        self, mock_client, mock_get_und, mock_qd, mock_anchors, mock_prices, mock_insert
    ):
        """Bars query must cover PROD_REAL_TRADE_START_DATE to now for all underlyings."""
        mock_get_und.return_value = ["btc"]
        mock_qd.return_value = [_make_bar()]
        mock_anchors.return_value = {}
        mock_prices.return_value = {"btc": {"2026-02-27 00:00:00": 100.0}}
        mock_insert.return_value = 5

        ctx = _make_context()
        result = _recompute_pnl_full(
            ctx,
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            label="production",
            insert_columns=PROD_INSERT_COLUMNS,
            mode="prod",
        )

        # bars query must include start boundary
        call_args = mock_qd.call_args[0][0]
        assert PROD_REAL_TRADE_START_DATE in call_args
        assert "strategy_output_history_v2" in call_args
        assert "btc" in call_args

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_prod_inserts_to_correct_table(
        self, mock_client, mock_get_und, mock_qd, mock_anchors, mock_prices, mock_insert
    ):
        """Rows must be inserted into the target table with PROD_INSERT_COLUMNS."""
        mock_get_und.return_value = ["btc"]
        mock_qd.return_value = [_make_bar()]
        mock_anchors.return_value = {}
        mock_prices.return_value = {"btc": {"2026-02-27 00:00:00": 100.0}}
        mock_insert.return_value = 5

        ctx = _make_context()
        _recompute_pnl_full(
            ctx,
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            label="production",
            insert_columns=PROD_INSERT_COLUMNS,
            mode="prod",
        )

        assert mock_insert.called
        table_arg = mock_insert.call_args[0][0]
        cols_arg = mock_insert.call_args[0][1]
        assert table_arg == "analytics.strategy_pnl_1min_prod_v2"
        assert cols_arg == PROD_INSERT_COLUMNS

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_empty_underlying_skipped_gracefully(
        self, mock_client, mock_get_und, mock_qd, mock_anchors, mock_prices, mock_insert
    ):
        """If source table has no bars for an underlying, insert is not called."""
        mock_get_und.return_value = ["btc"]
        mock_qd.return_value = []  # no bars
        mock_anchors.return_value = {}
        mock_prices.return_value = {"btc": {}}
        mock_insert.return_value = 0

        ctx = _make_context()
        result = _recompute_pnl_full(
            ctx,
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            label="production",
            insert_columns=PROD_INSERT_COLUMNS,
            mode="prod",
        )

        mock_insert.assert_not_called()
        assert result.metadata["rows_inserted"] == 0

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_real_trade_uses_real_trade_columns(
        self, mock_client, mock_get_und, mock_qd, mock_anchors, mock_prices, mock_insert
    ):
        """real_trade mode must insert with REAL_TRADE_INSERT_COLUMNS."""
        mock_get_und.return_value = ["btc"]
        mock_qd.return_value = [_make_rt_bar()]
        mock_anchors.return_value = {}
        mock_prices.return_value = {"btc": {"2026-02-27 00:00:00": 100.0}}
        mock_insert.return_value = 5

        ctx = _make_context()
        _recompute_pnl_full(
            ctx,
            target_table="strategy_pnl_1min_real_trade_v2",
            source_table="strategy_output_history_v2",
            label="real_trade",
            insert_columns=REAL_TRADE_INSERT_COLUMNS,
            mode="real_trade",
        )

        cols_arg = mock_insert.call_args[0][1]
        assert cols_arg == REAL_TRADE_INSERT_COLUMNS

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_progress_logged_per_underlying(
        self, mock_client, mock_get_und, mock_qd, mock_anchors, mock_prices, mock_insert
    ):
        """context.log.info must be called at least once per underlying processed."""
        mock_get_und.return_value = ["btc", "eth"]
        mock_qd.return_value = [_make_bar()]
        mock_anchors.return_value = {}
        mock_prices.return_value = {
            "btc": {"2026-02-27 00:00:00": 100.0},
            "eth": {"2026-02-27 00:00:00": 50.0},
        }
        mock_insert.return_value = 5

        ctx = _make_context()
        _recompute_pnl_full(
            ctx,
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            label="production",
            insert_columns=PROD_INSERT_COLUMNS,
            mode="prod",
        )

        assert ctx.log.info.call_count >= 2
```

- [ ] **Step 2: Run tests to verify they fail (import error expected)**

```bash
cd /Users/wzhang/Desktop/trading-analysis
source .venv/bin/activate
pytest tests/test_pnl_full_recompute.py -v 2>&1 | head -30
```

Expected: `ImportError: cannot import name '_recompute_pnl_full'`

---

## Task 2: Implement `_recompute_pnl_full` and the two new assets

**Files:**
- Modify: `trading_dagster/assets/pnl_strategy_v2.py`

- [ ] **Step 1: Add the imports needed at the top of `pnl_strategy_v2.py`**

The existing imports already cover everything needed. Verify `query_dicts` is imported (it is, line 23). No new imports required.

- [ ] **Step 2: Add `_recompute_pnl_full` after `_prepare_rows_for_clickhouse` (around line 73)**

Insert the following function before the `# 1. Production PnL` section comment:

```python
# ─────────────────────────────────────────────────────────────────────────────
# Full Recompute Logic (unpartitioned, sequential anchor chain)
# ─────────────────────────────────────────────────────────────────────────────

_FULL_RECOMPUTE_BATCH_ROWS = 200_000


def _recompute_pnl_full(context, target_table: str, source_table: str, label: str, insert_columns: list, mode: str):
    """Recompute PnL for all underlyings from PROD_REAL_TRADE_START_DATE to now.

    mode: "prod" uses iter_compute_prod_pnl; "real_trade" uses compute_real_trade_pnl.
    Rows are re-inserted with a fresh updated_at; ReplacingMergeTree deduplicates.
    No DELETE — safe to rerun alongside pnl_consumer.
    """
    from dagster import MaterializeResult

    start_ts = f"{PROD_REAL_TRADE_START_DATE} 00:00:00"
    end_dt = datetime.now(tz=timezone.utc)
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    client = get_client()
    underlyings = _get_underlyings(source_table)
    context.log.info(f"Full recompute {label}: {len(underlyings)} underlyings, {start_ts} → {end_ts}")

    total_rows = 0

    for underlying in underlyings:
        context.log.info(f"Processing underlying: {underlying}")

        # Fetch all strategy bars for this underlying across the full time range
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
  AND ts >= toDateTime('{start_ts}') AND ts < toDateTime('{end_ts}')
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
ORDER BY strategy_table_name, ts
"""
        else:  # real_trade
            sql = f"""\
SELECT
    strategy_table_name,
    strategy_id, strategy_name, underlying, config_timeframe, weighting,
    toString(ts) AS ts,
    toString(ts + toIntervalMinute(multiIf(
        config_timeframe = '5m', 5, config_timeframe = '10m', 10,
        config_timeframe = '15m', 15, config_timeframe = '30m', 30,
        config_timeframe = '1h', 60, config_timeframe = '4h', 240,
        config_timeframe = '1d', 1440, 5
    ))) AS closing_ts,
    toString(toStartOfMinute(revision_ts + INTERVAL 59 SECOND)) AS execution_ts,
    JSONExtractFloat(row_json, 'position') AS position,
    JSONExtractFloat(row_json, 'price') AS bar_price,
    JSONExtractFloat(row_json, 'final_signal') AS final_signal,
    JSONExtractFloat(row_json, 'benchmark') AS bar_benchmark
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(ts) >= toDateTime('{start_ts}') AND toDateTime(ts) < toDateTime('{end_ts}')
ORDER BY strategy_table_name, toDateTime(ts), revision_ts
"""

        rows_dict = query_dicts(sql, client)
        if not rows_dict:
            context.log.info(f"  No bars for {underlying}, skipping.")
            continue

        # Prices for the full range — extend_minutes=0 because end_ts is already exclusive
        all_prices = fetch_prices_multi(underlyings=[underlying], ts_min=start_ts, ts_max=end_ts, client=client, extend_minutes=0)
        prices = all_prices.get(underlying, {})

        # Bootstrap from whatever is already in the target (empty dict on clean table)
        anchors = fetch_anchors(target_table, underlying)

        if mode == "prod":
            for _stn, strategy_rows in iter_compute_prod_pnl(rows_dict, anchors, prices, source_label=label):
                _prepare_rows_for_clickhouse(strategy_rows)
                n = insert_rows(f"analytics.{target_table}", insert_columns, strategy_rows, client)
                total_rows += n
                context.log.info(f"  [{underlying}] inserted {n} rows for strategy {_stn}")
        else:
            all_rows = compute_real_trade_pnl(rows_dict, anchors, prices)
            _prepare_rows_for_clickhouse(all_rows)
            n = insert_rows(f"analytics.{target_table}", insert_columns, all_rows, client)
            total_rows += n
            context.log.info(f"  [{underlying}] inserted {n} real_trade rows")

        del rows_dict, prices, all_prices

    context.log.info(f"Full recompute {label} complete: {total_rows} total rows inserted")
    return MaterializeResult(metadata={"rows_inserted": total_rows, "start_ts": start_ts, "end_ts": end_ts})
```

- [ ] **Step 3: Add the two new asset definitions after the real trade asset (after line ~121)**

Add after `pnl_real_trade_v2_daily_asset` definition:

```python
# ─────────────────────────────────────────────────────────────────────────────
# 4. Production PnL (Full Recompute — unpartitioned, no timeout)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_prod_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
)
def pnl_prod_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Full sequential recompute of production PnL from start date to now.

    Processes all underlyings chronologically in a single long-running job.
    Anchor chain is maintained in memory — no parallel partition race conditions.
    Trigger manually from the Dagster UI when a full recompute is needed.
    """
    return _recompute_pnl_full(
        context,
        target_table="strategy_pnl_1min_prod_v2",
        source_table="strategy_output_history_v2",
        label="production",
        insert_columns=PROD_INSERT_COLUMNS,
        mode="prod",
    )


# ─────────────────────────────────────────────────────────────────────────────
# 5. Real Trade PnL (Full Recompute — unpartitioned, no timeout)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_real_trade_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
)
def pnl_real_trade_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Full sequential recompute of real_trade PnL from start date to now.

    Processes all underlyings chronologically in a single long-running job.
    Anchor chain is maintained in memory — no parallel partition race conditions.
    Trigger manually from the Dagster UI when a full recompute is needed.
    """
    return _recompute_pnl_full(
        context,
        target_table="strategy_pnl_1min_real_trade_v2",
        source_table="strategy_output_history_v2",
        label="real_trade",
        insert_columns=REAL_TRADE_INSERT_COLUMNS,
        mode="real_trade",
    )
```

- [ ] **Step 4: Run the tests — they should now pass**

```bash
cd /Users/wzhang/Desktop/trading-analysis
source .venv/bin/activate
pytest tests/test_pnl_full_recompute.py -v
```

Expected: all 5 tests PASS.

- [ ] **Step 5: Run the full test suite to check for regressions**

```bash
pytest -m unit -v
```

Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add trading_dagster/assets/pnl_strategy_v2.py tests/test_pnl_full_recompute.py
git commit -m "feat: add pnl_prod_v2_full and pnl_real_trade_v2_full unpartitioned recompute assets"
```

---

## Task 3: Register the new assets in Definitions

**Files:**
- Modify: `trading_dagster/definitions/__init__.py`

- [ ] **Step 1: Add imports for the two new assets**

In `trading_dagster/definitions/__init__.py`, update the import from `pnl_strategy_v2`:

```python
from ..assets.pnl_strategy_v2 import (
    pnl_prod_v2_daily_asset,
    pnl_bt_v2_daily_asset,
    pnl_real_trade_v2_daily_asset,
    pnl_prod_v2_full_asset,
    pnl_real_trade_v2_full_asset,
)
```

- [ ] **Step 2: Add the new assets to `all_assets`**

Update the `all_assets` list:

```python
all_assets = [
    # Market data (backfill only — real-time via pnl_consumer)
    binance_futures_backfill_asset,
    # Strategy PnL v2 (Daily Backfills)
    pnl_prod_v2_daily_asset,
    pnl_bt_v2_daily_asset,
    pnl_real_trade_v2_daily_asset,
    # Strategy PnL v2 (Full Recompute — manual trigger)
    pnl_prod_v2_full_asset,
    pnl_real_trade_v2_full_asset,
    # Rollups & Scans
    pnl_1hour_prod_rollup_asset,
    pnl_1hour_real_trade_rollup_asset,
    pnl_1hour_bt_rollup_asset,
    # Infra checks
    clickhouse_connectivity_check_asset,
    postgres_cleanup_asset,
]
```

- [ ] **Step 3: Verify Dagster loads the definitions without error**

```bash
cd /Users/wzhang/Desktop/trading-analysis
source .venv/bin/activate
python -c "from trading_dagster.definitions import defs; print('Assets:', [a.key.to_user_string() for a in defs.assets])"
```

Expected output includes `pnl_prod_v2_full` and `pnl_real_trade_v2_full` in the list.

- [ ] **Step 4: Run full test suite one more time**

```bash
pytest -m unit -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add trading_dagster/definitions/__init__.py
git commit -m "feat: register pnl_prod_v2_full and pnl_real_trade_v2_full in Dagster definitions"
```

---

## Self-Review Notes

**Spec coverage check:**
- ✅ `pnl_prod_v2_full` asset — Task 2
- ✅ `pnl_real_trade_v2_full` asset — Task 2
- ✅ No `op_tags` timeout — Task 2 (decorators have no `op_tags`)
- ✅ No `partitions_def` — Task 2 (decorators have no `partitions_def`)
- ✅ No `AutomationCondition` — Task 2 (decorators have no `automation_condition`)
- ✅ `deps=["binance_futures_backfill"]` — Task 2
- ✅ Sequential bar processing from `PROD_REAL_TRADE_START_DATE` — `_recompute_pnl_full` SQL `ORDER BY ts`
- ✅ `fetch_anchors()` bootstrap — Task 2
- ✅ `iter_compute_prod_pnl()` for prod — Task 2
- ✅ `compute_real_trade_pnl()` for real_trade — Task 2
- ✅ No DELETE, ReplacingMergeTree handles deduplication — Task 2 (no execute DELETE call)
- ✅ `_prepare_rows_for_clickhouse()` called before insert — Task 2
- ✅ Progress logging per underlying — Task 2 (`context.log.info`)
- ✅ Existing assets untouched — Tasks only add new code
- ✅ Registration in definitions — Task 3
- ✅ Tests — Task 1

**One important note:** `_recompute_pnl_full` calls `fetch_prices_multi` with `underlyings=[underlying]` (a single-element list) inside the per-underlying loop. This fires one ClickHouse query per underlying for the full date range. For 8 underlyings × 60+ days of 1-minute price data this is ~700k rows per underlying. ClickHouse Cloud handles this fine, but if memory pressure is observed, the `del all_prices` at the end of each underlying loop already releases the dict.
