"""
Unit tests for PnL computation logic.

Tests the anchor-chained PnL computation without ClickHouse dependency.
"""

import re
import pytest
from unittest.mock import patch

from libs.computation import (
    compute_prod_pnl,
    compute_real_trade_pnl,
    TIMEFRAME_MAP,
)
from libs.computation.fetch_bars import fetch_new_bars_real_trade
from libs.computation.fetch_prices import fetch_prices_multi

PROD_REAL_TRADE_START_DATE = "2026-02-27"


class TestComputeProdPnl:
    """Test compute_prod_pnl anchor chaining logic."""

    def test_single_bar_cold_start_no_anchor(self):
        """First bar with no anchor and position=0 — produces 5 rows with price from source."""
        bars = [
            {
                "strategy_table_name": "test_strategy",
                "strategy_instance_id": "test_strategy__1",
                "strategy_id": 1,
                "strategy_name": "Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": f"{PROD_REAL_TRADE_START_DATE} 00:00:00",
                "position": 1.0,
                "final_signal": 1.0,
                "bar_benchmark": 100.0,
            }
        ]
        anchors = {}
        prices = {f"{PROD_REAL_TRADE_START_DATE} 00:0{5+i}:00": 100.0 for i in range(5)}
        rows = compute_prod_pnl(bars, anchors, prices, source_label="production")
        assert len(rows) == 5
        for row in rows:
            assert row[11] == 100.0  # price column

    def test_anchor_chaining_two_bars(self):
        """Two consecutive bars should chain PnL correctly."""
        bars = [
            {
                "strategy_table_name": "test_strategy",
                "strategy_instance_id": "test_strategy__1",
                "strategy_id": 1,
                "strategy_name": "Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": "2024-01-01 00:00:00",
                "position": 1.0,
                "final_signal": 1.0,
                "bar_benchmark": 100.0,
            },
            {
                "strategy_table_name": "test_strategy",
                "strategy_instance_id": "test_strategy__1",
                "strategy_id": 1,
                "strategy_name": "Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": "2024-01-01 00:05:00",
                "position": 1.0,
                "final_signal": 1.0,
                "bar_benchmark": 110.0,
            },
        ]
        anchors = {}
        prices = {f"2024-01-01 00:{str(m).zfill(2)}:00": 100.0 for m in range(5, 15)}

        rows = compute_prod_pnl(bars, anchors, prices, source_label="production")

        # 2 bars × 5 min = 10 rows
        assert len(rows) == 10

    def test_existing_anchor(self):
        """Should continue from existing anchor PnL and price."""
        bars = [
            {
                "strategy_table_name": "test_strategy",
                "strategy_instance_id": "test_strategy__1",
                "strategy_id": 1,
                "strategy_name": "Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": "2024-01-01 00:10:00",
                "position": 1.0,
                "final_signal": 1.0,
                "bar_benchmark": 110.0,
            }
        ]
        anchors = {"test_strategy": (0.05, 105.0, 1.0)}  # 5% PnL at price 105, pos 1.0
        # Flat prices from our source — PnL stays at 0.05 (no movement)
        prices = {f"2024-01-01 00:{str(m).zfill(2)}:00": 105.0 for m in range(15, 20)}

        rows = compute_prod_pnl(bars, anchors, prices, source_label="production")

        assert len(rows) == 5
        # Flat price at anchor_price=105 → cpnl stays 0.05
        assert abs(rows[0][8] - 0.05) < 1e-10


class TestComputeRealTradePnl:
    """Test compute_real_trade_pnl with execution timing."""

    def _make_revision(
        self, ts, closing_ts, execution_ts, position,
        revision_ts=None,
    ):
        if revision_ts is None:
            revision_ts = execution_ts
        return {
            "strategy_table_name": "test_strategy",
            "strategy_instance_id": "test_strategy__1",
            "strategy_id": 1,
            "strategy_name": "Test",
            "underlying": "btc",
            "config_timeframe": "5m",
            "weighting": 1.0,
            "ts": ts,
            "closing_ts": closing_ts,
            "execution_ts": execution_ts,
            "revision_ts": revision_ts,
            "position": position,
            "final_signal": 1.0,
            "bar_benchmark": 100.0,
        }

    def test_single_bar(self):
        bars = [self._make_revision(
            "2024-01-01 00:00:00", "2024-01-01 00:05:00", "2024-01-01 00:05:00", 1.0,
        )]
        anchors = {}
        prices = {f"2024-01-01 00:0{5+i}:00": 100.0 for i in range(5)}

        rows = compute_real_trade_pnl(bars, anchors, prices)

        assert len(rows) == 5
        # Row has 16 columns matching INSERT_COLUMNS
        assert len(rows[0]) == 16
        # strategy_instance_id at index 15
        assert rows[0][15] == "test_strategy__1"

    def test_multiple_revisions_no_overlap(self):
        """Two revisions for the same bar must not produce overlapping timestamp ranges.

        Old bug: each revision emitted its own tf_minutes rows independently,
        causing timestamps 00:07 and 00:08 to appear twice with different positions.

        Correct behaviour: the bar's [exec_ts, next_exec_ts) window is expanded once,
        with position switching to revision 2's value at its execution_ts.
        """
        # Bar ts=00:00, closing=00:05.  Revision 1 executed at 00:05 (pos=0),
        # revision 2 executed at 00:07 (pos=1, the trade fired late).
        bars = [
            self._make_revision("2024-01-01 00:00:00", "2024-01-01 00:05:00", "2024-01-01 00:05:00", 0.0),
            self._make_revision("2024-01-01 00:00:00", "2024-01-01 00:05:00", "2024-01-01 00:07:00", 1.0),
        ]
        anchors = {}
        prices = {f"2024-01-01 00:0{5+i}:00": 100.0 for i in range(5)}

        rows = compute_real_trade_pnl(bars, anchors, prices)

        # Must be exactly 5 rows (00:05 to 00:09) — no duplicates.
        assert len(rows) == 5
        ts_values = [r[7] for r in rows]
        assert ts_values == sorted(set(ts_values)), "Duplicate timestamps detected"

        # Position should be 0 at 00:05 and 00:06, then switch to 1 from 00:07.
        positions = [r[10] for r in rows]
        assert positions[0] == 0.0  # 00:05
        assert positions[1] == 0.0  # 00:06
        assert positions[2] == 1.0  # 00:07 — revision 2 kicks in
        assert positions[3] == 1.0  # 00:08
        assert positions[4] == 1.0  # 00:09

    def test_two_consecutive_bars_hold_until_next_execution(self):
        """Position from bar 1 holds until bar 2's execution_ts fires."""
        bars = [
            self._make_revision(
                "2024-01-01 00:00:00", "2024-01-01 00:05:00", "2024-01-01 00:05:00", 1.0,
                revision_ts="2024-01-01 00:05:00",
            ),
            self._make_revision(
                "2024-01-01 00:05:00", "2024-01-01 00:10:00", "2024-01-01 00:10:00", -1.0,
                revision_ts="2024-01-01 00:10:00",
            ),
        ]
        anchors = {}
        prices = {f"2024-01-01 00:{str(5+i).zfill(2)}:00": 100.0 for i in range(10)}

        rows = compute_real_trade_pnl(bars, anchors, prices)

        # 5 rows from bar1 (00:05–00:09) + 5 rows from bar2 (00:10–00:14) = 10 total
        assert len(rows) == 10
        ts_values = [r[7] for r in rows]
        assert ts_values == sorted(set(ts_values))

    def test_late_revision_same_bar_accepted(self):
        """1h bar: a later revision (higher revision_ts, same bar_ts) is accepted after first."""
        bars = [
            self._make_revision(
                "2024-01-01 00:00:00", "2024-01-01 01:00:00", "2024-01-01 01:00:00", 0.0,
                revision_ts="2024-01-01 00:59:00",
            ),
            self._make_revision(
                "2024-01-01 00:00:00", "2024-01-01 01:00:00", "2024-01-01 01:33:00", 1.0,
                revision_ts="2024-01-01 01:32:00",
            ),
            self._make_revision(
                "2024-01-01 01:00:00", "2024-01-01 02:00:00", "2024-01-01 02:14:00", -1.0,
                revision_ts="2024-01-01 02:13:00",
            ),
        ]
        for bar in bars:
            bar["config_timeframe"] = "1h"
        anchors = {}
        prices = {f"2024-01-01 0{h}:{str(m).zfill(2)}:00": 100.0
                  for h in range(1, 3) for m in range(60)}
        prices.update({f"2024-01-01 02:{str(m).zfill(2)}:00": 100.0 for m in range(20)})

        rows = compute_real_trade_pnl(bars, anchors, prices)

        ts_values = [r[7] for r in rows]
        assert ts_values == sorted(set(ts_values)), "Duplicate timestamps detected"
        assert ts_values[0] == "2024-01-01 01:00:00"
        assert "2024-01-01 01:32:00" in ts_values  # last minute under first revision
        assert "2024-01-01 01:33:00" in ts_values  # second revision fires
        assert "2024-01-01 02:13:00" in ts_values  # last minute before bar B fires
        assert "2024-01-01 02:14:00" in ts_values  # bar B starts

    def test_older_bar_revision_after_newer_bar_discarded(self):
        """Revision for older bar_ts arriving after a newer bar is accepted is discarded."""
        bars = [
            self._make_revision(
                "2024-01-01 00:00:00", "2024-01-01 01:00:00", "2024-01-01 01:00:00", 1.0,
                revision_ts="2024-01-01 00:59:00",
            ),
            self._make_revision(
                "2024-01-01 01:00:00", "2024-01-01 02:00:00", "2024-01-01 02:00:00", -1.0,
                revision_ts="2024-01-01 01:59:00",
            ),
            # Stale: bar_ts=00:00 but revision_ts=02:30 — sorts last but bar_ts < 01:00 → discard
            self._make_revision(
                "2024-01-01 00:00:00", "2024-01-01 01:00:00", "2024-01-01 02:31:00", 0.5,
                revision_ts="2024-01-01 02:30:00",
            ),
        ]
        for bar in bars:
            bar["config_timeframe"] = "1h"
        anchors = {}
        prices = {f"2024-01-01 0{h}:{str(m).zfill(2)}:00": 100.0
                  for h in range(1, 3) for m in range(60)}
        prices.update({f"2024-01-01 02:{str(m).zfill(2)}:00": 100.0 for m in range(40)})

        rows = compute_real_trade_pnl(bars, anchors, prices)

        positions = {r[7]: r[10] for r in rows}
        # Stale revision's execution_ts=02:31 must not appear or have position=0.5
        assert all(p != 0.5 for p in positions.values()), "stale revision must be discarded"
        # After bar B (01:00), position must be -1.0
        assert positions.get("2024-01-01 02:00:00") == -1.0


class TestFetchNewBarsRealTradeSQL:
    """Test that fetch_new_bars_real_trade generates correct SQL (no alias collision)."""

    def _capture_sql(self, monkeypatch, **kwargs) -> str:
        captured = {}

        def fake_query_dicts(sql, client=None):
            captured["sql"] = sql
            return []

        monkeypatch.setattr(
            "libs.computation.fetch_bars.query_dicts", fake_query_dicts
        )
        fetch_new_bars_real_trade(**kwargs)
        return captured["sql"]

    def test_order_by_sorts_by_strategy_ts_revision(self, monkeypatch):
        """Final ORDER BY must sort by strategy_table_name, ts, revision_ts."""
        sql = self._capture_sql(
            monkeypatch,
            source_table="strategy_output_history_v2",
            underlying="btc",
            ts_start="2025-04-27 00:00:00",
            ts_end="2025-04-28 00:00:00",
        )
        # Use the last ORDER BY (outer query), not the one inside the WINDOW definition.
        order_by = re.findall(r"ORDER BY (.+)", sql)[-1].strip()
        assert "strategy_table_name" in order_by
        assert "revision_ts" in order_by


class TestTimeframeMap:
    """Test timeframe mapping."""

    def test_all_timeframes(self):
        expected = {
            "1m": 1, "3m": 3, "5m": 5, "10m": 10,
            "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440,
        }
        assert TIMEFRAME_MAP == expected


class TestFetchPricesMultiCloseColumn:
    """Test that fetch_prices_multi can fetch close prices."""

    def _capture_sql(self, monkeypatch, **kwargs) -> str:
        captured = {}

        def fake_query_rows(sql, client=None):
            captured["sql"] = sql
            return []

        monkeypatch.setattr("libs.computation.fetch_prices.query_rows", fake_query_rows)
        fetch_prices_multi(**kwargs)
        return captured["sql"]

    def test_default_fetches_open(self, monkeypatch):
        """Default price_column is open (existing prod/real_trade path)."""
        sql = self._capture_sql(
            monkeypatch,
            underlyings=["btc"],
            ts_min="2024-01-01 00:00:00",
            ts_max="2024-01-02 00:00:00",
        )
        assert "open" in sql

    def test_close_column_fetches_close(self, monkeypatch):
        """price_column='close' must select close from futures_price_1min."""
        sql = self._capture_sql(
            monkeypatch,
            underlyings=["btc"],
            ts_min="2024-01-01 00:00:00",
            ts_max="2024-01-02 00:00:00",
            price_column="close",
        )
        assert "close" in sql
        assert "open" not in sql
