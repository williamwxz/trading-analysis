"""
Unit tests for PnL computation logic.

Tests the anchor-chained PnL computation without ClickHouse dependency.
"""

import re
import pytest
from unittest.mock import patch

from trading_dagster.utils.pnl_compute import (
    assert_anchors_present,
    compute_prod_pnl,
    compute_bt_pnl,
    compute_real_trade_pnl,
    fetch_new_bars_bt,
    fetch_new_bars_real_trade,
    fetch_prices_multi,
    PROD_REAL_TRADE_START_DATE,
    BT_START_DATE,
    TIMEFRAME_MAP,
)


class TestComputeProdPnl:
    """Test compute_prod_pnl anchor chaining logic."""

    def test_single_bar_no_anchor_on_strategy_first_date_allowed(self):
        """First bar on strategy's first date with no anchor is allowed (cold start)."""
        bars = [
            {
                "strategy_table_name": "test_strategy",
                "strategy_id": 1,
                "strategy_name": "Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": f"{PROD_REAL_TRADE_START_DATE} 00:00:00",
                "position": 1.0,
                "bar_price": 100.0,
                "final_signal": 1.0,
                "bar_benchmark": 100.0,
            }
        ]
        anchors = {}
        prices = {}
        with patch("trading_dagster.utils.pnl_compute.fetch_strategy_start_dates") as mock_sd:
            mock_sd.return_value = {"test_strategy": PROD_REAL_TRADE_START_DATE}
            assert_anchors_present(anchors, bars, source_table="strategy_output_history_v2")  # must not raise

        # Price must come from our source — provide prices for all 5 expansion minutes.
        closing_ts_base = f"{PROD_REAL_TRADE_START_DATE} 00:05:00"
        prices = {f"{PROD_REAL_TRADE_START_DATE} 00:0{5+i}:00": 100.0 for i in range(5)}
        rows = compute_prod_pnl(bars, anchors, prices, source_label="production")
        assert len(rows) == 5
        for row in rows:
            assert row[11] == 100.0  # price column

    def test_single_bar_no_anchor_bt_first_date_allowed(self):
        """First bar on strategy's first BT date with no anchor is allowed (cold start)."""
        bars = [
            {
                "strategy_table_name": "test_bt_strategy",
                "strategy_id": 2,
                "strategy_name": "BT Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": f"{BT_START_DATE} 00:00:00",
                "position": 1.0,
                "bar_price": 50.0,
                "final_signal": 1.0,
                "bar_benchmark": 50.0,
            }
        ]
        with patch("trading_dagster.utils.pnl_compute.fetch_strategy_start_dates") as mock_sd:
            mock_sd.return_value = {"test_bt_strategy": BT_START_DATE}
            assert_anchors_present({}, bars, source_table="strategy_output_history_bt_v2")  # must not raise

    def test_no_anchor_after_first_date_raises(self):
        """Missing anchor for a bar after the strategy's first date must raise."""
        bars = [
            {
                "strategy_table_name": "test_strategy",
                "strategy_id": 1,
                "strategy_name": "Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": "2026-03-01 00:00:00",
                "position": 1.0,
                "bar_price": 100.0,
                "final_signal": 1.0,
                "bar_benchmark": 100.0,
            }
        ]
        with patch("trading_dagster.utils.pnl_compute.fetch_strategy_start_dates") as mock_sd:
            mock_sd.return_value = {"test_strategy": PROD_REAL_TRADE_START_DATE}
            with pytest.raises(RuntimeError, match="Missing PnL anchor"):
                assert_anchors_present({}, bars, source_table="strategy_output_history_v2")

    def test_no_anchor_after_bt_first_date_raises(self):
        """Missing anchor for a bt bar after its first date must raise."""
        bars = [
            {
                "strategy_table_name": "test_bt_strategy",
                "strategy_id": 2,
                "strategy_name": "BT Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": "2024-01-01 00:00:00",
                "position": 1.0,
                "bar_price": 50.0,
                "final_signal": 1.0,
                "bar_benchmark": 50.0,
            }
        ]
        with patch("trading_dagster.utils.pnl_compute.fetch_strategy_start_dates") as mock_sd:
            mock_sd.return_value = {"test_bt_strategy": BT_START_DATE}
            with pytest.raises(RuntimeError, match="Missing PnL anchor"):
                assert_anchors_present({}, bars, source_table="strategy_output_history_bt_v2")

    def test_anchor_chaining_two_bars(self):
        """Two consecutive bars should chain PnL correctly."""
        bars = [
            {
                "strategy_table_name": "test_strategy",
                "strategy_id": 1,
                "strategy_name": "Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": "2024-01-01 00:00:00",
                "position": 1.0,
                "bar_price": 100.0,
                "final_signal": 1.0,
                "bar_benchmark": 100.0,
            },
            {
                "strategy_table_name": "test_strategy",
                "strategy_id": 1,
                "strategy_name": "Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": "2024-01-01 00:05:00",
                "position": 1.0,
                "bar_price": 110.0,
                "final_signal": 1.0,
                "bar_benchmark": 110.0,
            },
        ]
        anchors = {}
        # Prices from our source covering both bars' expansion windows (closing_ts + 5 min each)
        prices = {f"2024-01-01 00:{str(m).zfill(2)}:00": 100.0 for m in range(5, 15)}

        rows = compute_prod_pnl(bars, anchors, prices, source_label="production")

        # 2 bars × 5 min = 10 rows
        assert len(rows) == 10

    def test_existing_anchor(self):
        """Should continue from existing anchor PnL and price."""
        bars = [
            {
                "strategy_table_name": "test_strategy",
                "strategy_id": 1,
                "strategy_name": "Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": "2024-01-01 00:10:00",
                "position": 1.0,
                "bar_price": 110.0,
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
        revision_ts=None, next_bar_closing_ts=None,
    ):
        # Default: revision arrives exactly at execution_ts (on-time),
        # no next bar (next_bar_closing_ts sentinel = closing_ts).
        if revision_ts is None:
            revision_ts = execution_ts
        if next_bar_closing_ts is None:
            next_bar_closing_ts = closing_ts  # sentinel: no next bar → always accept
        return {
            "strategy_table_name": "test_strategy",
            "strategy_id": 1,
            "strategy_name": "Test",
            "underlying": "btc",
            "config_timeframe": "5m",
            "weighting": 1.0,
            "ts": ts,
            "closing_ts": closing_ts,
            "execution_ts": execution_ts,
            "revision_ts": revision_ts,
            "next_bar_closing_ts": next_bar_closing_ts,
            "position": position,
            "bar_price": 100.0,
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
        # Should include closing_ts and execution_ts columns
        assert rows[0][15] == "2024-01-01 00:05:00"  # closing_ts
        assert rows[0][16] == "2024-01-01 00:05:00"  # execution_ts

    def test_multiple_revisions_no_overlap(self):
        """Two revisions for the same bar must not produce overlapping timestamp ranges.

        Old bug: each revision emitted its own tf_minutes rows independently,
        causing timestamps 00:07 and 00:08 to appear twice with different positions,
        which caused PnL overshoot/jumps in the Grafana aggregate curves.

        Correct behaviour: the bar's [closing_ts, next_closing_ts) window is expanded
        once, with position switching to revision 2's value at its execution_ts.
        """
        # Bar ts=00:00, closing=00:05.  Revision 1 executed at 00:05 (pos=0),
        # revision 2 executed at 00:07 (pos=1, the trade fired late).
        bars = [
            self._make_revision("2024-01-01 00:00:00", "2024-01-01 00:05:00", "2024-01-01 00:05:00", 0.0),
            self._make_revision("2024-01-01 00:00:00", "2024-01-01 00:05:00", "2024-01-01 00:07:00", 1.0),
        ]
        anchors = {}
        # Flat price at 100 for minutes 00:05 through 00:09
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
                next_bar_closing_ts="2024-01-01 00:10:00",
            ),
            self._make_revision(
                "2024-01-01 00:05:00", "2024-01-01 00:10:00", "2024-01-01 00:10:00", -1.0,
                revision_ts="2024-01-01 00:10:00",
                next_bar_closing_ts="2024-01-01 00:10:00",  # no next bar sentinel
            ),
        ]
        anchors = {}
        prices = {f"2024-01-01 00:{str(5+i).zfill(2)}:00": 100.0 for i in range(10)}

        rows = compute_real_trade_pnl(bars, anchors, prices)

        # 5 rows from bar1 (00:05–00:09) + 5 rows from bar2 (00:10–00:14) = 10 total
        assert len(rows) == 10
        ts_values = [r[7] for r in rows]
        assert ts_values == sorted(set(ts_values))

    def test_late_revision_accepted_before_next_bar_closes(self):
        """1h bar revision arriving after bar close but before next bar's close is accepted.

        Real data pattern: bar 00:00 (closes 01:00), revision arrives 01:32 → exec 01:33.
        next_bar_closing_ts = bar_B.closing = 02:00. Since 01:32 < 02:00 → ACCEPT.
        Position holds 01:33..02:13 continuously (until bar B's revision fires at 02:14).
        """
        bars = [
            self._make_revision(
                "2024-01-01 00:00:00", "2024-01-01 01:00:00", "2024-01-01 01:33:00", 1.0,
                revision_ts="2024-01-01 01:32:00",
                next_bar_closing_ts="2024-01-01 02:00:00",
            ),
            self._make_revision(
                "2024-01-01 01:00:00", "2024-01-01 02:00:00", "2024-01-01 02:14:00", -1.0,
                revision_ts="2024-01-01 02:13:00",
                next_bar_closing_ts="2024-01-01 02:00:00",  # no next bar sentinel
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
        assert ts_values[0] == "2024-01-01 01:33:00"
        assert "2024-01-01 01:59:00" in ts_values  # crosses bar A closing_ts
        assert "2024-01-01 02:00:00" in ts_values  # no gap
        assert "2024-01-01 02:13:00" in ts_values  # last minute before bar B fires
        assert "2024-01-01 02:14:00" in ts_values  # bar B starts

    def test_late_revision_discarded_after_next_bar_closes(self):
        """Revision arriving after next bar's closing_ts is discarded; previous position holds.

        Bar A (00:00, closing 01:00) revision arrives at 02:12 — after bar B closes at 02:00.
        Bar B (01:00, closing 02:00) revision arrives at 02:14 → exec 02:14, accepted.
        During 02:14 onward bar B's position (-1.0) is active.
        Before 02:14, the anchor position (0.0 = cold start) holds since bar A is discarded.
        """
        bars = [
            self._make_revision(
                "2024-01-01 00:00:00", "2024-01-01 01:00:00", "2024-01-01 02:13:00", 1.0,
                revision_ts="2024-01-01 02:12:00",
                next_bar_closing_ts="2024-01-01 02:00:00",  # 02:12 >= 02:00 → DISCARD
            ),
            self._make_revision(
                "2024-01-01 01:00:00", "2024-01-01 02:00:00", "2024-01-01 02:14:00", -1.0,
                revision_ts="2024-01-01 02:13:00",
                next_bar_closing_ts="2024-01-01 02:00:00",  # no next bar sentinel
            ),
        ]
        for bar in bars:
            bar["config_timeframe"] = "1h"
        anchors = {}
        prices = {f"2024-01-01 02:{str(m).zfill(2)}:00": 100.0 for m in range(20)}

        rows = compute_real_trade_pnl(bars, anchors, prices)

        ts_values = [r[7] for r in rows]
        positions = {r[7]: r[10] for r in rows}

        # Bar A discarded: no rows before 02:14
        assert "2024-01-01 02:13:00" not in ts_values
        # Bar B accepted: rows from 02:14 onward with position=-1.0
        assert "2024-01-01 02:14:00" in ts_values
        assert positions["2024-01-01 02:14:00"] == -1.0


class TestFetchNewBarsRealTradeSQL:
    """Test that fetch_new_bars_real_trade generates correct SQL (no alias collision)."""

    def _capture_sql(self, monkeypatch, **kwargs) -> str:
        captured = {}

        def fake_query_dicts(sql):
            captured["sql"] = sql
            return []

        monkeypatch.setattr(
            "trading_dagster.utils.pnl_compute.query_dicts", fake_query_dicts
        )
        fetch_new_bars_real_trade(**kwargs)
        return captured["sql"]

    def test_order_by_sorts_by_strategy_ts_revision(self, monkeypatch):
        """Final ORDER BY must sort by strategy_table_name, ts, revision_ts to ensure
        revisions are processed in chronological order per strategy."""
        sql = self._capture_sql(
            monkeypatch,
            source_table="strategy_output_history_v2",
            underlying="btc",
            since="2025-04-27 00:00:00",
            ts_end="2025-04-28 00:00:00",
        )
        # Use the last ORDER BY (outer query), not the one inside the WINDOW definition.
        order_by = re.findall(r"ORDER BY (.+)", sql)[-1].strip()
        assert "strategy_table_name" in order_by
        assert "revision_ts" in order_by


class TestTimeframeMap:
    """Test timeframe mapping."""

    def test_all_timeframes(self):
        expected = {"5m": 5, "10m": 10, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}
        assert TIMEFRAME_MAP == expected


class TestFetchNewBarsBtSQL:
    """Test that fetch_new_bars_bt generates correct SQL for daily partition path."""

    def _capture_sql(self, monkeypatch, **kwargs) -> str:
        captured = {}

        def fake_query_dicts(sql):
            captured["sql"] = sql
            return []

        monkeypatch.setattr("trading_dagster.utils.pnl_compute.query_dicts", fake_query_dicts)
        fetch_new_bars_bt(**kwargs)
        return captured["sql"]

    def test_daily_path_uses_ts_range_filter(self, monkeypatch):
        """When ts_end is provided, SQL must filter on ts range (not revision_ts watermark).

        The daily partition asset needs to re-process a specific day, not just new rows.
        """
        sql = self._capture_sql(
            monkeypatch,
            source_table="strategy_output_history_bt_v2",
            underlying="btc",
            since="2024-01-01 00:00:00",
            ts_end="2024-01-02 00:00:00",
        )
        assert "toDateTime(ts) >= toDateTime('2024-01-01 00:00:00')" in sql
        assert "toDateTime(ts) < toDateTime('2024-01-02 00:00:00')" in sql
        assert "revision_ts >= toDateTime" not in sql

    def test_live_path_uses_revision_ts_watermark(self, monkeypatch):
        """Without ts_end, SQL must use revision_ts >= since (watermark / live path)."""
        sql = self._capture_sql(
            monkeypatch,
            source_table="strategy_output_history_bt_v2",
            underlying="btc",
            since="2024-01-01 00:00:00",
        )
        assert "revision_ts >= toDateTime('2024-01-01 00:00:00')" in sql
        assert "toDateTime(ts) >= toDateTime" not in sql


class TestFetchPricesMultiCloseColumn:
    """Test that fetch_prices_multi can fetch close prices."""

    def _capture_sql(self, monkeypatch, **kwargs) -> str:
        captured = {}

        def fake_query_rows(sql, client=None):
            captured["sql"] = sql
            return []

        monkeypatch.setattr("trading_dagster.utils.pnl_compute.query_rows", fake_query_rows)
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


class TestComputeBtPnl:
    """Test compute_bt_pnl expands bars correctly using execution_ts."""

    def _make_bar(self, ts, execution_ts, position, bar_price, cumulative_pnl, tf="5m"):
        return {
            "strategy_table_name": "test_bt",
            "strategy_id": 1,
            "strategy_name": "BT Test",
            "underlying": "btc",
            "config_timeframe": tf,
            "weighting": 1.0,
            "ts": ts,
            "execution_ts": execution_ts,
            "position": position,
            "bar_price": bar_price,
            "final_signal": 1.0,
            "bar_benchmark": 100.0,
            "cumulative_pnl": cumulative_pnl,
        }

    def test_single_5m_bar_expands_to_5_rows(self):
        """A 5m bar should produce 5 1-minute rows starting at execution_ts."""
        bar = self._make_bar("2024-01-01 00:00:00", "2024-01-01 00:05:00", 1.0, 100.0, 0.0)
        prices = {
            "2024-01-01 00:05:00": 101.0,
            "2024-01-01 00:06:00": 102.0,
            "2024-01-01 00:07:00": 103.0,
            "2024-01-01 00:08:00": 104.0,
            "2024-01-01 00:09:00": 105.0,
        }

        rows = compute_bt_pnl([bar], prices)

        assert len(rows) == 5
        # First row ts is execution_ts, not bar ts
        assert rows[0][7] == "2024-01-01 00:05:00"
        # source label is "backtest"
        assert rows[0][5] == "backtest"

    def test_two_consecutive_bars_chain_pnl(self):
        """Second bar's PnL chains from the computed end of the first bar, not from its cumulative_pnl field."""
        bars = [
            self._make_bar("2024-01-01 00:00:00", "2024-01-01 00:05:00", 1.0, 100.0, 0.0),
            # cumulative_pnl=99.0 is a bogus value — must be ignored in favour of chaining
            self._make_bar("2024-01-01 00:05:00", "2024-01-01 00:10:00", -1.0, 105.0, 99.0),
        ]
        # Flat prices so first bar ends at cpnl=0.0; second bar should continue from there
        prices = {f"2024-01-01 00:0{i}:00": 100.0 for i in range(10)}

        rows = compute_bt_pnl(bars, prices)

        assert len(rows) == 10
        # The 6th row (first minute of bar 2) must NOT jump to 99.0; must be ~0.0
        assert abs(rows[5][8]) < 1e-6, f"Expected ~0.0 but got {rows[5][8]} — bar 2 is not chaining correctly"

    def test_cross_day_anchor_used_instead_of_source_cumulative_pnl(self):
        """When an anchor is provided from the previous day, it takes priority over bar cumulative_pnl."""
        bar = self._make_bar("2024-01-02 00:00:00", "2024-01-02 00:05:00", 1.0, 100.0, 99.0)
        prices = {"2024-01-02 00:05:00": 100.0}
        # Previous day ended at pnl=0.05, price=100.0
        anchors = {"test_bt": (0.05, 100.0, 1.0)}

        rows = compute_bt_pnl([bar], prices, anchors=anchors)

        assert len(rows) == 5
        # First row should start from anchor_pnl=0.05 (flat price → stays 0.05), not from 99.0
        assert abs(rows[0][8] - 0.05) < 1e-6, f"Expected 0.05 but got {rows[0][8]}"

    def test_output_row_has_correct_column_count(self):
        """Output row must have exactly 15 columns matching PROD_INSERT_COLUMNS."""
        bar = self._make_bar("2024-01-01 00:00:00", "2024-01-01 00:05:00", 1.0, 100.0, 0.0)
        prices = {f"2024-01-01 00:0{5+i}:00": 100.0 for i in range(5)}
        rows = compute_bt_pnl([bar], prices)
        assert len(rows) == 5
        assert len(rows[0]) == 15
