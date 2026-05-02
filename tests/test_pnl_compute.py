"""
Unit tests for PnL computation logic.

Tests the anchor-chained PnL computation without ClickHouse dependency.
"""

import pytest

from trading_dagster.utils.pnl_compute import (
    assert_anchors_present,
    compute_prod_pnl,
    compute_real_trade_pnl,
    PROD_REAL_TRADE_START_DATE,
    BT_START_DATE,
    TIMEFRAME_MAP,
)


class TestComputeProdPnl:
    """Test compute_prod_pnl anchor chaining logic."""

    def test_single_bar_no_anchor_prod_start_date(self):
        """First bar on PROD_REAL_TRADE_START_DATE with no anchor is allowed (cold start)."""
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

        assert_anchors_present(anchors, bars, start_date=PROD_REAL_TRADE_START_DATE)  # must not raise
        rows = compute_prod_pnl(bars, anchors, prices, source_label="production")

        assert len(rows) == 5
        for row in rows:
            assert row[11] == 100.0  # price column

    def test_single_bar_no_anchor_bt_start_date(self):
        """First bar on BT_START_DATE with no anchor is allowed (cold start)."""
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
        assert_anchors_present({}, bars, start_date=BT_START_DATE)  # must not raise

    def test_no_anchor_after_prod_start_date_raises(self):
        """Missing anchor for a prod bar after PROD_REAL_TRADE_START_DATE must raise."""
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
        with pytest.raises(RuntimeError, match="Missing PnL anchor"):
            assert_anchors_present({}, bars, start_date=PROD_REAL_TRADE_START_DATE)

    def test_no_anchor_after_bt_start_date_raises(self):
        """Missing anchor for a bt bar after BT_START_DATE must raise."""
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
        with pytest.raises(RuntimeError, match="Missing PnL anchor"):
            assert_anchors_present({}, bars, start_date=BT_START_DATE)

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
        prices = {}

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
        prices = {}

        rows = compute_prod_pnl(bars, anchors, prices, source_label="production")

        assert len(rows) == 5
        # First row uses anchor_price 105.0 as fallback, so PnL stays 0.05
        assert abs(rows[0][8] - 0.05) < 1e-10


class TestComputeRealTradePnl:
    """Test compute_real_trade_pnl with execution timing."""

    def test_single_bar(self):
        bars = [
            {
                "strategy_table_name": "test_strategy",
                "strategy_id": 1,
                "strategy_name": "Test",
                "underlying": "btc",
                "config_timeframe": "5m",
                "weighting": 1.0,
                "ts": "2024-01-01 00:00:00",
                "closing_ts": "2024-01-01 00:05:00",
                "execution_ts": "2024-01-01 00:05:00",
                "position": 1.0,
                "bar_price": 100.0,
                "final_signal": 1.0,
                "bar_benchmark": 100.0,
            }
        ]
        anchors = {}
        prices = {}

        rows = compute_real_trade_pnl(bars, anchors, prices)

        assert len(rows) == 5
        # Should include closing_ts and execution_ts columns
        assert rows[0][15] == "2024-01-01 00:05:00"  # closing_ts
        assert rows[0][16] == "2024-01-01 00:05:00"  # execution_ts


class TestTimeframeMap:
    """Test timeframe mapping."""

    def test_all_timeframes(self):
        expected = {"5m": 5, "10m": 10, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}
        assert TIMEFRAME_MAP == expected
