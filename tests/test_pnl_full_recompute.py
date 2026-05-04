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

from datetime import date
from unittest.mock import MagicMock, patch

# Import will fail until Task 2 adds the function — that's expected.
from trading_dagster.assets.pnl_strategy_v2 import (
    _recompute_pnl_full,
)
from trading_dagster.utils.pnl_compute import (
    PROD_INSERT_COLUMNS,
    PROD_REAL_TRADE_START_DATE,
    REAL_TRADE_INSERT_COLUMNS,
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


class TestRecomputePnlFull:

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_prod_fetches_bars_for_full_range(
        self, mock_client, mock_get_und, mock_qd, mock_anchors, mock_prices, mock_insert
    ):
        """Bars query covers PROD_REAL_TRADE_START_DATE to now for all underlyings."""
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

        # bars query must include start boundary
        call_args = mock_qd.call_args[0][0]
        assert PROD_REAL_TRADE_START_DATE in call_args
        assert "strategy_output_history_v2" in call_args
        assert "btc" in call_args
        today_str = date.today().strftime("%Y-%m-%d")
        assert today_str in call_args

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

        table_arg = mock_insert.call_args[0][0]
        cols_arg = mock_insert.call_args[0][1]
        assert table_arg == "analytics.strategy_pnl_1min_real_trade_v2"
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
