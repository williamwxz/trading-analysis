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

import pytest
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
        "revision_ts": "2026-02-27 00:00:30",
        "next_bar_closing_ts": "2026-02-27 00:05:00",
    }


class TestRecomputePnlFull:

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_scalar", return_value=None)
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_prod_fetches_bars_for_full_range(
        self, mock_client, mock_qs, mock_get_und, mock_qd, mock_prices, mock_insert
    ):
        """Bars query covers PROD_REAL_TRADE_START_DATE to now for all underlyings."""
        mock_get_und.return_value = ["btc"]
        mock_qd.return_value = [_make_bar()]
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

        first_call_args = mock_qd.call_args_list[0][0][0]
        assert PROD_REAL_TRADE_START_DATE in first_call_args
        assert "strategy_output_history_v2" in first_call_args
        assert "btc" in first_call_args
        last_call_args = mock_qd.call_args[0][0]
        assert str(date.today().year) in last_call_args

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_scalar", return_value=None)
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_prod_inserts_to_correct_table(
        self, mock_client, mock_qs, mock_get_und, mock_qd, mock_prices, mock_insert
    ):
        """Rows must be inserted into the target table with PROD_INSERT_COLUMNS."""
        mock_get_und.return_value = ["btc"]
        mock_qd.return_value = [_make_bar()]
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
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_scalar", return_value=None)
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_empty_underlying_skipped_gracefully(
        self, mock_client, mock_qs, mock_get_und, mock_qd, mock_prices, mock_insert
    ):
        """If source table has no bars for an underlying, insert is not called."""
        mock_get_und.return_value = ["btc"]
        mock_qd.return_value = []  # no bars
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
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_scalar", return_value=None)
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_real_trade_uses_real_trade_columns(
        self, mock_client, mock_qs, mock_get_und, mock_qd, mock_prices, mock_insert
    ):
        """real_trade mode must insert with REAL_TRADE_INSERT_COLUMNS."""
        mock_get_und.return_value = ["btc"]
        mock_qd.return_value = [_make_rt_bar()]
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
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_scalar", return_value=None)
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_progress_logged_per_underlying(
        self, mock_client, mock_qs, mock_get_und, mock_qd, mock_prices, mock_insert
    ):
        """context.log.info must be called at least once per underlying processed."""
        mock_get_und.return_value = ["btc", "eth"]
        mock_qd.return_value = [_make_bar()]
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


class TestRecomputePnlFullRealTradeAcceptance:
    """Verify revision acceptance logic: revision_ts < next_bar_closing_ts."""

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_scalar", return_value=None)
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_revision_accepted_when_before_next_bar_closing(
        self, mock_client, mock_qs, mock_get_und, mock_qd, mock_prices, mock_insert
    ):
        """A revision arriving before next_bar_closing_ts is accepted and produces PnL rows."""
        # bar open ts=00:00, closing=00:05, revision arrives at 00:04 (before next bar closes at 00:10)
        bar = {
            **_make_rt_bar(),
            "closing_ts": "2026-02-27 00:05:00",
            "execution_ts": "2026-02-27 00:05:00",  # toStartOfMinute(00:04 + 59s) = 00:04
            "revision_ts": "2026-02-27 00:04:00",
            "next_bar_closing_ts": "2026-02-27 00:10:00",  # next bar (00:05 open) closes at 00:10
        }
        mock_get_und.return_value = ["btc"]
        mock_qd.return_value = [bar]
        mock_prices.return_value = {"btc": {"2026-02-27 00:05:00": 100.0}}
        mock_insert.return_value = 1

        ctx = _make_context()
        _recompute_pnl_full(
            ctx,
            target_table="strategy_pnl_1min_real_trade_v2",
            source_table="strategy_output_history_v2",
            label="real_trade",
            insert_columns=REAL_TRADE_INSERT_COLUMNS,
            mode="real_trade",
        )

        assert mock_insert.called

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_scalar", return_value=None)
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_revision_discarded_when_after_next_bar_closing(
        self, mock_client, mock_qs, mock_get_und, mock_qd, mock_prices, mock_insert
    ):
        """A revision arriving at or after next_bar_closing_ts is discarded — produces 0 output rows."""
        # revision_ts=00:11 >= next_bar_closing_ts=00:10 → discard
        bar = {
            **_make_rt_bar(),
            "closing_ts": "2026-02-27 00:05:00",
            "execution_ts": "2026-02-27 00:12:00",
            "revision_ts": "2026-02-27 00:11:00",
            "next_bar_closing_ts": "2026-02-27 00:10:00",
        }
        mock_get_und.return_value = ["btc"]
        # Only the first chunk returns the bar; all others empty (single bar in range)
        mock_qd.side_effect = lambda sql, client: [bar] if mock_qd.call_count == 1 else []
        mock_prices.return_value = {"btc": {}}
        mock_insert.return_value = 0

        ctx = _make_context()
        result = _recompute_pnl_full(
            ctx,
            target_table="strategy_pnl_1min_real_trade_v2",
            source_table="strategy_output_history_v2",
            label="real_trade",
            insert_columns=REAL_TRADE_INSERT_COLUMNS,
            mode="real_trade",
        )

        # Discarded revision → compute_real_trade_pnl returns [] → no rows ever inserted
        all_row_args = [call.args[2] for call in mock_insert.call_args_list]
        assert all(rows == [] for rows in all_row_args), f"Expected all inserts to have empty rows, got {all_row_args}"
        assert result.metadata["rows_inserted"] == 0

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_scalar", return_value=None)
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_only_first_revision_accepted_when_second_is_late(
        self, mock_client, mock_qs, mock_get_und, mock_qd, mock_prices, mock_insert
    ):
        """Two revisions for same bar: first accepted, second discarded (arrived after next bar closed)."""
        bar_early = {
            **_make_rt_bar(),
            "closing_ts": "2026-02-27 00:05:00",
            "execution_ts": "2026-02-27 00:04:00",
            "revision_ts": "2026-02-27 00:03:00",   # 00:03 < next_bar_closing 00:10 → ACCEPT
            "next_bar_closing_ts": "2026-02-27 00:10:00",
        }
        bar_late = {
            **_make_rt_bar(),
            "closing_ts": "2026-02-27 00:05:00",
            "execution_ts": "2026-02-27 00:12:00",
            "revision_ts": "2026-02-27 00:11:00",   # 00:11 >= next_bar_closing 00:10 → DISCARD
            "next_bar_closing_ts": "2026-02-27 00:10:00",
        }
        mock_get_und.return_value = ["btc"]
        # Only the first chunk returns both bars; all others empty
        mock_qd.side_effect = lambda sql, client: [bar_early, bar_late] if mock_qd.call_count == 1 else []
        mock_prices.return_value = {"btc": {"2026-02-27 00:04:00": 100.0}}
        mock_insert.return_value = 1

        ctx = _make_context()
        result = _recompute_pnl_full(
            ctx,
            target_table="strategy_pnl_1min_real_trade_v2",
            source_table="strategy_output_history_v2",
            label="real_trade",
            insert_columns=REAL_TRADE_INSERT_COLUMNS,
            mode="real_trade",
        )

        # bar_late is discarded → only bar_early's rows inserted → total > 0
        assert result.metadata["rows_inserted"] > 0
        # All inserts that had rows came from the accepted revision only
        non_empty_inserts = [call.args[2] for call in mock_insert.call_args_list if call.args[2]]
        assert len(non_empty_inserts) == 1

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_scalar", return_value=None)
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_last_bar_in_chunk_sentinel_always_accepted(
        self, mock_client, mock_qs, mock_get_und, mock_qd, mock_prices, mock_insert
    ):
        """When next_bar_closing_ts == closing_ts (sentinel, no next bar), revision is always accepted."""
        bar = {
            **_make_rt_bar(),
            "closing_ts": "2026-02-27 00:05:00",
            "execution_ts": "2026-02-27 00:59:00",
            "revision_ts": "2026-02-27 00:58:00",   # very late, but sentinel → ACCEPT
            "next_bar_closing_ts": "2026-02-27 00:05:00",  # == closing_ts: sentinel
        }
        mock_get_und.return_value = ["btc"]
        mock_qd.return_value = [bar]
        mock_prices.return_value = {"btc": {"2026-02-27 00:59:00": 100.0}}
        mock_insert.return_value = 1

        ctx = _make_context()
        _recompute_pnl_full(
            ctx,
            target_table="strategy_pnl_1min_real_trade_v2",
            source_table="strategy_output_history_v2",
            label="real_trade",
            insert_columns=REAL_TRADE_INSERT_COLUMNS,
            mode="real_trade",
        )

        assert mock_insert.called


class TestRecomputePnlFullParallel:

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_scalar", return_value=None)
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_both_underlyings_processed(
        self, mock_client, mock_qs, mock_get_und, mock_qd, mock_prices, mock_insert
    ):
        """All underlyings returned by _get_underlyings are processed and rows summed."""
        mock_get_und.return_value = ["btc", "eth"]
        mock_qd.return_value = [_make_bar()]
        mock_prices.return_value = {"btc": {"2026-02-27 00:00:00": 100.0}, "eth": {"2026-02-27 00:00:00": 50.0}}
        mock_insert.return_value = 3

        ctx = _make_context()
        result = _recompute_pnl_full(
            ctx,
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            label="production",
            insert_columns=PROD_INSERT_COLUMNS,
            mode="prod",
        )

        # insert_rows called at least once per underlying
        assert mock_insert.call_count >= 2
        # total_rows = sum across both underlyings
        assert result.metadata["rows_inserted"] >= 2

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_scalar", return_value=None)
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_worker_exception_propagates(
        self, mock_client, mock_qs, mock_get_und, mock_qd, mock_prices, mock_insert
    ):
        """An exception in a worker thread must propagate and fail the asset run."""
        mock_get_und.return_value = ["btc"]
        mock_qd.side_effect = RuntimeError("ClickHouse connection refused")

        ctx = _make_context()
        with pytest.raises(RuntimeError, match="ClickHouse connection refused"):
            _recompute_pnl_full(
                ctx,
                target_table="strategy_pnl_1min_prod_v2",
                source_table="strategy_output_history_v2",
                label="production",
                insert_columns=PROD_INSERT_COLUMNS,
                mode="prod",
            )
