"""
Unit tests for the hourly rollup assets.

Tests partition window derivation and that correct table names are passed
to ClickHouse. Uses mocks — no live ClickHouse required.
"""

from unittest.mock import MagicMock, patch

from trading_dagster.assets.pnl_rollup import (
    _rollup_day,
    pnl_1hour_bt_rollup_asset,
    pnl_1hour_prod_rollup_asset,
    pnl_1hour_real_trade_rollup_asset,
)
from trading_dagster.utils.pnl_compute import BT_START_DATE, PROD_REAL_TRADE_START_DATE


def _make_context(partition_key: str) -> MagicMock:
    ctx = MagicMock()
    ctx.partition_key = partition_key
    return ctx


class TestRollupDay:
    @patch("trading_dagster.assets.pnl_rollup.get_client", return_value=MagicMock())
    @patch("trading_dagster.assets.pnl_rollup.query_scalar", return_value=24)
    @patch("trading_dagster.assets.pnl_rollup.execute")
    def test_delete_uses_correct_window(self, mock_execute, mock_scalar, mock_client):
        ctx = _make_context("2026-03-01")
        _rollup_day(ctx, "analytics.src_table", "analytics.tgt_table")

        delete_call = mock_execute.call_args_list[0]
        sql = delete_call[0][0]
        assert "DELETE FROM analytics.tgt_table" in sql
        assert "2026-03-01 00:00:00" in sql
        assert "2026-03-02 00:00:00" in sql

    @patch("trading_dagster.assets.pnl_rollup.get_client", return_value=MagicMock())
    @patch("trading_dagster.assets.pnl_rollup.query_scalar", return_value=24)
    @patch("trading_dagster.assets.pnl_rollup.execute")
    def test_insert_uses_correct_source_and_target(self, mock_execute, mock_scalar, mock_client):
        ctx = _make_context("2026-03-01")
        _rollup_day(ctx, "analytics.strategy_pnl_1min_prod_v2", "analytics.strategy_pnl_1hour_prod_v2")

        insert_call = mock_execute.call_args_list[1]
        sql = insert_call[0][0]
        assert "INSERT INTO analytics.strategy_pnl_1hour_prod_v2" in sql
        assert "FROM analytics.strategy_pnl_1min_prod_v2" in sql
        assert "toStartOfHour" in sql

    @patch("trading_dagster.assets.pnl_rollup.get_client", return_value=MagicMock())
    @patch("trading_dagster.assets.pnl_rollup.query_scalar", return_value=0)
    @patch("trading_dagster.assets.pnl_rollup.execute")
    def test_metadata_contains_partition_and_rows(self, mock_execute, mock_scalar, mock_client):
        ctx = _make_context("2026-03-15")
        result = _rollup_day(ctx, "analytics.src", "analytics.tgt")
        assert result.metadata["partition"] == "2026-03-15"
        assert result.metadata["rows_inserted"] == 0

    @patch("trading_dagster.assets.pnl_rollup.get_client", return_value=MagicMock())
    @patch("trading_dagster.assets.pnl_rollup.query_scalar", return_value=5)
    @patch("trading_dagster.assets.pnl_rollup.execute")
    def test_execute_called_twice(self, mock_execute, mock_scalar, mock_client):
        """DELETE + INSERT = exactly two execute() calls."""
        ctx = _make_context("2026-03-01")
        _rollup_day(ctx, "analytics.src", "analytics.tgt")
        assert mock_execute.call_count == 2

    @patch("trading_dagster.assets.pnl_rollup.get_client", return_value=MagicMock())
    @patch("trading_dagster.assets.pnl_rollup.query_scalar", return_value=10)
    @patch("trading_dagster.assets.pnl_rollup.execute")
    def test_extra_agg_cols_appear_in_insert(self, mock_execute, mock_scalar, mock_client):
        """extra_agg_cols causes closing_ts, execution_ts, traded to appear in INSERT SQL."""
        ctx = _make_context("2026-03-01")
        _rollup_day(
            ctx,
            "analytics.strategy_pnl_1min_real_trade_v2",
            "analytics.strategy_pnl_1hour_real_trade_v2",
            extra_agg_cols="closing_ts, execution_ts, traded",
        )
        insert_sql = mock_execute.call_args_list[1][0][0]
        assert "closing_ts" in insert_sql
        assert "execution_ts" in insert_sql
        assert "traded" in insert_sql


class TestAssetPartitionDefs:
    def test_prod_rollup_uses_prod_start_date(self):
        assert pnl_1hour_prod_rollup_asset.partitions_def.start.strftime("%Y-%m-%d") == PROD_REAL_TRADE_START_DATE

    def test_real_trade_rollup_uses_prod_start_date(self):
        assert pnl_1hour_real_trade_rollup_asset.partitions_def.start.strftime("%Y-%m-%d") == PROD_REAL_TRADE_START_DATE

    def test_bt_rollup_uses_bt_start_date(self):
        assert pnl_1hour_bt_rollup_asset.partitions_def.start.strftime("%Y-%m-%d") == BT_START_DATE
