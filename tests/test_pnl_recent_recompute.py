"""Unit tests for the 3-day recent recompute additions."""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

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
        before = datetime(2026, 5, 3, 0, 0, 0)
        fetch_anchors("strategy_pnl_1min_prod_v2", "btc", before_ts=before)
        sql = mock_qd.call_args[0][0]
        assert "ts < toDateTime('2026-05-03 00:00:00')" in sql

    @patch("trading_dagster.utils.pnl_compute.query_dicts")
    def test_before_ts_returns_correct_anchors(self, mock_qd):
        """Rows returned by the filtered query are parsed into the anchor tuple."""
        mock_qd.return_value = [
            {
                "strategy_table_name": "s1",
                "anchor_pnl": 1.5,
                "anchor_price": 200.0,
                "anchor_position": 1.0,
            }
        ]
        before = datetime(2026, 5, 3)
        result = fetch_anchors("strategy_pnl_1min_prod_v2", "btc", before_ts=before)
        assert result == {"s1": (1.5, 200.0, 1.0)}


class TestEcsPauseResume:

    def test_pause_sets_desired_count_zero(self):
        """_pause_ecs_service calls update_service with desiredCount=0."""
        from trading_dagster.assets.pnl_strategy_v2 import _pause_ecs_service

        mock_client = MagicMock()
        mock_client.get_waiter.return_value = MagicMock()
        _pause_ecs_service("trading-analysis-pnl-consumer-prod", "trading-analysis", mock_client)
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
        _pause_ecs_service("trading-analysis-pnl-consumer-prod", "trading-analysis", mock_client)
        mock_client.get_waiter.assert_called_once_with("services_stable")
        waiter.wait.assert_called_once_with(
            cluster="trading-analysis",
            services=["trading-analysis-pnl-consumer-prod"],
        )

    def test_resume_sets_desired_count_one(self):
        """_resume_ecs_service defaults desiredCount=1."""
        from trading_dagster.assets.pnl_strategy_v2 import _resume_ecs_service

        mock_client = MagicMock()
        _resume_ecs_service("trading-analysis-pnl-consumer-prod", "trading-analysis", mock_client)
        mock_client.update_service.assert_called_once_with(
            cluster="trading-analysis",
            service="trading-analysis-pnl-consumer-prod",
            desiredCount=1,
        )

    def test_resume_desired_count_zero_keeps_service_stopped(self):
        """_resume_ecs_service with desired_count=0 keeps the service stopped (bt consumer case)."""
        from trading_dagster.assets.pnl_strategy_v2 import _resume_ecs_service

        mock_client = MagicMock()
        _resume_ecs_service("trading-analysis-pnl-consumer-bt", "trading-analysis", mock_client, desired_count=0)
        mock_client.update_service.assert_called_once_with(
            cluster="trading-analysis",
            service="trading-analysis-pnl-consumer-bt",
            desiredCount=0,
        )


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

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2.execute")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_bt_mode_sql_includes_cumulative_pnl(self, mock_gc, mock_fa, mock_exec, mock_qd, mock_prices, mock_insert):
        """bt mode SQL must include cumulative_pnl column (absent from prod SQL)."""
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
            "btc", "strategy_pnl_1min_bt_v2", "strategy_output_history_bt_v2",
            "backtest", PROD_INSERT_COLUMNS, "bt", window_start, end_dt,
        )
        bt_sql = mock_qd.call_args_list[0][0][0]
        assert "cumulative_pnl" in bt_sql
        assert "strategy_output_history_bt_v2" in bt_sql

    @patch("trading_dagster.assets.pnl_strategy_v2.insert_rows")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_prices_multi")
    @patch("trading_dagster.assets.pnl_strategy_v2.query_dicts")
    @patch("trading_dagster.assets.pnl_strategy_v2.execute")
    @patch("trading_dagster.assets.pnl_strategy_v2.fetch_anchors")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_real_trade_mode_sql_includes_revision_ts(self, mock_gc, mock_fa, mock_exec, mock_qd, mock_prices, mock_insert):
        """real_trade mode SQL must include revision_ts column (absent from prod/bt SQL)."""
        from datetime import datetime, UTC
        from trading_dagster.assets.pnl_strategy_v2 import _process_underlying_recent
        from trading_dagster.utils.pnl_compute import REAL_TRADE_INSERT_COLUMNS

        mock_fa.return_value = {}
        mock_qd.return_value = []
        mock_prices.return_value = {"btc": {}}
        mock_insert.return_value = 0

        window_start = datetime(2026, 5, 3, 0, 0, 0, tzinfo=UTC)
        end_dt = datetime(2026, 5, 6, 0, 0, 0, tzinfo=UTC)
        _process_underlying_recent(
            "btc", "strategy_pnl_1min_real_trade_v2", "strategy_output_history_v2",
            "real_trade", REAL_TRADE_INSERT_COLUMNS, "real_trade", window_start, end_dt,
        )
        rt_sql = mock_qd.call_args_list[0][0][0]
        assert "revision_ts" in rt_sql
        assert "strategy_output_history_v2" in rt_sql


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
    def test_bt_ecs_resume_count_is_zero(
        self, mock_gc, mock_get_und, mock_fa, mock_exec, mock_qd, mock_prices, mock_insert, mock_boto3
    ):
        """bt asset must restore ECS desiredCount=0 so the bt consumer stays stopped."""
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
            target_table="strategy_pnl_1min_bt_v2",
            source_table="strategy_output_history_bt_v2",
            label="backtest",
            insert_columns=PROD_INSERT_COLUMNS,
            mode="bt",
            ecs_service="trading-analysis-pnl-consumer-bt",
            ecs_resume_count=0,
        )

        resume_calls = [c for c in mock_ecs.update_service.call_args_list if c.kwargs.get("desiredCount") == 1]
        stay_stopped_calls = [c for c in mock_ecs.update_service.call_args_list if c.kwargs.get("desiredCount") == 0]
        assert len(resume_calls) == 0, "bt consumer must NOT be set to desiredCount=1"
        assert len(stay_stopped_calls) >= 1  # at least the pause call

    @patch("trading_dagster.assets.pnl_strategy_v2.boto3")
    @patch("trading_dagster.assets.pnl_strategy_v2._get_underlyings")
    @patch("trading_dagster.assets.pnl_strategy_v2.get_client")
    def test_consumer_resumed_even_when_pause_waiter_fails(
        self, mock_gc, mock_get_und, mock_boto3
    ):
        """ECS update_service(desiredCount=1) must be called even when the pause waiter raises."""
        from trading_dagster.assets.pnl_strategy_v2 import _recompute_pnl_recent
        from trading_dagster.utils.pnl_compute import PROD_INSERT_COLUMNS

        mock_ecs = MagicMock()
        waiter = MagicMock()
        waiter.wait.side_effect = RuntimeError("waiter timeout")
        mock_ecs.get_waiter.return_value = waiter
        mock_boto3.client.return_value = mock_ecs
        mock_get_und.return_value = ["btc"]

        with pytest.raises(RuntimeError, match="waiter timeout"):
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
