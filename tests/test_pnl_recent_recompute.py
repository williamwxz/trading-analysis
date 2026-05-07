"""Unit tests for the 3-day recent recompute additions."""

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
        """_resume_ecs_service calls update_service with desiredCount=1."""
        from trading_dagster.assets.pnl_strategy_v2 import _resume_ecs_service

        mock_client = MagicMock()
        _resume_ecs_service("trading-analysis-pnl-consumer-prod", "trading-analysis", mock_client)
        mock_client.update_service.assert_called_once_with(
            cluster="trading-analysis",
            service="trading-analysis-pnl-consumer-prod",
            desiredCount=1,
        )
