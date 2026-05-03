from datetime import datetime
from unittest.mock import patch

import pytest

from pnl_consumer.ch_lookup import (
    StrategyRevision,
    fetch_real_trade_revisions_for_candle,
    fetch_strategies_for_candle,
)


@pytest.mark.unit
def test_fetch_strategies_returns_list_of_strategy_bars():
    # ClickHouse stores short names (BTC, ETH); instrument arg is full symbol (BTCUSDT)
    mock_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_id": 1,
            "strategy_name": "momentum",
            "underlying": "BTC",
            "config_timeframe": "5m",
            "weighting": 1.0,
            "row_json": '{"position": 0.5, "final_signal": 1.0, "benchmark": 0.0}',
        }
    ]
    with patch("pnl_consumer.ch_lookup.query_dicts", return_value=mock_rows):
        bars = fetch_strategies_for_candle(
            instrument="BTCUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 0, 0),
        )
    assert len(bars) == 1
    assert bars[0].strategy_table_name == "strat_prod_1"
    assert bars[0].position == 0.5
    assert bars[0].underlying == "BTC"


@pytest.mark.unit
def test_fetch_strategies_returns_empty_list_when_no_rows():
    with patch("pnl_consumer.ch_lookup.query_dicts", return_value=[]):
        bars = fetch_strategies_for_candle(
            instrument="FETUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 0, 0),
        )
    assert bars == []


@pytest.mark.unit
def test_strategy_bar_position_parsed_from_row_json():
    mock_rows = [
        {
            "strategy_table_name": "strat_prod_2",
            "strategy_id": 2,
            "strategy_name": "mean_rev",
            "underlying": "ETH",
            "config_timeframe": "15m",
            "weighting": 0.5,
            "row_json": '{"position": -1.0, "final_signal": -1.0, "benchmark": 0.01}',
        }
    ]
    with patch("pnl_consumer.ch_lookup.query_dicts", return_value=mock_rows):
        bars = fetch_strategies_for_candle(
            instrument="ETHUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 0, 0),
        )
    assert bars[0].position == -1.0
    assert bars[0].final_signal == -1.0
    assert bars[0].benchmark == 0.01


@pytest.mark.unit
def test_fetch_real_trade_revisions_returns_list_of_strategy_revisions():
    mock_rows = [
        {
            "strategy_table_name": "strat_rt_1",
            "strategy_id": 1,
            "strategy_name": "momentum",
            "underlying": "BTC",
            "config_timeframe": "5m",
            "weighting": 1.0,
            "revision_ts": datetime(2026, 4, 26, 0, 1, 10),
            "closing_ts": datetime(2026, 4, 26, 0, 5, 59),
            "row_json": '{"position": 0.5, "final_signal": 1.0, "benchmark": 0.0}',
        }
    ]
    with patch("pnl_consumer.ch_lookup.query_dicts", return_value=mock_rows):
        revisions = fetch_real_trade_revisions_for_candle(
            instrument="BTCUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 1, 0),
        )
    assert len(revisions) == 1
    rev = revisions[0]
    assert isinstance(rev, StrategyRevision)
    assert rev.strategy_table_name == "strat_rt_1"
    assert rev.position == 0.5
    assert rev.revision_ts == datetime(2026, 4, 26, 0, 1, 10)
    assert rev.closing_ts == datetime(2026, 4, 26, 0, 5, 59)


@pytest.mark.unit
def test_fetch_real_trade_revisions_returns_empty_when_no_rows():
    with patch("pnl_consumer.ch_lookup.query_dicts", return_value=[]):
        revisions = fetch_real_trade_revisions_for_candle(
            instrument="BTCUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 1, 0),
        )
    assert revisions == []


@pytest.mark.unit
def test_fetch_real_trade_revisions_filters_by_exact_candle_ts():
    """Verify the SQL uses ts = candle_ts (not a range)."""
    captured_sql = []

    def capture(sql):
        captured_sql.append(sql)
        return []

    with patch("pnl_consumer.ch_lookup.query_dicts", side_effect=capture):
        fetch_real_trade_revisions_for_candle(
            instrument="BTCUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 1, 0),
        )
    assert "2026-04-26 00:01:00" in captured_sql[0]
    assert "revision_ts <= closing_ts" in captured_sql[0]
