from datetime import datetime
from unittest.mock import patch

import pytest

from pnl_consumer.ch_lookup import fetch_strategies_for_candle


@pytest.mark.unit
def test_fetch_strategies_returns_list_of_strategy_bars():
    mock_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_id": 1,
            "strategy_name": "momentum",
            "underlying": "BTCUSDT",
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
    assert bars[0].underlying == "BTCUSDT"


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
            "underlying": "ETHUSDT",
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
