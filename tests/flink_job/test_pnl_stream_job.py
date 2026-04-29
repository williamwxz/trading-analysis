"""
Integration test for the PyFlink PnL stream job.
Runs without a real Flink cluster — tests the process_candle() function
which contains all the business logic (lookup + anchor + compute).
"""

from datetime import datetime
from unittest.mock import patch

import pytest

from flink_job.anchor_state import AnchorRecord, AnchorState
from flink_job.ch_lookup import StrategyBar
from flink_job.pnl_stream_job import process_candle
from streaming.models import CandleEvent


def _make_candle(instrument="BTCUSDT", close=93200.0) -> CandleEvent:
    return CandleEvent(
        exchange="binance",
        instrument=instrument,
        ts=datetime(2026, 4, 26, 0, 1, 0),
        open=93100.0,
        high=93250.0,
        low=93050.0,
        close=close,
        volume=12.34,
    )


def _make_strategy(position=1.0, instrument="BTCUSDT") -> StrategyBar:
    return StrategyBar(
        strategy_table_name="strat_prod_1",
        strategy_id=1,
        strategy_name="momentum",
        underlying=instrument,
        config_timeframe="5m",
        weighting=1.0,
        position=position,
        final_signal=1.0,
        benchmark=0.0,
    )


@pytest.mark.unit
def test_process_candle_produces_pnl_rows():
    state = AnchorState()
    state.update(
        "strat_prod_1",
        AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0),
    )
    candle = _make_candle(close=93200.0)
    strategies = [_make_strategy(position=1.0)]

    with patch(
        "flink_job.pnl_stream_job.fetch_strategies_for_candle",
        return_value=strategies,
    ):
        rows = process_candle(candle, state)

    pnl_rows = [r for r in rows if r.get("_sink") == "pnl"]
    assert len(pnl_rows) == 1
    row = pnl_rows[0]
    assert row["strategy_table_name"] == "strat_prod_1"
    assert row["underlying"] == "BTCUSDT"
    assert row["price"] == 93200.0
    # pnl = 0.0 + 1.0 * (93200 - 93100) / 93100 ≈ 0.001074
    assert abs(row["cumulative_pnl"] - (100.0 / 93100.0)) < 1e-6


@pytest.mark.unit
def test_process_candle_carry_forward_when_no_strategies():
    state = AnchorState()
    candle = _make_candle()

    with patch("flink_job.pnl_stream_job.fetch_strategies_for_candle", return_value=[]):
        rows = process_candle(candle, state)

    pnl_rows = [r for r in rows if r.get("_sink") == "pnl"]
    assert pnl_rows == []


@pytest.mark.unit
def test_process_candle_writes_candle_to_price_table():
    state = AnchorState()
    candle = _make_candle()
    strategies = [_make_strategy()]

    with patch(
        "flink_job.pnl_stream_job.fetch_strategies_for_candle",
        return_value=strategies,
    ):
        rows = process_candle(candle, state)

    # price row is always emitted regardless of strategies
    price_row = next((r for r in rows if r.get("_sink") == "price"), None)
    assert price_row is not None
    assert price_row["instrument"] == "BTCUSDT"
