from datetime import datetime
from unittest.mock import patch

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.candle_lookup import StrategyBar, StrategyRevision
from flink_pnl.sink_config import SinkConfig
from flink_pnl.state import StateMap, build_state_from_bootstrap
from flink_pnl.process_candle import process_candle
from streaming.models import CandleEvent


def _candle(instrument: str = "BTCUSDT", open_price: float = 50000.0) -> CandleEvent:
    return CandleEvent(
        exchange="binance",
        instrument=instrument,
        ts=datetime(2026, 5, 15, 10, 0, 0),
        open=open_price,
        high=open_price + 100,
        low=open_price - 100,
        close=open_price + 50,
        volume=1.0,
    )


def _bar(stn: str = "strat_btc_5m", underlying: str = "BTC", position: float = 1.0) -> StrategyBar:
    return StrategyBar(
        strategy_table_name=stn,
        strategy_instance_id="sid1",
        strategy_id=1,
        strategy_name="momentum",
        underlying=underlying,
        config_timeframe="5m",
        weighting=1.0,
        position=position,
        final_signal=1.0,
        benchmark=0.0,
        bar_ts=datetime(2026, 5, 15, 9, 55, 0),
    )


def _revision(stn: str = "strat_btc_rt", underlying: str = "BTC", position: float = 0.5) -> StrategyRevision:
    return StrategyRevision(
        strategy_table_name=stn,
        strategy_instance_id="sid_rt",
        strategy_id=2,
        strategy_name="real_trade_mom",
        underlying=underlying,
        config_timeframe="5m",
        weighting=1.0,
        position=position,
        final_signal=0.5,
        benchmark=0.0,
        bar_ts=datetime(2026, 5, 15, 9, 55, 0),
        revision_ts=datetime(2026, 5, 15, 9, 59, 0),
    )


@pytest.mark.unit
def test_price_sink_emits_price_row():
    cfg = SinkConfig(price=True, prod=False, bt=False, real_trade=False)
    candle = _candle()

    rows = process_candle(candle, {}, {}, {}, cfg)

    price_rows = [r for r in rows if r["_sink"] == "price"]
    assert len(price_rows) == 1
    assert price_rows[0]["instrument"] == "BTCUSDT"
    assert price_rows[0]["open"] == 50000.0


@pytest.mark.unit
def test_all_sinks_false_returns_empty():
    cfg = SinkConfig(price=False, prod=False, bt=False, real_trade=False)
    rows = process_candle(_candle(), {}, {}, {}, cfg)
    assert rows == []


@pytest.mark.unit
def test_prod_new_bar_emits_pnl_row():
    cfg = SinkConfig(price=False, prod=True, bt=False, real_trade=False)
    candle = _candle(open_price=50000.0)

    anchor = AnchorState()
    anchor.set("strat_btc_5m", AnchorRecord(
        pnl=0.0, price=49000.0, position=1.0,
        underlying="BTC", strategy_instance_id="sid1",
        strategy_id=1, strategy_name="momentum",
        config_timeframe="5m", weighting=1.0,
        final_signal=1.0, benchmark=0.0,
    ))
    state_prod = build_state_from_bootstrap(anchor)

    bar = _bar(position=1.0)
    with patch("flink_pnl.process_candle.fetch_strategies_for_candle", return_value=[bar]):
        rows = process_candle(candle, state_prod, {}, {}, cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert len(pnl_rows) == 1
    row_data = pnl_rows[0]["_row"]
    # price must be candle.open = 50000.0, not from ClickHouse
    assert row_data[11] == 50000.0  # price is index 11 in INSERT_COLUMNS
    # cumulative_pnl = 0.0 + 1.0 * (50000 - 49000) / 49000
    expected_pnl = 0.0 + 1.0 * (50000.0 - 49000.0) / 49000.0
    assert abs(row_data[8] - expected_pnl) < 1e-9  # cumulative_pnl is index 8


@pytest.mark.unit
def test_prod_carry_forward_when_no_new_bar():
    cfg = SinkConfig(price=False, prod=True, bt=False, real_trade=False)
    candle = _candle(instrument="BTCUSDT", open_price=50000.0)

    anchor = AnchorState()
    anchor.set("strat_btc_5m", AnchorRecord(
        pnl=1.0, price=49000.0, position=1.0,
        underlying="BTC", strategy_instance_id="sid1",
        strategy_id=1, strategy_name="momentum",
        config_timeframe="5m", weighting=1.0,
        final_signal=1.0, benchmark=0.0,
    ))
    state_prod = build_state_from_bootstrap(anchor)

    with patch("flink_pnl.process_candle.fetch_strategies_for_candle", return_value=[]):
        rows = process_candle(candle, state_prod, {}, {}, cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert len(pnl_rows) == 1  # carry-forward row emitted
    row_data = pnl_rows[0]["_row"]
    assert row_data[11] == 50000.0  # price updated to candle.open
    assert row_data[10] == 1.0      # position unchanged (index 10)


@pytest.mark.unit
def test_carry_forward_only_fires_for_matching_underlying():
    """A SOL candle must not carry-forward BTC strategies."""
    cfg = SinkConfig(price=False, prod=True, bt=False, real_trade=False)
    candle = _candle(instrument="SOLUSDT", open_price=150.0)

    anchor = AnchorState()
    anchor.set("strat_btc_5m", AnchorRecord(
        pnl=1.0, price=49000.0, position=1.0,
        underlying="BTC", strategy_instance_id="sid1",
    ))
    state_prod = build_state_from_bootstrap(anchor)

    with patch("flink_pnl.process_candle.fetch_strategies_for_candle", return_value=[]):
        rows = process_candle(candle, state_prod, {}, {}, cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert pnl_rows == []  # BTC strategy must NOT be carry-forwarded on SOL candle


@pytest.mark.unit
def test_real_trade_revision_guard_blocks_stale_revision():
    cfg = SinkConfig(price=False, prod=False, bt=False, real_trade=True)
    candle = _candle()

    anchor = AnchorState()
    anchor.set("strat_btc_rt", AnchorRecord(
        pnl=1.0, price=49000.0, position=0.5,
        underlying="BTC", strategy_instance_id="sid_rt",
        strategy_id=2, strategy_name="rt",
        config_timeframe="5m", weighting=1.0,
        final_signal=0.5, benchmark=0.0,
        bar_ts=datetime(2026, 5, 15, 10, 0, 0),      # newer bar already active
        revision_ts=datetime(2026, 5, 15, 10, 0, 30),
    ))
    state_rt = build_state_from_bootstrap(anchor)

    stale_rev = StrategyRevision(
        strategy_table_name="strat_btc_rt",
        strategy_instance_id="sid_rt",
        strategy_id=2,
        strategy_name="rt",
        underlying="BTC",
        config_timeframe="5m",
        weighting=1.0,
        position=0.0,
        final_signal=0.0,
        benchmark=0.0,
        bar_ts=datetime(2026, 5, 15, 9, 55, 0),   # older bar — guard must block this
        revision_ts=datetime(2026, 5, 15, 9, 59, 0),
    )

    with patch("flink_pnl.process_candle.fetch_real_trade_for_candle", return_value=[stale_rev]):
        rows = process_candle(candle, {}, {}, state_rt, cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_real_trade"]
    assert len(pnl_rows) == 1  # carry-forward emitted (guard blocked new position)
    row_data = pnl_rows[0]["_row"]
    assert row_data[10] == 0.5  # position from anchor unchanged (index 10)


@pytest.mark.unit
def test_fetch_strategies_receives_full_instrument_not_underlying():
    """fetch_strategies_for_candle must receive 'BTCUSDT', not 'BTC'."""
    cfg = SinkConfig(price=False, prod=True, bt=False, real_trade=False)
    candle = _candle(instrument="BTCUSDT")

    with patch("flink_pnl.process_candle.fetch_strategies_for_candle", return_value=[]) as mock_fetch:
        process_candle(candle, {}, {}, {}, cfg)

    mock_fetch.assert_called_once_with("BTCUSDT", candle.ts)
