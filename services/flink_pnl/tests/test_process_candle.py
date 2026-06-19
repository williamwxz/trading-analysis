from datetime import datetime
from unittest.mock import patch

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.candle_lookup import BtLiveAnchor, StrategyBar, StrategyRevision
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


def _bt_anchor(
    stn: str = "strat_btc_5m",
    cum_pnl_first: float = 0.1,
    pos_first: float = 1.0,
    anchor_price: float = 49000.0,
) -> BtLiveAnchor:
    return BtLiveAnchor(
        strategy_table_name=stn,
        strategy_instance_id="sid_bt",
        strategy_id=3,
        strategy_name="bt_mom",
        underlying="BTC",
        config_timeframe="5m",
        weighting=1.0,
        cum_pnl_first=cum_pnl_first,
        pos_first=pos_first,
        anchor_ts="2026-05-15 09:55:00",
        anchor_price=anchor_price,
        benchmark=0.0,
    )


@pytest.mark.unit
def test_all_sinks_false_returns_empty():
    cfg = SinkConfig(price=False, prod=False, bt=False, real_trade=False)
    rows, prod_f, bt_f, rt_f = process_candle(_candle(), {}, {}, cfg)
    assert rows == []
    assert prod_f == bt_f == rt_f == 0


@pytest.mark.unit
def test_price_sink_flag_produces_no_rows():
    """flink_pnl ignores the price sink — enabling it should produce no price rows."""
    cfg = SinkConfig(price=True, prod=False, bt=False, real_trade=False)
    rows, prod_f, bt_f, rt_f = process_candle(_candle(), {}, {}, cfg)
    assert rows == []
    assert prod_f == bt_f == rt_f == 0


@pytest.mark.unit
def test_prod_new_bar_emits_pnl_row_and_fetched_count():
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
        rows, prod_fetched, bt_fetched, rt_fetched = process_candle(candle, state_prod, {}, cfg)

    assert prod_fetched == 1
    assert bt_fetched == 0
    assert rt_fetched == 0

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert len(pnl_rows) == 1
    row_data = pnl_rows[0]["_row"]
    # price must be candle.open = 50000.0
    assert row_data[11] == 50000.0  # price is index 11
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
        rows, prod_fetched, _, _ = process_candle(candle, state_prod, {}, cfg)

    # fetched count is 0 (no bars from query) but a carry-forward row is still emitted
    assert prod_fetched == 0
    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert len(pnl_rows) == 1
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
        rows, _, _, _ = process_candle(candle, state_prod, {}, cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert pnl_rows == []


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
        bar_ts=datetime(2026, 5, 15, 10, 0, 0),       # newer bar already active
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
        bar_ts=datetime(2026, 5, 15, 9, 55, 0),    # older bar — guard must block
        revision_ts=datetime(2026, 5, 15, 9, 59, 0),
    )

    with patch("flink_pnl.process_candle.fetch_real_trade_for_candle", return_value=[stale_rev]):
        rows, _, _, rt_fetched = process_candle(candle, {}, state_rt, cfg)

    assert rt_fetched == 1  # revision was fetched
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
        process_candle(candle, {}, {}, cfg)

    mock_fetch.assert_called_once_with("BTCUSDT", candle.ts)


@pytest.mark.unit
def test_bt_fetched_count_matches_anchors_returned():
    cfg = SinkConfig(price=False, prod=False, bt=True, real_trade=False)
    candle = _candle()

    anchors = [_bt_anchor("strat_btc_5m"), _bt_anchor("strat_btc_1h")]
    with patch(
        "flink_pnl.process_candle.fetch_bt_anchors_for_candle", return_value=anchors
    ):
        _, prod_f, bt_f, rt_f = process_candle(candle, {}, {}, cfg)

    assert prod_f == 0
    assert bt_f == 2
    assert rt_f == 0


@pytest.mark.unit
def test_bt_stateless_cpnl_when_anchor_price_equals_candle_open():
    """When anchor_price == candle.open, cpnl == cum_pnl_first (zero price move)."""
    cfg = SinkConfig(price=False, prod=False, bt=True, real_trade=False)
    candle = _candle(open_price=50000.0)
    anchor = _bt_anchor(cum_pnl_first=0.05, pos_first=1.0, anchor_price=50000.0)

    with patch(
        "flink_pnl.process_candle.fetch_bt_anchors_for_candle", return_value=[anchor]
    ):
        rows, _, bt_f, _ = process_candle(candle, {}, {}, cfg)

    assert bt_f == 1
    pnl_rows = [r for r in rows if r["_sink"] == "pnl_bt"]
    assert len(pnl_rows) == 1
    row_data = pnl_rows[0]["_row"]
    # cpnl = cum_pnl_first + pos * (50000 - 50000) / 50000 == cum_pnl_first
    assert abs(row_data[8] - 0.05) < 1e-9  # cumulative_pnl at index 8
    assert row_data[11] == 50000.0  # price at index 11
