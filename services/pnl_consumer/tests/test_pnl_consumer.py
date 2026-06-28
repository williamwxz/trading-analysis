"""Unit tests for pnl_consumer.pnl_consumer.

Tests the orchestration layer only — no ClickHouse, no Kafka.
Computation logic is in libs.computation and tested separately.
"""
import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from libs.computation import AnchorRecord, AnchorState, BtLiveAnchor, StrategyBar, StrategyRevision
from libs.computation.bootstrap import BootstrapSeed, LastPnlAnchor, WalkRow
from pnl_consumer.pnl_consumer import (
    SinkConfig,
    _bootstrap_state,
    _flush_candle,
    emit_candle_metrics,
    process_candle,
    resolve_group_id,
)
from streaming.models import CandleEvent

_MOD = "pnl_consumer.pnl_consumer"

_CANDLE_TS = datetime(2026, 4, 26, 2, 6, 0)


def _candle(instrument="BTCUSDT", open=93200.0, ts=None) -> CandleEvent:
    return CandleEvent(
        exchange="binance",
        instrument=instrument,
        ts=ts or _CANDLE_TS,
        open=open,
        high=93250.0,
        low=93050.0,
        close=93100.0,
        volume=12.34,
    )


def _bar(
    stn="strat_prod_1",
    siid="inst_001",
    position=1.0,
    underlying="BTC",
    bar_ts=None,
) -> StrategyBar:
    return StrategyBar(
        strategy_table_name=stn,
        strategy_instance_id=siid,
        strategy_id=1,
        strategy_name="momentum",
        underlying=underlying,
        config_timeframe="5m",
        weighting=1.0,
        position=position,
        final_signal=1.0,
        benchmark=0.0,
        bar_ts=bar_ts or _CANDLE_TS,
    )


def _revision(
    stn="strat_rt_1",
    siid="inst_rt_001",
    position=1.0,
    underlying="BTC",
    bar_ts=None,
    revision_ts=None,
) -> StrategyRevision:
    return StrategyRevision(
        strategy_table_name=stn,
        strategy_instance_id=siid,
        strategy_id=1,
        strategy_name="momentum",
        underlying=underlying,
        config_timeframe="5m",
        weighting=1.0,
        position=position,
        final_signal=1.0,
        benchmark=0.0,
        bar_ts=bar_ts or datetime(2026, 4, 26, 1, 0, 0),
        revision_ts=revision_ts or datetime(2026, 4, 26, 2, 5, 30),
    )


# ─────────────────────────────────────────────────────────────────────────────
# peek_reference_ts — regression: hard-fail on Kafka errors, never fall back
# to now() (would let later commits silently advance past unprocessed messages)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_peek_reference_ts_raises_on_list_topics_failure():
    """If the broker is unreachable, peek_reference_ts MUST raise — not return
    None — so the caller exits before constructing a Consumer or committing
    anything. Was a real bug observed 2026-06-09: a NAT-EIP rate-limit by
    ClickHouse Cloud (separate from Kafka) caused both bt and prod consumers
    to crash-loop, and the prior behavior (fall back to now() on Kafka
    errors) would have silently advanced offsets past the backlog on the
    first successful retry.
    """
    from pnl_consumer.pnl_consumer import peek_reference_ts

    class _BrokenConsumer:
        def list_topics(self, _topic, timeout):
            raise RuntimeError("broker unreachable")

        def close(self):
            pass

    with patch(f"{_MOD}.Consumer", return_value=_BrokenConsumer()):
        with pytest.raises(RuntimeError, match="list_topics"):
            peek_reference_ts("broker:9092", "grp")


@pytest.mark.unit
def test_peek_reference_ts_raises_on_committed_failure():
    """Same intent as above — failure when fetching committed offsets must
    raise, not fall back."""
    from pnl_consumer.pnl_consumer import peek_reference_ts

    class _Meta:
        topics = {}  # empty: no partitions

    class _PartialBroken:
        def list_topics(self, _topic, timeout):
            return _Meta()

        def committed(self, _partitions, timeout):
            raise RuntimeError("group coordinator down")

        def close(self):
            pass

    with patch(f"{_MOD}.Consumer", return_value=_PartialBroken()):
        with pytest.raises(RuntimeError, match="committed"):
            peek_reference_ts("broker:9092", "grp")


@pytest.mark.unit
def test_peek_reference_ts_returns_none_for_genuinely_empty_topic():
    """When the topic exists, all partitions are queried, and there's no
    candle to read, the function returns None (caller treats this as a fresh
    deploy — seed from now() with no walk-verify). This is the ONE remaining
    None-return path; broker errors raise."""
    from pnl_consumer.pnl_consumer import peek_reference_ts

    class _Meta:
        topics = {}  # topic not in metadata → 0 partitions → no commits to read

    class _EmptyTopic:
        def list_topics(self, _topic, timeout):
            return _Meta()

        def committed(self, _partitions, timeout):
            return []

        def close(self):
            pass

    with patch(f"{_MOD}.Consumer", return_value=_EmptyTopic()):
        assert peek_reference_ts("broker:9092", "grp") is None


# ─────────────────────────────────────────────────────────────────────────────
# SinkConfig
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_sink_config_defaults():
    cfg = SinkConfig.from_env({})
    assert cfg.price is True
    assert cfg.prod is False
    assert cfg.real_trade is False
    assert cfg.bt is False


@pytest.mark.unit
def test_sink_config_enables_prod():
    cfg = SinkConfig.from_env({"ENABLE_PROD_SINK": "true"})
    assert cfg.prod is True


@pytest.mark.unit
def test_sink_config_enables_all():
    cfg = SinkConfig.from_env({
        "ENABLE_PRICE_SINK": "true",
        "ENABLE_PROD_SINK": "true",
        "ENABLE_REAL_TRADE_SINK": "true",
        "ENABLE_BT_SINK": "true",
    })
    assert all([cfg.price, cfg.prod, cfg.real_trade, cfg.bt])


@pytest.mark.unit
def test_sink_config_disables_price():
    cfg = SinkConfig.from_env({"ENABLE_PRICE_SINK": "false"})
    assert cfg.price is False


# ─────────────────────────────────────────────────────────────────────────────
# resolve_group_id
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_resolve_group_id_from_env():
    assert resolve_group_id({"KAFKA_GROUP_ID": "my-group"}) == "my-group"


@pytest.mark.unit
def test_resolve_group_id_default():
    assert resolve_group_id({}) == "flink-pnl-consumer"


# ─────────────────────────────────────────────────────────────────────────────
# process_candle — price row
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_process_candle_always_emits_price_row():
    candle = _candle()
    cfg = SinkConfig(price=True, prod=False, real_trade=False, bt=False)
    rows, _, _, _ = process_candle(candle, AnchorState(), AnchorState(),
                          cfg)
    price_rows = [r for r in rows if r["_sink"] == "price"]
    assert len(price_rows) == 1
    assert price_rows[0]["instrument"] == "BTCUSDT"
    assert price_rows[0]["open"] == candle.open


@pytest.mark.unit
def test_process_candle_no_price_row_when_disabled():
    candle = _candle()
    cfg = SinkConfig(price=False, prod=False, real_trade=False, bt=False)
    rows, _, _, _ = process_candle(candle, AnchorState(), AnchorState(),
                          cfg)
    assert rows == []


# ─────────────────────────────────────────────────────────────────────────────
# process_candle — prod
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_process_candle_prod_computes_pnl():
    """Prod bar produces a pnl_prod row with correct PnL formula."""
    state = AnchorState()
    state.set("strat_prod_1", AnchorRecord(pnl=0.0, price=93100.0, position=1.0))
    candle = _candle(open=93200.0)
    bar = _bar(position=1.0)
    cfg = SinkConfig(price=False, prod=True, real_trade=False, bt=False)

    with patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[bar]):
        rows, _, _, _ = process_candle(candle, state, AnchorState(),
                              cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert len(pnl_rows) == 1
    row = pnl_rows[0]["_row"]
    assert row[0] == "strat_prod_1"   # strategy_table_name
    assert row[11] == 93200.0          # price
    assert row[5] == "production"      # source
    expected_pnl = 0.0 + 1.0 * (93200.0 - 93100.0) / 93100.0
    assert row[8] == pytest.approx(expected_pnl)


@pytest.mark.unit
def test_process_candle_prod_lazy_seeds_new_strategy():
    """A strategy not in state is seeded from zero (pnl=0, price=candle.open)."""
    candle = _candle(open=93200.0)
    bar = _bar(position=1.0)
    cfg = SinkConfig(price=False, prod=True, real_trade=False, bt=False)

    with patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[bar]):
        rows, _, _, _ = process_candle(candle, AnchorState(), AnchorState(),
                              cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert len(pnl_rows) == 1
    # First minute: seeded at candle.open, so pnl = 0
    assert pnl_rows[0]["_row"][8] == pytest.approx(0.0)


@pytest.mark.unit
def test_process_candle_prod_disabled_emits_no_rows():
    cfg = SinkConfig(price=True, prod=False, real_trade=False, bt=False)
    candle = _candle()
    bar = _bar()
    with patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[bar]):
        rows, _, _, _ = process_candle(candle, AnchorState(), AnchorState(),
                              cfg)
    assert [r for r in rows if r["_sink"] == "pnl_prod"] == []


# ─────────────────────────────────────────────────────────────────────────────
# process_candle — real_trade (AnchorState revision guard)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_process_candle_real_trade_applies_new_revision():
    """Revision with newer (bar_ts, revision_ts) than anchor is applied."""
    state = AnchorState()
    state.set("strat_rt_1", AnchorRecord(pnl=0.0, price=93100.0))
    candle = _candle(open=93200.0)
    bar_ts = datetime(2026, 4, 26, 1, 0, 0)
    rev_ts = datetime(2026, 4, 26, 2, 5, 30)
    rev = _revision(bar_ts=bar_ts, revision_ts=rev_ts, position=1.0)
    cfg = SinkConfig(price=False, prod=False, real_trade=True, bt=False)

    with patch(f"{_MOD}.fetch_real_trade_for_candle", return_value=[rev]):
        rows, _, _, _ = process_candle(candle, AnchorState(), state,
                              cfg)

    rt_rows = [r for r in rows if r["_sink"] == "pnl_real_trade"]
    assert len(rt_rows) == 1
    assert rt_rows[0]["_row"][5] == "real_trade"  # source
    expected_pnl = 0.0 + 1.0 * (93200.0 - 93100.0) / 93100.0
    assert rt_rows[0]["_row"][8] == pytest.approx(expected_pnl)


@pytest.mark.unit
def test_process_candle_real_trade_ignores_stale_revision():
    """Revision for an older bar than the anchor's bar_ts is ignored."""
    anchor_bar_ts = datetime(2026, 4, 26, 2, 0, 0)
    state = AnchorState()
    state.set("strat_rt_1", AnchorRecord(
        pnl=0.1, price=93100.0,
        bar_ts=anchor_bar_ts,
        revision_ts=datetime(2026, 4, 26, 2, 3, 0),
    ))
    candle = _candle(open=93200.0)
    # Old bar — should be ignored
    stale_bar_ts = datetime(2026, 4, 26, 1, 0, 0)
    stale_rev = _revision(bar_ts=stale_bar_ts, revision_ts=datetime(2026, 4, 26, 2, 55, 0))
    cfg = SinkConfig(price=False, prod=False, real_trade=True, bt=False)

    with patch(f"{_MOD}.fetch_real_trade_for_candle", return_value=[stale_rev]):
        rows, _, _, _ = process_candle(candle, AnchorState(), state,
                              cfg)

    assert [r for r in rows if r["_sink"] == "pnl_real_trade"] == []


@pytest.mark.unit
def test_process_candle_real_trade_ignores_same_revision_twice():
    """Same (bar_ts, revision_ts) as anchor: not re-applied."""
    bar_ts = datetime(2026, 4, 26, 1, 0, 0)
    rev_ts = datetime(2026, 4, 26, 2, 5, 30)
    state = AnchorState()
    state.set("strat_rt_1", AnchorRecord(
        pnl=0.05, price=93100.0, bar_ts=bar_ts, revision_ts=rev_ts,
    ))
    candle = _candle(open=93200.0)
    rev = _revision(bar_ts=bar_ts, revision_ts=rev_ts)
    cfg = SinkConfig(price=False, prod=False, real_trade=True, bt=False)

    with patch(f"{_MOD}.fetch_real_trade_for_candle", return_value=[rev]):
        rows, _, _, _ = process_candle(candle, AnchorState(), state,
                              cfg)

    assert [r for r in rows if r["_sink"] == "pnl_real_trade"] == []


@pytest.mark.unit
def test_process_candle_real_trade_same_bar_newer_revision_applies():
    """Same bar, newer revision_ts than anchor: applied."""
    bar_ts = datetime(2026, 4, 26, 1, 0, 0)
    state = AnchorState()
    state.set("strat_rt_1", AnchorRecord(
        pnl=0.0, price=93100.0, bar_ts=bar_ts,
        revision_ts=datetime(2026, 4, 26, 2, 3, 0),
    ))
    candle = _candle(open=93200.0)
    newer_rev = _revision(bar_ts=bar_ts, revision_ts=datetime(2026, 4, 26, 2, 8, 0))
    cfg = SinkConfig(price=False, prod=False, real_trade=True, bt=False)

    with patch(f"{_MOD}.fetch_real_trade_for_candle", return_value=[newer_rev]):
        rows, _, _, _ = process_candle(candle, AnchorState(), state,
                              cfg)

    assert len([r for r in rows if r["_sink"] == "pnl_real_trade"]) == 1


@pytest.mark.unit
def test_process_candle_real_trade_lazy_seeds_new_strategy():
    """Brand-new strategy not in state is seeded from zero."""
    candle = _candle(open=93200.0)
    rev = _revision(position=1.0)
    cfg = SinkConfig(price=False, prod=False, real_trade=True, bt=False)

    with patch(f"{_MOD}.fetch_real_trade_for_candle", return_value=[rev]):
        rows, _, _, _ = process_candle(candle, AnchorState(), AnchorState(),
                              cfg)

    rt_rows = [r for r in rows if r["_sink"] == "pnl_real_trade"]
    assert len(rt_rows) == 1
    assert rt_rows[0]["_row"][8] == pytest.approx(0.0)  # first minute pnl = 0


# ─────────────────────────────────────────────────────────────────────────────
# process_candle — bt
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_process_candle_bt_chains_from_state():
    """Stateful BT: cpnl chains minute-to-minute via state_bt.

    A brand-new strategy lazy-seeds cum_pnl_first with price=0, so candle 1 holds
    cum_pnl_first; candle 2 chains: cpnl = cum_pnl_first + pos*(open2-open1)/open1.
    """

    def _bt_anchor():
        return BtLiveAnchor(
            strategy_table_name="strat_bt_1",
            strategy_instance_id="inst_bt_001",
            strategy_id=2,
            strategy_name="bt_mom",
            underlying="BTC",
            config_timeframe="5m",
            weighting=1.0,
            cum_pnl_first=0.30,
            pos_first=2.0,
            anchor_ts=_CANDLE_TS.strftime("%Y-%m-%d %H:%M:%S"),
            benchmark=0.0,
        )

    cfg = SinkConfig(price=False, prod=False, real_trade=False, bt=True)
    state_bt = AnchorState()

    with patch(f"{_MOD}.fetch_bt_anchors_for_candle", return_value=[_bt_anchor()]):
        rows1, _, _, _ = process_candle(
            _candle(open=100.0), AnchorState(), AnchorState(), cfg, state_bt
        )
    with patch(f"{_MOD}.fetch_bt_anchors_for_candle", return_value=[_bt_anchor()]):
        rows2, _, _, _ = process_candle(
            _candle(open=110.0), AnchorState(), AnchorState(), cfg, state_bt
        )

    r1 = [r for r in rows1 if r["_sink"] == "pnl_bt"][0]["_row"]
    r2 = [r for r in rows2 if r["_sink"] == "pnl_bt"][0]["_row"]
    # candle 1 lazy-seeds and holds cum_pnl_first
    assert r1[8] == pytest.approx(0.30)
    # candle 2 chains: 0.30 + 2.0*(110-100)/100 = 0.50
    assert r2[8] == pytest.approx(0.30 + 2.0 * (110 - 100) / 100)
    assert r2[10] == 2.0  # position = pos_first


# ─────────────────────────────────────────────────────────────────────────────
# _flush_candle
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_flush_candle_writes_all_four_tables():
    consumer = MagicMock()
    price_row = ["binance", "BTCUSDT", datetime(2026, 4, 26), 93100.0, 93250.0, 93050.0, 93200.0, 12.0]
    prod_rows = [["strat"] + [None] * 15]
    rt_rows = [["strat_rt"] + [None] * 15]
    bt_rows = [["strat_bt"] + [None] * 15]

    with patch("pnl_consumer.pnl_consumer.insert_rows") as mock_insert:
        _flush_candle(consumer, price_row, prod_rows, rt_rows, bt_rows)

    assert mock_insert.call_count == 4
    tables = [c.args[0] for c in mock_insert.call_args_list]
    assert "analytics.futures_price_1min" in tables
    assert "analytics.strategy_pnl_1min_prod_v2" in tables
    assert "analytics.strategy_pnl_1min_real_trade_v2" in tables
    assert "analytics.strategy_pnl_1min_bt_v2" in tables
    consumer.commit.assert_called_once_with(asynchronous=False)


@pytest.mark.unit
def test_flush_candle_skips_none_price_and_empty_pnl():
    consumer = MagicMock()
    with patch("pnl_consumer.pnl_consumer.insert_rows") as mock_insert:
        _flush_candle(consumer, None, [], [], [])
    mock_insert.assert_not_called()
    consumer.commit.assert_called_once_with(asynchronous=False)


@pytest.mark.unit
def test_flush_candle_commits_after_all_inserts():
    """Offset must not be committed if an insert raises."""
    consumer = MagicMock()
    price_row = ["binance", "BTCUSDT", datetime(2026, 4, 26), 100.0, 101.0, 99.0, 100.5, 1.0]
    with patch("pnl_consumer.pnl_consumer.insert_rows", side_effect=RuntimeError("CH down")):
        with pytest.raises(RuntimeError):
            _flush_candle(consumer, price_row, [], [], [])
    consumer.commit.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# emit_candle_metrics
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_emit_candle_metrics_puts_lag_and_counts():
    candle_ts = datetime(2026, 5, 4, 10, 0, 0)
    fake_now = datetime(2026, 5, 4, 10, 1, 30)
    mock_cw = MagicMock()

    with patch("pnl_consumer.pnl_consumer.datetime") as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        emit_candle_metrics(candle_ts, mock_cw, "price", messages_received=3, prod_rows=10, real_trade_rows=5, bt_rows=0)

    call_kwargs = mock_cw.put_metric_data.call_args.kwargs
    assert call_kwargs["Namespace"] == "trading-analysis"
    metric_names = {m["MetricName"]: m["Value"] for m in call_kwargs["MetricData"]}
    assert metric_names["CandleLagSeconds"] == 90.0
    assert metric_names["MessagesReceived"] == 3
    assert metric_names["ClickHouseSinkProd"] == 10
    assert metric_names["ClickHouseSinkRealTrade"] == 5
    assert "ClickHouseSinkBt" not in metric_names  # zero rows — not emitted


@pytest.mark.unit
def test_emit_candle_metrics_swallows_exceptions():
    mock_cw = MagicMock()
    mock_cw.put_metric_data.side_effect = Exception("network error")
    with patch("pnl_consumer.pnl_consumer.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 5, 4, 10, 1, 0)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        emit_candle_metrics(datetime(2026, 5, 4, 10, 0, 0), mock_cw, "bt", messages_received=1, prod_rows=0, real_trade_rows=0, bt_rows=4)  # must not raise


# ─────────────────────────────────────────────────────────────────────────────
# peek_reference_ts
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_peek_reference_ts_returns_min_ts_across_partitions():
    from confluent_kafka import OFFSET_INVALID
    from pnl_consumer.pnl_consumer import peek_reference_ts

    mock_consumer = MagicMock()
    tp0, tp1 = MagicMock(), MagicMock()
    tp0.partition, tp0.offset = 0, 5
    tp1.partition, tp1.offset = 1, 10
    meta_mock = MagicMock()
    meta_mock.topics = {"binance.price.ticks": MagicMock(partitions={0: None, 1: None})}
    mock_consumer.list_topics.return_value = meta_mock
    mock_consumer.committed.return_value = [tp0, tp1]

    ts0 = datetime(2026, 5, 4, 10, 0, 0)
    ts1 = datetime(2026, 5, 4, 11, 0, 0)
    msg0, msg1 = MagicMock(), MagicMock()
    for msg, ts in [(msg0, ts0), (msg1, ts1)]:
        msg.error.return_value = None
        msg.value.return_value = json.dumps({
            "exchange": "binance", "instrument": "BTCUSDT",
            "ts": ts.isoformat(), "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0,
        }).encode()
    mock_consumer.poll.side_effect = [msg0, msg1]

    with patch(f"{_MOD}.Consumer", return_value=mock_consumer):
        result = peek_reference_ts("localhost:9092", "test-group")

    assert result == ts0


@pytest.mark.unit
def test_peek_reference_ts_returns_none_when_no_messages():
    from pnl_consumer.pnl_consumer import peek_reference_ts

    mock_consumer = MagicMock()
    tp0 = MagicMock()
    tp0.partition, tp0.offset = 0, 5
    meta_mock = MagicMock()
    meta_mock.topics = {"binance.price.ticks": MagicMock(partitions={0: None})}
    mock_consumer.list_topics.return_value = meta_mock
    mock_consumer.committed.return_value = [tp0]
    mock_consumer.poll.return_value = None

    with patch(f"{_MOD}.Consumer", return_value=mock_consumer):
        result = peek_reference_ts("localhost:9092", "test-group")

    assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# Carry-forward: strategies in state with no active bar this candle
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_carry_forward_emits_row_when_bar_absent():
    """Strategy in state with no bar in lookup gets a carry-forward row."""
    state = AnchorState()
    state.set("strat_late", AnchorRecord(
        pnl=0.05, price=93100.0, position=-1.0,
        strategy_id=11, strategy_name="late_strat", underlying="BTC",
        config_timeframe="1h", weighting=1.0,
        strategy_instance_id="inst_late", final_signal=-1.0, benchmark=0.0,
    ))
    candle = _candle(open=93000.0)
    cfg = SinkConfig(price=False, prod=True, real_trade=False, bt=False)

    # No bars returned — simulates the case where the next bar hasn't arrived yet
    with patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]):
        rows, _, _, _ = process_candle(candle, state, AnchorState(), cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert len(pnl_rows) == 1, "carry-forward must emit one row for the late strategy"
    row = pnl_rows[0]["_row"]
    assert row[0] == "strat_late"          # strategy_table_name
    assert row[10] == -1.0                 # position carried forward
    assert row[11] == 93000.0             # current price
    assert row[5] == "production"          # source_label
    # PnL: 0.05 + (-1.0) * (93000 - 93100) / 93100 ≈ 0.05 + 0.001074
    expected = 0.05 + (-1.0) * (93000.0 - 93100.0) / 93100.0
    assert row[8] == pytest.approx(expected)


@pytest.mark.unit
def test_carry_forward_skipped_when_no_metadata():
    """Strategy in state with no bar metadata (never saw a bar) emits no carry-forward."""
    state = AnchorState()
    # AnchorRecord with empty strategy_instance_id — seeded from bootstrap with no bar seen
    state.set("strat_no_meta", AnchorRecord(pnl=0.0, price=93100.0, position=0.0))
    candle = _candle()
    cfg = SinkConfig(price=False, prod=True, real_trade=False, bt=False)

    with patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]):
        rows, _, _, _ = process_candle(candle, state, AnchorState(), cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert pnl_rows == [], "no metadata means no carry-forward row"


@pytest.mark.unit
def test_carry_forward_not_emitted_when_bar_present():
    """When a bar IS returned for a strategy, no carry-forward is emitted."""
    state = AnchorState()
    state.set("strat_prod_1", AnchorRecord(
        pnl=0.0, price=93100.0, position=1.0,
        strategy_id=1, strategy_name="momentum", underlying="BTC",
        config_timeframe="5m", weighting=1.0,
        strategy_instance_id="inst_001", final_signal=1.0, benchmark=0.0,
    ))
    candle = _candle(open=93200.0)
    bar = _bar(position=1.0)
    cfg = SinkConfig(price=False, prod=True, real_trade=False, bt=False)

    with patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[bar]):
        rows, _, _, _ = process_candle(candle, state, AnchorState(), cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    # Exactly 1 row — from the bar, not a duplicate carry-forward
    assert len(pnl_rows) == 1


@pytest.mark.unit
def test_carry_forward_metadata_updated_after_new_bar():
    """After a new bar is processed, carry-forward uses the new bar's metadata."""
    state = AnchorState()
    state.set("strat_prod_1", AnchorRecord(pnl=0.0, price=93000.0, position=1.0,
        strategy_id=1, strategy_name="momentum", underlying="BTC",
        config_timeframe="5m", weighting=1.0,
        strategy_instance_id="inst_001", final_signal=1.0, benchmark=0.0,
    ))
    # Candle 1: bar arrives, position flips to -1.0
    candle1 = _candle(open=93100.0)
    bar_new = _bar(position=-1.0, siid="inst_001")
    cfg = SinkConfig(price=False, prod=True, real_trade=False, bt=False)

    with patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[bar_new]):
        process_candle(candle1, state, AnchorState(), cfg)

    # Candle 2: no bar — carry-forward should use position=-1.0
    candle2 = _candle(open=93200.0)
    with patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]):
        rows, _, _, _ = process_candle(candle2, state, AnchorState(), cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert len(pnl_rows) == 1
    assert pnl_rows[0]["_row"][10] == -1.0  # new position carried forward


# ─────────────────────────────────────────────────────────────────────────────
# _bootstrap_state: continuous seeding (anchor from last stored row, no re-anchor
# step on restart) + seed metadata preserved.
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_bootstrap_state_seeds_from_last_anchor_and_preserves_seed_metadata():
    """The live anchor's pnl/price come from the strategy's LAST stored row
    (fetch_last_pnl_anchors); position + metadata come from the current seed; the
    verification walk overwrites neither. So a restart chains continuously from the
    stored value (no re-anchor step), and metadata is preserved (defaults would
    break carry-forward after a restart).
    """
    bar_ts = datetime(2026, 5, 25, 12, 0, 0)
    revision_ts = datetime(2026, 5, 25, 12, 1, 0)
    walk_ts = datetime(2026, 5, 26, 1, 0, 0)

    seed = BootstrapSeed(
        strategy_table_name="strat_rt_BTC_1h",
        strategy_instance_id="inst_BTC_001",
        pnl=10.0,
        price=70000.0,
        position=1.0,
        underlying="BTC",
        strategy_id=7,
        strategy_name="trend",
        config_timeframe="1h",
        weighting=1.0,
        final_signal=1.0,
        benchmark=0.01,
        bar_ts=bar_ts,
        revision_ts=revision_ts,
    )
    # Last stored row — distinct values to prove the anchor comes from here.
    anchors = {
        "strat_rt_BTC_1h": LastPnlAnchor(
            strategy_table_name="strat_rt_BTC_1h",
            pnl=42.0,
            price=99000.0,
            ts=datetime(2026, 5, 26, 2, 59, 0),
        )
    }
    # Walk row passes verification (prev anchor 11.0/70500) but must NOT seed.
    verified_pnl = 11.0 + 1.0 * (71000.0 - 70500.0) / 70500.0
    walk_row = WalkRow(
        strategy_table_name="strat_rt_BTC_1h",
        strategy_instance_id="inst_BTC_001",
        ts=walk_ts,
        cumulative_pnl=verified_pnl,
        price=71000.0,
        position=1.0,
        bar_ts=bar_ts,
        revision_ts=revision_ts,
    )

    with (
        patch(f"{_MOD}.fetch_last_pnl_anchors", return_value=anchors),
        patch(f"{_MOD}.fetch_bootstrap_seeds", return_value=[seed]),
        patch(
            f"{_MOD}._fetch_walk_anchors",
            return_value=({"strat_rt_BTC_1h": 11.0}, {"strat_rt_BTC_1h": 70500.0}),
        ),
        patch(f"{_MOD}.fetch_walk_rows", return_value=[walk_row]),
    ):
        state = _bootstrap_state(
            "real_trade", reference_ts=datetime(2026, 5, 26, 3, 0, 0)
        )

    rec = state.get("strat_rt_BTC_1h")
    # Anchor comes from the last stored row, NOT the walk's verified_pnl.
    assert rec.pnl == pytest.approx(42.0)
    assert rec.price == pytest.approx(99000.0)
    assert rec.position == 1.0
    # Seed metadata preserved — these would all be defaults if it were wiped.
    assert rec.strategy_instance_id == "inst_BTC_001"
    assert rec.underlying == "BTC"
    assert rec.strategy_id == 7
    assert rec.strategy_name == "trend"
    assert rec.config_timeframe == "1h"
    assert rec.weighting == 1.0
    assert rec.final_signal == 1.0
    assert rec.benchmark == pytest.approx(0.01)


@pytest.mark.unit
def test_bootstrap_state_seeds_flat_when_pnl_chain_but_no_current_revision():
    """A strategy with a stored pnl chain but no current revision is seeded flat,
    continuing its pnl from the last stored value."""
    anchors = {
        "RETIRED": LastPnlAnchor(
            strategy_table_name="RETIRED",
            pnl=7.5,
            price=50.0,
            ts=datetime(2026, 6, 10, 0, 0, 0),
        )
    }
    with (
        patch(f"{_MOD}.fetch_last_pnl_anchors", return_value=anchors),
        patch(f"{_MOD}.fetch_bootstrap_seeds", return_value=[]),
        patch(f"{_MOD}._fetch_walk_anchors", return_value=({}, {})),
        patch(f"{_MOD}.fetch_walk_rows", return_value=[]),
    ):
        state = _bootstrap_state(
            "real_trade", reference_ts=datetime(2026, 6, 17, 20, 0, 0)
        )

    rec = state.get("RETIRED")
    assert rec.pnl == pytest.approx(7.5)  # chain continues from last stored value
    assert rec.position == pytest.approx(0.0)  # flat — no current revision
