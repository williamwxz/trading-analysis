from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from pnl_consumer.anchor_state import AnchorRecord, AnchorState
from pnl_consumer.ch_lookup import BtStrategyBar, StrategyBar, StrategyRevision
from pnl_consumer.pnl_consumer import (
    _bootstrap_anchors,
    _flush,
    _flush_and_reseed,
    emit_candle_lag,
    process_candle,
    SinkConfig,
    resolve_group_id,
)
from streaming.models import CandleEvent
from trading_dagster.utils.pnl_compute import (
    PROD_INSERT_COLUMNS,
    REAL_TRADE_INSERT_COLUMNS,
)

_MOD = "pnl_consumer.pnl_consumer"


def _make_candle(instrument="BTCUSDT", open=93200.0, ts=None) -> CandleEvent:
    return CandleEvent(
        exchange="binance",
        instrument=instrument,
        ts=ts or datetime(2026, 4, 26, 2, 6, 0),
        open=open,
        high=93250.0,
        low=93050.0,
        close=93100.0,
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
    state_prod = AnchorState()
    state_prod.update(
        "strat_prod_1",
        AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0),
    )
    candle = _make_candle(open=93200.0)
    strategies = [_make_strategy(position=1.0)]

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=strategies),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, state_prod, AnchorState(), AnchorState())

    pnl_rows = [r for r in rows if r.get("_sink") == "pnl_prod"]
    assert len(pnl_rows) == 1
    row = pnl_rows[0]
    assert row["strategy_table_name"] == "strat_prod_1"
    assert row["underlying"] == "BTCUSDT"
    assert row["price"] == 93200.0  # open price
    assert abs(row["cumulative_pnl"] - (100.0 / 93100.0)) < 1e-6


@pytest.mark.unit
def test_process_candle_no_strategies_returns_only_price_row():
    candle = _make_candle()

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, AnchorState(), AnchorState(), AnchorState())

    pnl_rows = [r for r in rows if r.get("_sink") == "pnl_prod"]
    assert pnl_rows == []
    price_rows = [r for r in rows if r.get("_sink") == "price"]
    assert len(price_rows) == 1


@pytest.mark.unit
def test_process_candle_always_emits_price_row():
    candle = _make_candle()
    strategies = [_make_strategy()]
    state_prod = AnchorState()
    state_prod.update("strat_prod_1", AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0))

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=strategies),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, state_prod, AnchorState(), AnchorState())

    price_row = next((r for r in rows if r.get("_sink") == "price"), None)
    assert price_row is not None
    assert price_row["instrument"] == "BTCUSDT"
    assert price_row["exchange"] == "binance"


COL = {name: i for i, name in enumerate(PROD_INSERT_COLUMNS)}


def _make_row(strategy: str, ts: datetime, cumulative_pnl: float) -> list:
    row = [None] * len(PROD_INSERT_COLUMNS)
    row[COL["strategy_table_name"]] = strategy
    row[COL["strategy_id"]] = 1
    row[COL["strategy_name"]] = strategy
    row[COL["underlying"]] = "BTC"
    row[COL["config_timeframe"]] = "5m"
    row[COL["source"]] = "production"
    row[COL["version"]] = "v2"
    row[COL["ts"]] = ts
    row[COL["cumulative_pnl"]] = cumulative_pnl
    row[COL["benchmark"]] = 0.0
    row[COL["position"]] = 1.0
    row[COL["price"]] = 100.0
    row[COL["final_signal"]] = 1.0
    row[COL["weighting"]] = 1.0
    row[COL["updated_at"]] = datetime(2026, 3, 1, 10, 5, 0)
    return row


def _make_revision(
    position=1.0,
    revision_ts=datetime(2026, 4, 26, 2, 5, 30),  # arrives after bar 01:00 closes at 02:00
    closing_ts=datetime(2026, 4, 26, 2, 0, 0),    # 1h bar at 01:00 closes at 02:00
) -> StrategyRevision:
    return StrategyRevision(
        strategy_table_name="strat_rt_1",
        strategy_id=1,
        strategy_name="momentum",
        underlying="BTC",
        config_timeframe="5m",
        weighting=1.0,
        position=position,
        final_signal=1.0,
        benchmark=0.0,
        revision_ts=revision_ts,
        closing_ts=closing_ts,
    )


def _make_bt_bar(cumulative_pnl=0.05) -> BtStrategyBar:
    return BtStrategyBar(
        strategy_table_name="strat_bt_1",
        strategy_id=2,
        strategy_name="bt_momentum",
        underlying="BTC",
        config_timeframe="5m",
        weighting=1.0,
        position=1.0,
        final_signal=1.0,
        benchmark=0.0,
        cumulative_pnl=cumulative_pnl,
    )


@pytest.mark.unit
def test_process_candle_produces_pnl_real_trade_rows():
    """Revision that has fired (execution_ts <= candle.ts) produces one PnL row.

    Real timing: bar 01:00 closes at 02:00, revision arrives at 02:05:30,
    execution_ts = 02:06. Candle tick at 02:06 — revision is active.
    """
    state_real_trade = AnchorState()
    state_real_trade.update(
        "strat_rt_1",
        AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0),
    )
    # candle.ts=02:06, revision_ts=02:05:30 → execution_ts=02:06 <= 02:06 ✓
    candle = _make_candle(open=93200.0, ts=datetime(2026, 4, 26, 2, 6, 0))
    revision = _make_revision(
        position=1.0,
        revision_ts=datetime(2026, 4, 26, 2, 5, 30),
        closing_ts=datetime(2026, 4, 26, 2, 0, 0),
    )

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[revision]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, AnchorState(), state_real_trade, AnchorState())

    rt_rows = [r for r in rows if r.get("_sink") == "pnl_real_trade"]
    assert len(rt_rows) == 1
    row = rt_rows[0]
    assert row["strategy_table_name"] == "strat_rt_1"
    assert row["source"] == "real_trade"
    assert row["closing_ts"] == revision.closing_ts
    expected_exec_ts = datetime(2026, 4, 26, 2, 6, 0)
    assert row["execution_ts"] == expected_exec_ts
    assert abs(row["cumulative_pnl"] - (100.0 / 93100.0)) < 1e-6


@pytest.mark.unit
def test_process_candle_revision_not_yet_fired_is_skipped():
    """Revision whose execution_ts > candle.ts is not yet active — no PnL row emitted.

    The position from the previous bar's anchor is held implicitly (no new row written).
    Bar 01:00 closes at 02:00. Revision arrives at 02:05:30 → execution_ts=02:06.
    At candle tick 02:05, the revision hasn't fired yet.
    """
    state_real_trade = AnchorState()
    state_real_trade.update(
        "strat_rt_1",
        AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0),
    )
    # candle.ts=02:05, execution_ts=02:06 → 02:06 > 02:05, skip
    candle = _make_candle(open=93200.0, ts=datetime(2026, 4, 26, 2, 5, 0))
    revision = _make_revision(
        position=1.0,
        revision_ts=datetime(2026, 4, 26, 2, 5, 30),
        closing_ts=datetime(2026, 4, 26, 2, 0, 0),
    )

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[revision]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, AnchorState(), state_real_trade, AnchorState())

    rt_rows = [r for r in rows if r.get("_sink") == "pnl_real_trade"]
    assert rt_rows == []


@pytest.mark.unit
def test_process_candle_two_revisions_only_first_active():
    """When two revisions exist but only the first has fired, emit one row with first position.

    Rev1: revision_ts=02:05:30 → execution_ts=02:06, position=1.0
    Rev2: revision_ts=02:32:25 → execution_ts=02:33, position=0.0
    At candle.ts=02:10, only rev1 is active.
    """
    state_real_trade = AnchorState()
    state_real_trade.update(
        "strat_rt_1",
        AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=0.0),
    )
    candle = _make_candle(open=93200.0, ts=datetime(2026, 4, 26, 2, 10, 0))
    rev1 = _make_revision(
        position=1.0,
        revision_ts=datetime(2026, 4, 26, 2, 5, 30),   # execution_ts=02:06 <= 02:10 ✓
        closing_ts=datetime(2026, 4, 26, 2, 0, 0),
    )
    rev2 = _make_revision(
        position=0.0,
        revision_ts=datetime(2026, 4, 26, 2, 32, 25),  # execution_ts=02:33 > 02:10 ✗
        closing_ts=datetime(2026, 4, 26, 2, 0, 0),
    )

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[rev1, rev2]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, AnchorState(), state_real_trade, AnchorState())

    rt_rows = [r for r in rows if r.get("_sink") == "pnl_real_trade"]
    assert len(rt_rows) == 1
    assert rt_rows[0]["position"] == 1.0
    assert rt_rows[0]["execution_ts"] == datetime(2026, 4, 26, 2, 6, 0)


@pytest.mark.unit
def test_process_candle_two_revisions_both_active_last_wins():
    """When both revisions have fired, the last one (latest execution_ts) is active.

    Rev1: execution_ts=02:06, position=1.0
    Rev2: execution_ts=02:33, position=0.0
    At candle.ts=02:33, rev2 supersedes rev1 — emit position=0.0.
    """
    state_real_trade = AnchorState()
    state_real_trade.update(
        "strat_rt_1",
        AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0),
    )
    candle = _make_candle(open=93200.0, ts=datetime(2026, 4, 26, 2, 33, 0))
    rev1 = _make_revision(
        position=1.0,
        revision_ts=datetime(2026, 4, 26, 2, 5, 30),   # execution_ts=02:06 <= 02:33 ✓
        closing_ts=datetime(2026, 4, 26, 2, 0, 0),
    )
    rev2 = _make_revision(
        position=0.0,
        revision_ts=datetime(2026, 4, 26, 2, 32, 25),  # execution_ts=02:33 <= 02:33 ✓
        closing_ts=datetime(2026, 4, 26, 2, 0, 0),
    )

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[rev1, rev2]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, AnchorState(), state_real_trade, AnchorState())

    rt_rows = [r for r in rows if r.get("_sink") == "pnl_real_trade"]
    assert len(rt_rows) == 1
    assert rt_rows[0]["position"] == 0.0
    assert rt_rows[0]["execution_ts"] == datetime(2026, 4, 26, 2, 33, 0)


@pytest.mark.unit
def test_process_candle_produces_pnl_bt_rows():
    """BT rows are anchor-chained via state_bt, not raw cumulative_pnl from bar."""
    state_prod = AnchorState()
    state_real_trade = AnchorState()
    state_bt = AnchorState()
    state_bt.update("strat_bt_1", AnchorRecord(anchor_pnl=0.05, anchor_price=93100.0, anchor_position=1.0))
    candle = _make_candle(open=93200.0)
    bt_bar = _make_bt_bar(cumulative_pnl=99.0)  # raw value must be ignored

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[bt_bar]),
    ):
        rows = process_candle(candle, state_prod, state_real_trade, state_bt)

    bt_rows = [r for r in rows if r.get("_sink") == "pnl_bt"]
    assert len(bt_rows) == 1
    row = bt_rows[0]
    assert row["strategy_table_name"] == "strat_bt_1"
    assert row["source"] == "backtest"
    # Anchor-chained: 0.05 + 1.0 * (93200 - 93100) / 93100 ≈ 0.0511
    assert abs(row["cumulative_pnl"] - (0.05 + 100.0 / 93100.0)) < 1e-6


@pytest.mark.unit
def test_process_candle_bt_lazy_seeds_anchor_when_missing():
    """bt row is skipped when no anchor exists and lazy-seed also fails."""
    state_bt = AnchorState()
    candle = _make_candle(open=93200.0)
    bt_bar = _make_bt_bar(cumulative_pnl=0.05)

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[bt_bar]),
        patch(f"{_MOD}.fetch_anchor_for_strategy", return_value=None),
    ):
        rows = process_candle(candle, AnchorState(), AnchorState(), state_bt)

    bt_rows = [r for r in rows if r.get("_sink") == "pnl_bt"]
    assert bt_rows == []  # skipped — no anchor, lazy-seed returned None


@pytest.mark.unit
def test_process_candle_lazy_seeds_anchor_when_missing():
    """When anchor is absent, fetch_anchor_for_strategy is called and PnL row is emitted."""
    candle = _make_candle(open=93200.0)
    strategies = [_make_strategy(position=1.0)]
    seeded_anchor = AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0)

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=strategies),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_anchor_for_strategy", return_value=seeded_anchor) as mock_fetch,
    ):
        rows = process_candle(candle, AnchorState(), AnchorState(), AnchorState())

    mock_fetch.assert_called_once_with("strat_prod_1")
    pnl_rows = [r for r in rows if r.get("_sink") == "pnl_prod"]
    assert len(pnl_rows) == 1
    assert pnl_rows[0]["cumulative_pnl"] == pytest.approx(100.0 / 93100.0)


@pytest.mark.unit
def test_process_candle_skips_pnl_when_lazy_seed_also_fails():
    """When fetch_anchor_for_strategy returns None, the PnL row is skipped but price row still emits."""
    candle = _make_candle(open=93200.0)
    strategies = [_make_strategy(position=1.0)]

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=strategies),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_anchor_for_strategy", return_value=None),
    ):
        rows = process_candle(candle, AnchorState(), AnchorState(), AnchorState())

    pnl_rows = [r for r in rows if r.get("_sink") == "pnl_prod"]
    assert pnl_rows == []
    price_rows = [r for r in rows if r.get("_sink") == "price"]
    assert len(price_rows) == 1


@pytest.mark.unit
def test_flush_writes_to_all_three_pnl_tables():
    consumer = MagicMock()
    price_batch = [
        [
            "binance",
            "BTCUSDT",
            datetime(2026, 4, 26, 0, 1),
            93100.0,
            93250.0,
            93050.0,
            93200.0,
            12.0,
        ]
    ]
    pnl_prod_batch = [["strat_prod_1"] + [None] * (len(PROD_INSERT_COLUMNS) - 1)]
    pnl_real_trade_batch = [
        ["strat_rt_1"] + [None] * (len(REAL_TRADE_INSERT_COLUMNS) - 1)
    ]
    pnl_bt_batch = [["strat_bt_1"] + [None] * (len(PROD_INSERT_COLUMNS) - 1)]

    with patch("pnl_consumer.pnl_consumer.insert_rows") as mock_insert:
        _flush(
            consumer, price_batch, pnl_prod_batch, pnl_real_trade_batch, pnl_bt_batch
        )

    assert mock_insert.call_count == 4
    tables_called = [c.args[0] for c in mock_insert.call_args_list]
    assert "analytics.futures_price_1min" in tables_called
    assert "analytics.strategy_pnl_1min_prod_v2" in tables_called
    assert "analytics.strategy_pnl_1min_real_trade_v2" in tables_called
    assert "analytics.strategy_pnl_1min_bt_v2" in tables_called
    consumer.commit.assert_called_once_with(asynchronous=False)


@pytest.mark.unit
def test_flush_skips_empty_batches():
    consumer = MagicMock()
    with patch("pnl_consumer.pnl_consumer.insert_rows") as mock_insert:
        _flush(consumer, [], [], [], [])
    mock_insert.assert_not_called()
    consumer.commit.assert_called_once_with(asynchronous=False)


@pytest.mark.unit
def test_flush_clears_all_batches_after_insert():
    consumer = MagicMock()
    price_batch = [["row"]]
    pnl_prod_batch = [["row"]]
    pnl_real_trade_batch = [["row"]]
    pnl_bt_batch = [["row"]]
    with patch("pnl_consumer.pnl_consumer.insert_rows"):
        _flush(
            consumer, price_batch, pnl_prod_batch, pnl_real_trade_batch, pnl_bt_batch
        )
    assert price_batch == []
    assert pnl_prod_batch == []
    assert pnl_real_trade_batch == []
    assert pnl_bt_batch == []


@pytest.mark.unit
def test_bootstrap_anchors_seeds_prod_and_real_trade():
    prod_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "anchor_pnl": 0.1,
            "anchor_price": 93000.0,
            "anchor_position": 1.0,
            "anchor_ts": datetime(2026, 4, 26, 1, 0, 0),
        }
    ]
    rt_rows = [
        {
            "strategy_table_name": "strat_rt_1",
            "anchor_pnl": 0.2,
            "anchor_price": 92000.0,
            "anchor_position": -1.0,
            "anchor_ts": datetime(2026, 4, 26, 1, 0, 0),
        }
    ]
    state_prod = AnchorState()
    state_real_trade = AnchorState()
    state_bt = AnchorState()

    def mock_query(sql):
        if "strategy_pnl_1min_prod_v2" in sql:
            return prod_rows
        if "strategy_pnl_1min_real_trade_v2" in sql:
            return rt_rows
        return []

    with patch("pnl_consumer.pnl_consumer.query_dicts", side_effect=mock_query):
        _bootstrap_anchors(state_prod, state_real_trade, state_bt)

    assert state_prod.get("strat_prod_1").anchor_price == 93000.0
    assert state_real_trade.get("strat_rt_1").anchor_price == 92000.0
    assert len(state_prod) == 1
    assert len(state_real_trade) == 1


@pytest.mark.unit
def test_flush_and_reseed_reseeds_anchors_from_clickhouse_after_flush():
    """After a backfill overwrites ClickHouse data, the next flush must re-read
    anchor state from ClickHouse when the ClickHouse row is strictly newer than
    the in-memory anchor_ts."""
    consumer = MagicMock()
    state_prod = AnchorState()
    state_real_trade = AnchorState()
    # Seed with an older anchor_ts so reseed (with a newer ts) will overwrite.
    state_prod.update(
        "strat_prod_1",
        AnchorRecord(
            anchor_pnl=0.0,
            anchor_price=50000.0,
            anchor_position=1.0,
            anchor_ts=datetime(2026, 4, 26, 0, 0, 0),
        ),
    )

    fresh_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "anchor_pnl": 0.5,
            "anchor_price": 95000.0,
            "anchor_position": 1.0,
            "anchor_ts": datetime(2026, 4, 26, 1, 0, 0),  # newer ts → should overwrite
        }
    ]

    def mock_query(sql):
        if "strategy_pnl_1min_prod_v2" in sql:
            return fresh_rows
        return []

    with (
        patch("pnl_consumer.pnl_consumer.insert_rows"),
        patch("pnl_consumer.pnl_consumer.query_dicts", side_effect=mock_query),
    ):
        _flush_and_reseed(
            consumer, [["row"]], [], [], [], state_prod, state_real_trade, AnchorState()
        )

    assert state_prod.get("strat_prod_1").anchor_price == 95000.0
    assert state_prod.get("strat_prod_1").anchor_pnl == 0.5


@pytest.mark.unit
def test_flush_and_reseed_reseeds_even_when_batches_empty():
    """Reseed happens on every flush, not just when there were rows to write."""
    consumer = MagicMock()
    state_prod = AnchorState()
    state_real_trade = AnchorState()

    fresh_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "anchor_pnl": 0.9,
            "anchor_price": 96000.0,
            "anchor_position": -1.0,
            "anchor_ts": datetime(2026, 4, 26, 2, 0, 0),
        }
    ]

    def mock_query(sql):
        if "strategy_pnl_1min_prod_v2" in sql:
            return fresh_rows
        return []

    with (
        patch("pnl_consumer.pnl_consumer.insert_rows"),
        patch("pnl_consumer.pnl_consumer.query_dicts", side_effect=mock_query),
    ):
        _flush_and_reseed(consumer, [], [], [], [], state_prod, state_real_trade, AnchorState())

    assert state_prod.get("strat_prod_1").anchor_price == 96000.0


@pytest.mark.unit
def test_emit_candle_lag_calls_put_metric_data_with_lag_in_seconds():
    """emit_candle_lag puts CandleLagSeconds = (now - candle_ts).total_seconds()."""
    candle_ts = datetime(2026, 5, 4, 10, 0, 0)
    fake_now = datetime(2026, 5, 4, 10, 1, 30)  # 90 seconds later

    mock_cw = MagicMock()

    with patch("pnl_consumer.pnl_consumer.datetime") as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        emit_candle_lag(candle_ts, mock_cw, "price")

    mock_cw.put_metric_data.assert_called_once()
    call_kwargs = mock_cw.put_metric_data.call_args.kwargs
    assert call_kwargs["Namespace"] == "trading-analysis"
    metric = call_kwargs["MetricData"][0]
    assert metric["MetricName"] == "CandleLagSeconds"
    assert metric["Value"] == 90.0
    assert metric["Unit"] == "Seconds"
    assert metric["Dimensions"] == [{"Name": "Sink", "Value": "price"}]


@pytest.mark.unit
def test_emit_candle_lag_also_emits_processing_ts_as_unix_epoch():
    """emit_candle_lag emits CandleProcessingTs = candle_ts as Unix epoch."""
    candle_ts = datetime(2026, 5, 4, 10, 0, 0)
    fake_now = datetime(2026, 5, 4, 10, 1, 30)
    mock_cw = MagicMock()

    with patch("pnl_consumer.pnl_consumer.datetime") as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        emit_candle_lag(candle_ts, mock_cw, "prod")

    call_kwargs = mock_cw.put_metric_data.call_args.kwargs
    metric_names = [m["MetricName"] for m in call_kwargs["MetricData"]]
    assert "CandleProcessingTs" in metric_names

    pts_metric = next(m for m in call_kwargs["MetricData"] if m["MetricName"] == "CandleProcessingTs")
    expected_epoch = candle_ts.timestamp()
    assert pts_metric["Value"] == expected_epoch
    assert pts_metric["Unit"] == "None"
    assert pts_metric["Dimensions"] == [{"Name": "Sink", "Value": "prod"}]


@pytest.mark.unit
def test_emit_candle_lag_swallows_exceptions_without_raising():
    """A CloudWatch failure must not crash the consumer loop."""
    candle_ts = datetime(2026, 5, 4, 10, 0, 0)
    mock_cw = MagicMock()
    mock_cw.put_metric_data.side_effect = Exception("network error")

    with patch("pnl_consumer.pnl_consumer.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 5, 4, 10, 1, 0)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        emit_candle_lag(candle_ts, mock_cw, "bt")  # must not raise


# --- SinkConfig tests ---


@pytest.mark.unit
def test_sink_config_defaults_price_true_others_false():
    cfg = SinkConfig.from_env({})
    assert cfg.price is True
    assert cfg.prod is False
    assert cfg.real_trade is False
    assert cfg.bt is False


@pytest.mark.unit
def test_sink_config_enables_prod_via_env():
    cfg = SinkConfig.from_env({"ENABLE_PROD_SINK": "true"})
    assert cfg.prod is True
    assert cfg.price is True
    assert cfg.real_trade is False
    assert cfg.bt is False


@pytest.mark.unit
def test_sink_config_enables_real_trade_via_env():
    cfg = SinkConfig.from_env({"ENABLE_REAL_TRADE_SINK": "true"})
    assert cfg.real_trade is True
    assert cfg.prod is False


@pytest.mark.unit
def test_sink_config_enables_bt_via_env():
    cfg = SinkConfig.from_env({"ENABLE_BT_SINK": "true"})
    assert cfg.bt is True
    assert cfg.prod is False


@pytest.mark.unit
def test_sink_config_can_disable_price_sink():
    cfg = SinkConfig.from_env({"ENABLE_PRICE_SINK": "false"})
    assert cfg.price is False


@pytest.mark.unit
def test_sink_config_enables_all_sinks():
    cfg = SinkConfig.from_env(
        {
            "ENABLE_PRICE_SINK": "true",
            "ENABLE_PROD_SINK": "true",
            "ENABLE_REAL_TRADE_SINK": "true",
            "ENABLE_BT_SINK": "true",
        }
    )
    assert cfg.price is True
    assert cfg.prod is True
    assert cfg.real_trade is True
    assert cfg.bt is True


@pytest.mark.unit
def test_process_candle_respects_sink_config_prod_disabled():
    """When prod sink is disabled, no pnl_prod rows are emitted."""
    state_prod = AnchorState()
    state_prod.update(
        "strat_prod_1",
        AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0),
    )
    candle = _make_candle(open=93200.0)
    strategies = [_make_strategy(position=1.0)]
    cfg = SinkConfig(price=True, prod=False, real_trade=False, bt=False)

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=strategies),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, state_prod, AnchorState(), AnchorState(), cfg)

    assert [r for r in rows if r["_sink"] == "pnl_prod"] == []
    assert len([r for r in rows if r["_sink"] == "price"]) == 1


@pytest.mark.unit
def test_process_candle_respects_sink_config_price_disabled():
    """When price sink is disabled, no price rows are emitted."""
    candle = _make_candle()
    cfg = SinkConfig(price=False, prod=False, real_trade=False, bt=False)

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, AnchorState(), AnchorState(), AnchorState(), cfg)

    assert rows == []


@pytest.mark.unit
def test_process_candle_respects_sink_config_bt_disabled():
    """When bt sink is disabled, no pnl_bt rows are emitted."""
    state_bt = AnchorState()
    state_bt.update("strat_bt_1", AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0))
    candle = _make_candle(open=93200.0)
    bt_bar = _make_bt_bar()
    cfg = SinkConfig(price=True, prod=False, real_trade=False, bt=False)

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[bt_bar]),
    ):
        rows = process_candle(candle, AnchorState(), AnchorState(), state_bt, cfg)

    assert [r for r in rows if r["_sink"] == "pnl_bt"] == []


@pytest.mark.unit
def test_process_candle_no_sink_config_defaults_to_all_enabled():
    """Calling process_candle without cfg uses backward-compatible all-enabled behaviour."""
    state_prod = AnchorState()
    state_prod.update(
        "strat_prod_1",
        AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0),
    )
    candle = _make_candle(open=93200.0)
    strategies = [_make_strategy(position=1.0)]

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=strategies),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, state_prod, AnchorState(), AnchorState())

    assert len([r for r in rows if r["_sink"] == "price"]) == 1
    assert len([r for r in rows if r["_sink"] == "pnl_prod"]) == 1


# --- resolve_group_id tests ---


@pytest.mark.unit
def test_bootstrap_anchors_skipped_when_no_pnl_sinks_enabled():
    """Price-only sink must not query ClickHouse for anchor state."""
    cfg = SinkConfig(price=True, prod=False, real_trade=False, bt=False)
    state_prod = AnchorState()
    state_real_trade = AnchorState()
    state_bt = AnchorState()

    with patch("pnl_consumer.pnl_consumer.query_dicts") as mock_query:
        _bootstrap_anchors(state_prod, state_real_trade, state_bt, cfg)

    mock_query.assert_not_called()


@pytest.mark.unit
def test_bootstrap_anchors_runs_when_any_pnl_sink_enabled():
    """Bootstrap must run when at least one PnL sink is enabled."""
    cfg = SinkConfig(price=True, prod=True, real_trade=False, bt=False)
    prod_rows = [{
        "strategy_table_name": "s1",
        "anchor_pnl": 0.1,
        "anchor_price": 93000.0,
        "anchor_position": 1.0,
        "anchor_ts": datetime(2026, 4, 26, 1, 0, 0),
    }]

    def mock_query(sql):
        if "strategy_pnl_1min_prod_v2" in sql:
            return prod_rows
        return []

    state_prod = AnchorState()
    with patch("pnl_consumer.pnl_consumer.query_dicts", side_effect=mock_query):
        _bootstrap_anchors(state_prod, AnchorState(), AnchorState(), cfg)

    assert state_prod.get("s1").anchor_price == 93000.0


@pytest.mark.unit
def test_resolve_group_id_returns_env_var_when_set():
    assert resolve_group_id({"KAFKA_GROUP_ID": "my-custom-group"}) == "my-custom-group"


@pytest.mark.unit
def test_resolve_group_id_returns_default_when_env_not_set():
    assert resolve_group_id({}) == "flink-pnl-consumer"


@pytest.mark.unit
def test_resolve_group_id_uses_os_environ_by_default(monkeypatch):
    monkeypatch.setenv("KAFKA_GROUP_ID", "env-group")
    assert resolve_group_id() == "env-group"
