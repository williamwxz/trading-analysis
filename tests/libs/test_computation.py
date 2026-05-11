"""Unit tests for libs.computation.candle_lookup and libs.computation.bootstrap.

All tests mock libs.computation.*.query_dicts so no real ClickHouse is needed.
"""
import json
from datetime import datetime, date
from unittest.mock import patch

import pytest

from libs.computation.candle_lookup import (
    StrategyBar,
    StrategyRevision,
    fetch_bt_strategies_for_candle,
    fetch_real_trade_for_candle,
    fetch_strategies_for_candle,
)
from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.bootstrap import (
    BootstrapSeed,
    WalkRow,
    fetch_bootstrap_seeds,
    fetch_walk_rows,
)

DATETIME_MIN = datetime.min

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_CANDLE_TS = datetime(2026, 5, 10, 12, 0, 0)
_START_TS = datetime(2026, 5, 7, 0, 0, 0)
_REF_TS = datetime(2026, 5, 10, 12, 0, 0)


def _make_strategy_bar_row(
    stn: str = "strat_prod_1",
    strategy_id: int = 1,
    strategy_name: str = "momentum",
    underlying: str = "BTC",
    config_timeframe: str = "5m",
    weighting: float = 1.0,
    position: float = 0.5,
    final_signal: float = 1.0,
    benchmark: float = 0.01,
    strategy_instance_id: str = "inst_001",
    latest_ts: datetime = _CANDLE_TS,
) -> dict:
    """Build a mock row as returned by candle_lookup SQL for prod/bt."""
    return {
        "strategy_table_name": stn,
        "strategy_id": strategy_id,
        "strategy_name": strategy_name,
        "underlying": underlying,
        "config_timeframe": config_timeframe,
        "weighting": weighting,
        "strategy_instance_id": strategy_instance_id,
        "latest_ts": latest_ts,
        "row_json": json.dumps(
            {"position": position, "final_signal": final_signal, "benchmark": benchmark}
        ),
    }


def _make_revision_row(
    stn: str = "strat_rt_1",
    strategy_id: int = 1,
    strategy_name: str = "momentum",
    underlying: str = "BTC",
    config_timeframe: str = "5m",
    weighting: float = 1.0,
    position: float = 0.5,
    final_signal: float = 1.0,
    benchmark: float = 0.01,
    strategy_instance_id: str = "inst_001",
    bar_ts: datetime = _CANDLE_TS,
    revision_ts: datetime = _CANDLE_TS,
) -> dict:
    """Build a mock row as returned by fetch_real_trade_for_candle SQL."""
    return {
        "strategy_table_name": stn,
        "strategy_id": strategy_id,
        "strategy_name": strategy_name,
        "underlying": underlying,
        "config_timeframe": config_timeframe,
        "weighting": weighting,
        "strategy_instance_id": strategy_instance_id,
        "bar_ts": bar_ts,
        "revision_ts": revision_ts,
        "row_json": json.dumps(
            {"position": position, "final_signal": final_signal, "benchmark": benchmark}
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# candle_lookup — fetch_strategies_for_candle
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_fetch_strategies_returns_list_of_strategy_bars():
    """Returns a list of StrategyBar with correctly parsed fields."""
    mock_rows = [_make_strategy_bar_row(position=0.5, final_signal=1.0, benchmark=0.02)]
    with patch("libs.computation.candle_lookup.query_dicts", return_value=mock_rows):
        bars = fetch_strategies_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert len(bars) == 1
    bar = bars[0]
    assert isinstance(bar, StrategyBar)
    assert bar.strategy_table_name == "strat_prod_1"
    assert bar.position == pytest.approx(0.5)
    assert bar.final_signal == pytest.approx(1.0)
    assert bar.benchmark == pytest.approx(0.02)


@pytest.mark.unit
def test_fetch_strategies_returns_empty_list_when_no_rows():
    """Empty result set returns empty list."""
    with patch("libs.computation.candle_lookup.query_dicts", return_value=[]):
        bars = fetch_strategies_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert bars == []


@pytest.mark.unit
def test_fetch_strategies_bar_ts_from_latest_ts():
    """bar_ts is populated from the 'latest_ts' field in the row."""
    expected_ts = datetime(2026, 5, 10, 11, 55, 0)
    mock_rows = [_make_strategy_bar_row(latest_ts=expected_ts)]
    with patch("libs.computation.candle_lookup.query_dicts", return_value=mock_rows):
        bars = fetch_strategies_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert bars[0].bar_ts == expected_ts


@pytest.mark.unit
def test_fetch_strategies_strategy_instance_id_populated():
    """strategy_instance_id is correctly parsed from the row."""
    mock_rows = [_make_strategy_bar_row(strategy_instance_id="inst_XYZ")]
    with patch("libs.computation.candle_lookup.query_dicts", return_value=mock_rows):
        bars = fetch_strategies_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert bars[0].strategy_instance_id == "inst_XYZ"


@pytest.mark.unit
def test_fetch_strategies_sql_uses_closing_ts_filter():
    """SQL filters on ts + toIntervalMinute(...) <= candle_ts, not raw ts <=."""
    captured = []

    def capture(sql):
        captured.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_strategies_for_candle(
            instrument="BTCUSDT", candle_ts=datetime(2026, 5, 10, 18, 5, 0)
        )
    sql = captured[0]
    assert "ts + toIntervalMinute" in sql, "must use closing_ts gate, not raw ts"
    assert "<= '2026-05-10 18:05:00'" in sql


@pytest.mark.unit
def test_fetch_strategies_sql_uses_argmin_revision_ts():
    """SQL uses argMin(row_json, revision_ts) to select first revision only."""
    captured = []

    def capture(sql):
        captured.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_strategies_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert "argMin(row_json, revision_ts)" in captured[0]


@pytest.mark.unit
def test_fetch_strategies_sql_has_limit_1_by_strategy_instance_id():
    """SQL uses LIMIT 1 BY strategy_instance_id to dedup per logical strategy."""
    captured = []

    def capture(sql):
        captured.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_strategies_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert "LIMIT 1 BY strategy_instance_id" in captured[0]


@pytest.mark.unit
def test_fetch_strategies_strips_usdt_suffix_from_instrument():
    """underlying is 'BTC' when instrument is 'BTCUSDT' (USDT stripped)."""
    mock_rows = [_make_strategy_bar_row(underlying="BTC")]
    captured_sql = []

    def capture(sql):
        captured_sql.append(sql)
        return mock_rows

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        bars = fetch_strategies_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert bars[0].underlying == "BTC"
    assert "'BTC'" in captured_sql[0]


# ─────────────────────────────────────────────────────────────────────────────
# candle_lookup — fetch_bt_strategies_for_candle
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_fetch_bt_strategies_closing_ts_filter():
    """bt lookup also gates on closing_ts, not raw ts."""
    captured = []

    def capture(sql):
        captured.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_bt_strategies_for_candle(
            instrument="BTCUSDT", candle_ts=datetime(2026, 5, 10, 18, 5, 0)
        )
    sql = captured[0]
    assert "ts + toIntervalMinute" in sql
    assert "<= '2026-05-10 18:05:00'" in sql


@pytest.mark.unit
def test_fetch_bt_strategies_uses_argmin_revision_ts():
    """bt lookup uses argMin(row_json, revision_ts) — first revision only."""
    captured = []

    def capture(sql):
        captured.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_bt_strategies_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert "argMin(row_json, revision_ts)" in captured[0]


@pytest.mark.unit
def test_fetch_bt_strategies_queries_bt_table():
    """bt lookup queries strategy_output_history_bt_v2, not the prod table."""
    captured = []

    def capture(sql):
        captured.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_bt_strategies_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert "strategy_output_history_bt_v2" in captured[0]
    assert "strategy_output_history_v2" not in captured[0].replace(
        "strategy_output_history_bt_v2", ""
    )


@pytest.mark.unit
def test_fetch_bt_strategies_limit_1_by_strategy_instance_id():
    """bt lookup uses LIMIT 1 BY strategy_instance_id."""
    captured = []

    def capture(sql):
        captured.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_bt_strategies_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert "LIMIT 1 BY strategy_instance_id" in captured[0]


# ─────────────────────────────────────────────────────────────────────────────
# candle_lookup — fetch_real_trade_for_candle
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_fetch_real_trade_returns_list_of_strategy_revisions():
    """Returns a list of StrategyRevision with bar_ts and revision_ts populated."""
    bar_ts_val = datetime(2026, 5, 10, 11, 50, 0)
    rev_ts_val = datetime(2026, 5, 10, 11, 53, 0)
    mock_rows = [
        _make_revision_row(
            position=0.75,
            bar_ts=bar_ts_val,
            revision_ts=rev_ts_val,
        )
    ]
    with patch("libs.computation.candle_lookup.query_dicts", return_value=mock_rows):
        revisions = fetch_real_trade_for_candle(
            instrument="BTCUSDT", candle_ts=_CANDLE_TS
        )
    assert len(revisions) == 1
    rev = revisions[0]
    assert isinstance(rev, StrategyRevision)
    assert rev.position == pytest.approx(0.75)
    assert rev.bar_ts == bar_ts_val
    assert rev.revision_ts == rev_ts_val


@pytest.mark.unit
def test_fetch_real_trade_sql_uses_revision_ts_filter():
    """Real-trade lookup uses revision_ts <= candle_ts, NOT closing_ts gate."""
    captured = []

    def capture(sql):
        captured.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_real_trade_for_candle(
            instrument="BTCUSDT", candle_ts=datetime(2026, 5, 10, 12, 0, 0)
        )
    sql = captured[0]
    assert "revision_ts <=" in sql
    assert "ts + toIntervalMinute" not in sql, "must NOT use closing_ts gate for real_trade"


@pytest.mark.unit
def test_fetch_real_trade_sql_uses_argmax_revision_ts():
    """Real-trade lookup uses argMax(row_json, revision_ts) — latest revision."""
    captured = []

    def capture(sql):
        captured.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_real_trade_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert "argMax(row_json, revision_ts)" in captured[0]


@pytest.mark.unit
def test_fetch_real_trade_sql_limit_1_by_strategy_instance_id():
    """Real-trade lookup uses LIMIT 1 BY strategy_instance_id."""
    captured = []

    def capture(sql):
        captured.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_real_trade_for_candle(instrument="BTCUSDT", candle_ts=_CANDLE_TS)
    assert "LIMIT 1 BY strategy_instance_id" in captured[0]


@pytest.mark.unit
def test_fetch_real_trade_returns_empty_when_no_rows():
    """Empty result returns empty list."""
    with patch("libs.computation.candle_lookup.query_dicts", return_value=[]):
        revisions = fetch_real_trade_for_candle(
            instrument="BTCUSDT", candle_ts=_CANDLE_TS
        )
    assert revisions == []


# ─────────────────────────────────────────────────────────────────────────────
# bootstrap — fetch_bootstrap_seeds (prod/bt, real_trade=False)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_fetch_bootstrap_seeds_returns_bootstrap_seed_with_correct_fields():
    """Returns BootstrapSeed with pnl, price, position from mocked rows."""
    pnl_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_instance_id": "inst_001",
            "cumulative_pnl": 5.5,
            "price": 93000.0,
        }
    ]
    pos_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_instance_id": "inst_001",
            "row_json": json.dumps({"position": 1.0}),
        }
    ]
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, pos_rows, [{"cnt": 0}]]
    ):
        seeds = fetch_bootstrap_seeds(
            pnl_table="analytics.strategy_pnl_1min_prod_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            real_trade=False,
        )
    assert len(seeds) == 1
    seed = seeds[0]
    assert isinstance(seed, BootstrapSeed)
    assert seed.pnl == pytest.approx(5.5)
    assert seed.price == pytest.approx(93000.0)
    assert seed.position == pytest.approx(1.0)
    assert seed.strategy_table_name == "strat_prod_1"
    assert seed.strategy_instance_id == "inst_001"


@pytest.mark.unit
def test_fetch_bootstrap_seeds_bar_ts_revision_ts_are_datetime_min_for_prod():
    """For prod/bt mode, bar_ts and revision_ts default to datetime.min."""
    pnl_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_instance_id": "inst_001",
            "cumulative_pnl": 1.0,
            "price": 50000.0,
        }
    ]
    pos_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_instance_id": "inst_001",
            "row_json": json.dumps({"position": 0.5}),
        }
    ]
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, pos_rows, [{"cnt": 0}]]
    ):
        seeds = fetch_bootstrap_seeds(
            pnl_table="analytics.strategy_pnl_1min_prod_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            real_trade=False,
        )
    assert seeds[0].bar_ts == DATETIME_MIN
    assert seeds[0].revision_ts == DATETIME_MIN


@pytest.mark.unit
def test_fetch_bootstrap_seeds_position_sql_uses_argmin_revision_ts():
    """Prod/bt mode position SQL uses argMin(row_json, revision_ts) — first revision."""
    pnl_rows: list = []
    pos_rows: list = []
    captured = []

    def capture(sql):
        captured.append(sql)
        return captured_results.pop(0)

    captured_results = [pnl_rows, pos_rows, [{"cnt": 0}]]
    with patch("libs.computation.bootstrap.query_dicts", side_effect=capture):
        fetch_bootstrap_seeds(
            pnl_table="analytics.strategy_pnl_1min_prod_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            real_trade=False,
        )
    # Second call is the position SQL; third is completeness check
    assert len(captured) == 3
    pos_sql = captured[1]
    assert "argMin(row_json, revision_ts)" in pos_sql
    assert "ts <=" in pos_sql


@pytest.mark.unit
def test_fetch_bootstrap_seeds_strategy_in_history_but_not_pnl_gets_zero_pnl_price():
    """Strategy present in history but not in pnl table gets pnl=0.0, price=0.0."""
    pnl_rows: list = []  # no pnl data
    pos_rows = [
        {
            "strategy_table_name": "strat_new",
            "strategy_instance_id": "inst_new",
            "row_json": json.dumps({"position": 0.3}),
        }
    ]
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, pos_rows, [{"cnt": 0}]]
    ):
        seeds = fetch_bootstrap_seeds(
            pnl_table="analytics.strategy_pnl_1min_prod_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            real_trade=False,
        )
    assert len(seeds) == 1
    seed = seeds[0]
    assert seed.pnl == pytest.approx(0.0)
    assert seed.price == pytest.approx(0.0)
    assert seed.position == pytest.approx(0.3)


@pytest.mark.unit
def test_fetch_bootstrap_seeds_strategy_in_pnl_but_not_history_gets_zero_position():
    """Strategy in pnl table but not in history gets position=0.0."""
    pnl_rows = [
        {
            "strategy_table_name": "strat_old",
            "strategy_instance_id": "inst_old",
            "cumulative_pnl": 10.0,
            "price": 80000.0,
        }
    ]
    pos_rows: list = []  # no position data
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, pos_rows, [{"cnt": 0}]]
    ):
        seeds = fetch_bootstrap_seeds(
            pnl_table="analytics.strategy_pnl_1min_prod_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            real_trade=False,
        )
    assert len(seeds) == 1
    seed = seeds[0]
    assert seed.pnl == pytest.approx(10.0)
    assert seed.price == pytest.approx(80000.0)
    assert seed.position == pytest.approx(0.0)


@pytest.mark.unit
def test_fetch_bootstrap_seeds_all_keys_union_of_both_sources():
    """all_keys = union of pnl table keys and history table keys."""
    pnl_rows = [
        {
            "strategy_table_name": "strat_A",
            "strategy_instance_id": "inst_A",
            "cumulative_pnl": 1.0,
            "price": 90000.0,
        }
    ]
    pos_rows = [
        {
            "strategy_table_name": "strat_B",
            "strategy_instance_id": "inst_B",
            "row_json": json.dumps({"position": 0.5}),
        }
    ]
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, pos_rows, [{"cnt": 0}]]
    ):
        seeds = fetch_bootstrap_seeds(
            pnl_table="analytics.strategy_pnl_1min_prod_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            real_trade=False,
        )
    stns = {s.strategy_table_name for s in seeds}
    assert stns == {"strat_A", "strat_B"}


# ─────────────────────────────────────────────────────────────────────────────
# bootstrap — fetch_bootstrap_seeds (real_trade=True)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_fetch_bootstrap_seeds_real_trade_position_sql_uses_argmax_revision_ts():
    """Real-trade mode position SQL uses argMax(row_json, revision_ts) — latest revision."""
    pnl_rows: list = []
    pos_rows: list = []
    captured = []

    captured_results = [pnl_rows, pos_rows, [{"cnt": 0}]]

    def capture(sql):
        captured.append(sql)
        return captured_results.pop(0)

    with patch("libs.computation.bootstrap.query_dicts", side_effect=capture):
        fetch_bootstrap_seeds(
            pnl_table="analytics.strategy_pnl_1min_real_trade_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            real_trade=True,
        )
    pos_sql = captured[1]
    assert "argMax(row_json, revision_ts)" in pos_sql
    assert "revision_ts <=" in pos_sql


@pytest.mark.unit
def test_fetch_bootstrap_seeds_real_trade_bar_ts_revision_ts_populated():
    """For real_trade mode, bar_ts and revision_ts are populated from position query."""
    expected_bar_ts = datetime(2026, 5, 9, 23, 55, 0)
    expected_rev_ts = datetime(2026, 5, 9, 23, 57, 30)
    pnl_rows: list = []
    pos_rows = [
        {
            "strategy_table_name": "strat_rt",
            "strategy_instance_id": "inst_rt",
            "bar_ts": expected_bar_ts,
            "revision_ts": expected_rev_ts,
            "row_json": json.dumps({"position": 0.8}),
        }
    ]
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, pos_rows, [{"cnt": 0}]]
    ):
        seeds = fetch_bootstrap_seeds(
            pnl_table="analytics.strategy_pnl_1min_real_trade_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            real_trade=True,
        )
    assert len(seeds) == 1
    seed = seeds[0]
    assert seed.bar_ts == expected_bar_ts
    assert seed.revision_ts == expected_rev_ts
    assert seed.position == pytest.approx(0.8)


@pytest.mark.unit
def test_fetch_bootstrap_seeds_real_trade_correct_position():
    """Real-trade mode returns correct position from argMax revision."""
    pnl_rows = [
        {
            "strategy_table_name": "strat_rt",
            "strategy_instance_id": "inst_rt",
            "cumulative_pnl": 3.0,
            "price": 70000.0,
        }
    ]
    pos_rows = [
        {
            "strategy_table_name": "strat_rt",
            "strategy_instance_id": "inst_rt",
            "bar_ts": datetime(2026, 5, 9, 22, 0, 0),
            "revision_ts": datetime(2026, 5, 9, 22, 5, 0),
            "row_json": json.dumps({"position": -1.0}),
        }
    ]
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, pos_rows, [{"cnt": 0}]]
    ):
        seeds = fetch_bootstrap_seeds(
            pnl_table="analytics.strategy_pnl_1min_real_trade_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            real_trade=True,
        )
    assert seeds[0].position == pytest.approx(-1.0)


# ─────────────────────────────────────────────────────────────────────────────
# bootstrap — fetch_walk_rows (prod/bt, real_trade=False)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_fetch_walk_rows_returns_walk_row_list_with_correct_fields():
    """Returns WalkRow list with cumulative_pnl, price, position populated."""
    row_ts = datetime(2026, 5, 7, 1, 0, 0)
    pnl_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_instance_id": "inst_001",
            "ts": row_ts,
            "cumulative_pnl": 7.2,
            "underlying": "BTC",
            "price": 95000.0,
        }
    ]
    # One bar before and at row_ts
    bar_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "bar_ts": datetime(2026, 5, 7, 0, 55, 0),
            "row_json": json.dumps({"position": 0.5}),
        }
    ]
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, bar_rows]
    ):
        walk_rows = fetch_walk_rows(
            pnl_table="analytics.strategy_pnl_1min_prod_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            reference_ts=_REF_TS,
            real_trade=False,
        )
    assert len(walk_rows) == 1
    wr = walk_rows[0]
    assert isinstance(wr, WalkRow)
    assert wr.cumulative_pnl == pytest.approx(7.2)
    assert wr.price == pytest.approx(95000.0)
    assert wr.position == pytest.approx(0.5)


@pytest.mark.unit
def test_fetch_walk_rows_price_from_price_field_not_cumulative_pnl():
    """price comes from the joined futures_price_1min column, not cumulative_pnl."""
    row_ts = datetime(2026, 5, 7, 2, 0, 0)
    pnl_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_instance_id": "inst_001",
            "ts": row_ts,
            "cumulative_pnl": 999.0,  # large sentinel — should NOT appear as price
            "underlying": "BTC",
            "price": 88000.0,  # this is the joined futures price
        }
    ]
    bar_rows: list = []
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, bar_rows]
    ):
        walk_rows = fetch_walk_rows(
            pnl_table="analytics.strategy_pnl_1min_prod_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            reference_ts=_REF_TS,
            real_trade=False,
        )
    assert walk_rows[0].price == pytest.approx(88000.0)
    assert walk_rows[0].cumulative_pnl == pytest.approx(999.0)


@pytest.mark.unit
def test_fetch_walk_rows_position_resolved_by_bisect_latest_bar_le_row_ts():
    """Position for each walk row is the latest bar with bar_ts <= row.ts."""
    ts1 = datetime(2026, 5, 7, 1, 0, 0)
    ts2 = datetime(2026, 5, 7, 2, 0, 0)
    ts3 = datetime(2026, 5, 7, 3, 0, 0)

    # Three PnL rows at ts1, ts2, ts3
    pnl_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_instance_id": "inst_001",
            "ts": ts,
            "cumulative_pnl": 1.0,
            "underlying": "BTC",
            "price": 90000.0,
        }
        for ts in [ts1, ts2, ts3]
    ]
    # Two bars: bar at ts1 (position=0.5) and bar at ts2 (position=1.0)
    bar_ts_a = datetime(2026, 5, 7, 0, 55, 0)  # before ts1 — applies to ts1
    bar_ts_b = datetime(2026, 5, 7, 1, 30, 0)  # between ts1 and ts2 — applies to ts2, ts3
    bar_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "bar_ts": bar_ts_a,
            "row_json": json.dumps({"position": 0.5}),
        },
        {
            "strategy_table_name": "strat_prod_1",
            "bar_ts": bar_ts_b,
            "row_json": json.dumps({"position": 1.0}),
        },
    ]
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, bar_rows]
    ):
        walk_rows = fetch_walk_rows(
            pnl_table="analytics.strategy_pnl_1min_prod_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            reference_ts=_REF_TS,
            real_trade=False,
        )
    assert len(walk_rows) == 3
    # ts1 (01:00): bar_ts_a (00:55) is latest bar <= 01:00, position=0.5
    assert walk_rows[0].position == pytest.approx(0.5)
    # ts2 (02:00): bar_ts_b (01:30) is latest bar <= 02:00, position=1.0
    assert walk_rows[1].position == pytest.approx(1.0)
    # ts3 (03:00): bar_ts_b (01:30) is still latest bar <= 03:00, position=1.0
    assert walk_rows[2].position == pytest.approx(1.0)


@pytest.mark.unit
def test_fetch_walk_rows_returns_empty_when_no_pnl_rows():
    """Returns empty list when there are no PnL rows in the window."""
    pnl_rows: list = []
    with patch("libs.computation.bootstrap.query_dicts", return_value=pnl_rows):
        walk_rows = fetch_walk_rows(
            pnl_table="analytics.strategy_pnl_1min_prod_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            reference_ts=_REF_TS,
            real_trade=False,
        )
    assert walk_rows == []


@pytest.mark.unit
def test_fetch_walk_rows_bar_ts_revision_ts_are_datetime_min_for_prod():
    """For prod/bt mode walk rows, bar_ts and revision_ts are datetime.min."""
    row_ts = datetime(2026, 5, 7, 1, 0, 0)
    pnl_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_instance_id": "inst_001",
            "ts": row_ts,
            "cumulative_pnl": 1.0,
            "underlying": "BTC",
            "price": 90000.0,
        }
    ]
    bar_rows: list = []
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, bar_rows]
    ):
        walk_rows = fetch_walk_rows(
            pnl_table="analytics.strategy_pnl_1min_prod_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            reference_ts=_REF_TS,
            real_trade=False,
        )
    assert walk_rows[0].bar_ts == DATETIME_MIN
    assert walk_rows[0].revision_ts == DATETIME_MIN


# ─────────────────────────────────────────────────────────────────────────────
# bootstrap — fetch_walk_rows (real_trade=True)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_fetch_walk_rows_real_trade_position_resolved_by_bisect_on_revision_ts():
    """Real-trade walk uses revision_ts bisect (not bar_ts) to resolve position."""
    ts1 = datetime(2026, 5, 7, 1, 0, 0)
    ts2 = datetime(2026, 5, 7, 1, 5, 0)
    ts3 = datetime(2026, 5, 7, 1, 10, 0)

    pnl_rows = [
        {
            "strategy_table_name": "strat_rt",
            "strategy_instance_id": "inst_rt",
            "ts": ts,
            "cumulative_pnl": 2.0,
            "underlying": "BTC",
            "price": 90000.0,
        }
        for ts in [ts1, ts2, ts3]
    ]
    # Revision at 01:02 changes position to 1.0; applies to ts2 (01:05) and ts3 (01:10)
    rev_ts_a = datetime(2026, 5, 7, 0, 58, 0)  # before ts1 — position=0.0
    rev_ts_b = datetime(2026, 5, 7, 1, 2, 0)   # between ts1 and ts2 — position=1.0
    bar_ts_common = datetime(2026, 5, 7, 0, 55, 0)

    hist_rows = [
        {
            "strategy_instance_id": "inst_rt",
            "bar_ts": bar_ts_common,
            "revision_ts": rev_ts_a,
            "row_json": json.dumps({"position": 0.0}),
        },
        {
            "strategy_instance_id": "inst_rt",
            "bar_ts": bar_ts_common,
            "revision_ts": rev_ts_b,
            "row_json": json.dumps({"position": 1.0}),
        },
    ]
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, hist_rows]
    ):
        walk_rows = fetch_walk_rows(
            pnl_table="analytics.strategy_pnl_1min_real_trade_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            reference_ts=_REF_TS,
            real_trade=True,
        )
    assert len(walk_rows) == 3
    # ts1=01:00: rev_ts_a (00:58) is latest revision <= 01:00, position=0.0
    assert walk_rows[0].position == pytest.approx(0.0)
    # ts2=01:05: rev_ts_b (01:02) is latest revision <= 01:05, position=1.0
    assert walk_rows[1].position == pytest.approx(1.0)
    # ts3=01:10: rev_ts_b (01:02) still latest, position=1.0
    assert walk_rows[2].position == pytest.approx(1.0)


@pytest.mark.unit
def test_fetch_walk_rows_real_trade_bar_ts_revision_ts_populated():
    """Real-trade walk rows have bar_ts and revision_ts populated."""
    row_ts = datetime(2026, 5, 7, 1, 5, 0)
    expected_bar_ts = datetime(2026, 5, 7, 0, 55, 0)
    expected_rev_ts = datetime(2026, 5, 7, 1, 2, 0)

    pnl_rows = [
        {
            "strategy_table_name": "strat_rt",
            "strategy_instance_id": "inst_rt",
            "ts": row_ts,
            "cumulative_pnl": 2.0,
            "underlying": "BTC",
            "price": 90000.0,
        }
    ]
    hist_rows = [
        {
            "strategy_instance_id": "inst_rt",
            "bar_ts": expected_bar_ts,
            "revision_ts": expected_rev_ts,
            "row_json": json.dumps({"position": 0.5}),
        }
    ]
    with patch(
        "libs.computation.bootstrap.query_dicts", side_effect=[pnl_rows, hist_rows]
    ):
        walk_rows = fetch_walk_rows(
            pnl_table="analytics.strategy_pnl_1min_real_trade_v2",
            history_table="analytics.strategy_output_history_v2",
            start_ts=_START_TS,
            reference_ts=_REF_TS,
            real_trade=True,
        )
    assert len(walk_rows) == 1
    wr = walk_rows[0]
    assert wr.bar_ts == expected_bar_ts
    assert wr.revision_ts == expected_rev_ts


# ─────────────────────────────────────────────────────────────────────────────
# AnchorState revision guard logic (pure Python, no mocks)
#
# The revision guard is the tuple comparison:
#   (new_bar_ts, new_revision_ts) > (anchor.bar_ts, anchor.revision_ts)
# We test this logic directly in Python without importing a class,
# since it mirrors the guard used by the streaming consumer's AnchorState.
# ─────────────────────────────────────────────────────────────────────────────

def _should_apply_revision(
    anchor_bar_ts: datetime,
    anchor_rev_ts: datetime,
    new_bar_ts: datetime,
    new_rev_ts: datetime,
) -> bool:
    """Replica of the AnchorState revision guard: apply iff (bar_ts, rev_ts) > anchor."""
    return (new_bar_ts, new_rev_ts) > (anchor_bar_ts, anchor_rev_ts)


@pytest.mark.unit
def test_revision_guard_new_bar_applies():
    """A revision for a newer bar always applies."""
    anchor_bar_ts = datetime(2026, 5, 10, 10, 0, 0)
    anchor_rev_ts = datetime(2026, 5, 10, 10, 5, 0)
    new_bar_ts = datetime(2026, 5, 10, 11, 0, 0)
    new_rev_ts = datetime(2026, 5, 10, 11, 2, 0)
    assert _should_apply_revision(anchor_bar_ts, anchor_rev_ts, new_bar_ts, new_rev_ts)


@pytest.mark.unit
def test_revision_guard_same_bar_newer_revision_applies():
    """Same bar, newer revision_ts: applies."""
    anchor_bar_ts = datetime(2026, 5, 10, 10, 0, 0)
    anchor_rev_ts = datetime(2026, 5, 10, 10, 5, 0)
    new_bar_ts = datetime(2026, 5, 10, 10, 0, 0)
    new_rev_ts = datetime(2026, 5, 10, 10, 7, 0)
    assert _should_apply_revision(anchor_bar_ts, anchor_rev_ts, new_bar_ts, new_rev_ts)


@pytest.mark.unit
def test_revision_guard_same_bar_same_revision_ignored():
    """Same bar, same revision_ts: NOT applied (already processed)."""
    ts = datetime(2026, 5, 10, 10, 0, 0)
    rev = datetime(2026, 5, 10, 10, 5, 0)
    assert not _should_apply_revision(ts, rev, ts, rev)


@pytest.mark.unit
def test_revision_guard_older_bar_ignored():
    """A revision for an older bar than anchor: NOT applied."""
    anchor_bar_ts = datetime(2026, 5, 10, 11, 0, 0)
    anchor_rev_ts = datetime(2026, 5, 10, 11, 2, 0)
    old_bar_ts = datetime(2026, 5, 10, 10, 0, 0)
    old_rev_ts = datetime(2026, 5, 10, 10, 9, 0)  # even with newer rev_ts, older bar
    assert not _should_apply_revision(anchor_bar_ts, anchor_rev_ts, old_bar_ts, old_rev_ts)


@pytest.mark.unit
def test_revision_guard_first_revision_always_applies():
    """First revision always applies: anchor starts at datetime.min."""
    anchor_bar_ts = DATETIME_MIN
    anchor_rev_ts = DATETIME_MIN
    new_bar_ts = datetime(2026, 5, 10, 10, 0, 0)
    new_rev_ts = datetime(2026, 5, 10, 10, 3, 0)
    assert _should_apply_revision(anchor_bar_ts, anchor_rev_ts, new_bar_ts, new_rev_ts)


# ─────────────────────────────────────────────────────────────────────────────
# AnchorState / AnchorRecord
# ─────────────────────────────────────────────────────────────────────────────

_BAR_TS = datetime(2026, 5, 10, 10, 0, 0)
_REV_TS = datetime(2026, 5, 10, 10, 3, 0)


@pytest.mark.unit
def test_anchor_record_defaults():
    """AnchorRecord defaults: pnl=0, price=0, position=0, bar_ts=datetime.min, revision_ts=datetime.min."""
    rec = AnchorRecord()
    assert rec.pnl == 0.0
    assert rec.price == 0.0
    assert rec.position == 0.0
    assert rec.bar_ts == DATETIME_MIN
    assert rec.revision_ts == DATETIME_MIN


@pytest.mark.unit
def test_anchor_state_get_returns_default_for_unknown_strategy():
    """get() returns a zero AnchorRecord for unknown strategy_table_name."""
    state = AnchorState()
    rec = state.get("unknown_strategy")
    assert rec.pnl == 0.0
    assert rec.price == 0.0
    assert rec.position == 0.0


@pytest.mark.unit
def test_anchor_state_set_and_get_roundtrip():
    """set() stores a record; get() retrieves the same object."""
    state = AnchorState()
    rec = AnchorRecord(pnl=5.0, price=90000.0, position=1.0)
    state.set("strat_1", rec)
    retrieved = state.get("strat_1")
    assert retrieved.pnl == pytest.approx(5.0)
    assert retrieved.price == pytest.approx(90000.0)
    assert retrieved.position == pytest.approx(1.0)


@pytest.mark.unit
def test_anchor_state_has():
    """has() returns True only after set()."""
    state = AnchorState()
    assert not state.has("strat_X")
    state.set("strat_X", AnchorRecord(pnl=1.0, price=100.0))
    assert state.has("strat_X")


@pytest.mark.unit
def test_anchor_state_len():
    """len() reflects the number of seeded strategies."""
    state = AnchorState()
    assert len(state) == 0
    state.set("strat_1", AnchorRecord(price=90000.0))
    state.set("strat_2", AnchorRecord(price=3000.0))
    assert len(state) == 2


@pytest.mark.unit
def test_anchor_state_compute_pnl_basic_formula():
    """compute_pnl applies: pnl = prev_pnl + position * (price - prev_price) / prev_price."""
    state = AnchorState()
    state.set("strat_1", AnchorRecord(pnl=10.0, price=100.0, position=0.5))
    new_pnl = state.compute_pnl("strat_1", current_price=110.0, position=0.5)
    # pnl = 10.0 + 0.5 * (110 - 100) / 100 = 10.0 + 0.05 = 10.05
    assert new_pnl == pytest.approx(10.05)


@pytest.mark.unit
def test_anchor_state_compute_pnl_advances_price():
    """After compute_pnl, the stored price is updated to current_price."""
    state = AnchorState()
    state.set("strat_1", AnchorRecord(pnl=0.0, price=100.0, position=1.0))
    state.compute_pnl("strat_1", current_price=105.0, position=1.0)
    assert state.get("strat_1").price == pytest.approx(105.0)


@pytest.mark.unit
def test_anchor_state_compute_pnl_advances_position():
    """After compute_pnl, the stored position is updated to the new position."""
    state = AnchorState()
    state.set("strat_1", AnchorRecord(pnl=0.0, price=100.0, position=0.0))
    state.compute_pnl("strat_1", current_price=100.0, position=1.5)
    assert state.get("strat_1").position == pytest.approx(1.5)


@pytest.mark.unit
def test_anchor_state_compute_pnl_stores_bar_ts_revision_ts():
    """compute_pnl stores bar_ts and revision_ts on the updated record."""
    state = AnchorState()
    state.set("strat_1", AnchorRecord(pnl=0.0, price=100.0))
    state.compute_pnl("strat_1", current_price=100.0, position=0.0,
                      bar_ts=_BAR_TS, revision_ts=_REV_TS)
    rec = state.get("strat_1")
    assert rec.bar_ts == _BAR_TS
    assert rec.revision_ts == _REV_TS


@pytest.mark.unit
def test_anchor_state_compute_pnl_zero_price_holds_pnl():
    """When prev_price is 0.0, pnl is held unchanged (no division)."""
    state = AnchorState()
    state.set("strat_1", AnchorRecord(pnl=5.0, price=0.0, position=1.0))
    new_pnl = state.compute_pnl("strat_1", current_price=100.0, position=1.0)
    assert new_pnl == pytest.approx(5.0)
    assert state.get("strat_1").price == pytest.approx(100.0)


@pytest.mark.unit
def test_anchor_state_compute_pnl_raises_for_unknown_strategy():
    """compute_pnl raises RuntimeError if strategy has not been seeded."""
    state = AnchorState()
    with pytest.raises(RuntimeError, match="No anchor state"):
        state.compute_pnl("not_seeded", current_price=100.0, position=1.0)


@pytest.mark.unit
def test_anchor_state_compute_pnl_chain():
    """Chain of compute_pnl calls accumulates pnl correctly."""
    state = AnchorState()
    state.set("strat_1", AnchorRecord(pnl=0.0, price=100.0, position=1.0))
    # Minute 1: 100 → 110, position=1.0: delta = 0.10
    pnl1 = state.compute_pnl("strat_1", current_price=110.0, position=1.0)
    # Minute 2: 110 → 121, position=1.0: delta = 110/110 = 0.10
    pnl2 = state.compute_pnl("strat_1", current_price=121.0, position=1.0)
    assert pnl1 == pytest.approx(0.10)
    assert pnl2 == pytest.approx(0.20)


@pytest.mark.unit
def test_anchor_state_should_apply_revision_new_bar():
    """should_apply_revision returns True for a newer bar."""
    state = AnchorState()
    state.set("strat_rt", AnchorRecord(bar_ts=_BAR_TS, revision_ts=_REV_TS))
    new_bar = datetime(2026, 5, 10, 11, 0, 0)
    new_rev = datetime(2026, 5, 10, 11, 2, 0)
    assert state.should_apply_revision("strat_rt", new_bar, new_rev)


@pytest.mark.unit
def test_anchor_state_should_apply_revision_same_bar_newer_revision():
    """should_apply_revision returns True for same bar with newer revision_ts."""
    state = AnchorState()
    state.set("strat_rt", AnchorRecord(bar_ts=_BAR_TS, revision_ts=_REV_TS))
    later_rev = datetime(2026, 5, 10, 10, 8, 0)
    assert state.should_apply_revision("strat_rt", _BAR_TS, later_rev)


@pytest.mark.unit
def test_anchor_state_should_apply_revision_same_bar_same_revision_ignored():
    """should_apply_revision returns False when (bar_ts, revision_ts) are identical."""
    state = AnchorState()
    state.set("strat_rt", AnchorRecord(bar_ts=_BAR_TS, revision_ts=_REV_TS))
    assert not state.should_apply_revision("strat_rt", _BAR_TS, _REV_TS)


@pytest.mark.unit
def test_anchor_state_should_apply_revision_older_bar_ignored():
    """should_apply_revision returns False for a stale revision of an older bar."""
    state = AnchorState()
    newer_bar = datetime(2026, 5, 10, 11, 0, 0)
    state.set("strat_rt", AnchorRecord(bar_ts=newer_bar, revision_ts=_REV_TS))
    # Older bar even with newer revision_ts — should be ignored
    old_bar = datetime(2026, 5, 10, 10, 0, 0)
    late_rev = datetime(2026, 5, 10, 12, 0, 0)
    assert not state.should_apply_revision("strat_rt", old_bar, late_rev)


@pytest.mark.unit
def test_anchor_state_should_apply_revision_no_anchor_always_applies():
    """should_apply_revision for an unseeded strategy always returns True (datetime.min baseline)."""
    state = AnchorState()
    new_bar = datetime(2026, 5, 10, 10, 0, 0)
    new_rev = datetime(2026, 5, 10, 10, 3, 0)
    assert state.should_apply_revision("brand_new", new_bar, new_rev)
