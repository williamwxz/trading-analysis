import json
from datetime import datetime
from unittest.mock import patch

import pytest

from libs.computation.candle_lookup import (
    StrategyRevision,
    fetch_bt_anchors_for_candle,
    fetch_real_trade_for_candle,
    fetch_strategies_for_candle,
)


@pytest.mark.unit
def test_fetch_strategies_returns_list_of_strategy_bars():
    # ClickHouse stores short names (BTC, ETH); instrument arg is full symbol (BTCUSDT)
    mock_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_instance_id": "strat_prod_1__1",
            "strategy_id": 1,
            "strategy_name": "momentum",
            "underlying": "BTC",
            "config_timeframe": "5m",
            "weighting": 1.0,
            "latest_ts": datetime(2026, 4, 26, 0, 0, 0),
            "row_json": '{"position": 0.5, "final_signal": 1.0, "benchmark": 0.0}',
        }
    ]
    with patch("libs.computation.candle_lookup.query_dicts", return_value=mock_rows):
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
    with patch("libs.computation.candle_lookup.query_dicts", return_value=[]):
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
            "strategy_instance_id": "strat_prod_2__2",
            "strategy_id": 2,
            "strategy_name": "mean_rev",
            "underlying": "ETH",
            "config_timeframe": "15m",
            "weighting": 0.5,
            "latest_ts": datetime(2026, 4, 26, 0, 0, 0),
            "row_json": '{"position": -1.0, "final_signal": -1.0, "benchmark": 0.01}',
        }
    ]
    with patch("libs.computation.candle_lookup.query_dicts", return_value=mock_rows):
        bars = fetch_strategies_for_candle(
            instrument="ETHUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 0, 0),
        )
    assert bars[0].position == -1.0
    assert bars[0].final_signal == -1.0
    assert bars[0].benchmark == 0.01


@pytest.mark.unit
def test_fetch_real_trade_returns_list_of_strategy_revisions():
    mock_rows = [
        {
            "strategy_table_name": "strat_rt_1",
            "strategy_instance_id": "strat_rt_1__1",
            "strategy_id": 1,
            "strategy_name": "momentum",
            "underlying": "BTC",
            "config_timeframe": "5m",
            "weighting": 1.0,
            "bar_ts": datetime(2026, 4, 26, 0, 0, 0),
            "max_revision_ts": datetime(2026, 4, 26, 0, 1, 10),
            "row_json": '{"position": 0.5, "final_signal": 1.0, "benchmark": 0.0}',
        }
    ]
    with patch("libs.computation.candle_lookup.query_dicts", return_value=mock_rows):
        revisions = fetch_real_trade_for_candle(
            instrument="BTCUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 1, 0),
        )
    assert len(revisions) == 1
    rev = revisions[0]
    assert isinstance(rev, StrategyRevision)
    assert rev.strategy_table_name == "strat_rt_1"
    assert rev.position == 0.5
    assert rev.revision_ts == datetime(2026, 4, 26, 0, 1, 10)
    assert rev.bar_ts == datetime(2026, 4, 26, 0, 0, 0)


@pytest.mark.unit
def test_fetch_real_trade_returns_empty_when_no_rows():
    with patch("libs.computation.candle_lookup.query_dicts", return_value=[]):
        revisions = fetch_real_trade_for_candle(
            instrument="BTCUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 1, 0),
        )
    assert revisions == []


@pytest.mark.unit
def test_fetch_real_trade_filters_by_revision_ts():
    """Verify the SQL uses revision_ts <= candle_ts."""
    captured_sql = []

    def capture(sql):
        captured_sql.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_real_trade_for_candle(
            instrument="BTCUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 1, 0),
        )
    assert "2026-04-26 00:01:00" in captured_sql[0]
    assert "revision_ts" in captured_sql[0]


@pytest.mark.unit
def test_fetch_strategies_uses_closing_ts_filter():
    """Prod/bt positions activate at closing_ts (ts + tf_minutes), not ts."""
    captured_sql = []

    def capture(sql):
        captured_sql.append(sql)
        return []

    with patch("libs.computation.candle_lookup.query_dicts", side_effect=capture):
        fetch_strategies_for_candle(
            instrument="BTCUSDT",
            candle_ts=datetime(2026, 5, 10, 18, 5, 0),
        )
    sql = captured_sql[0]
    assert "ts + toIntervalMinute" in sql, "must filter on closing_ts, not raw ts"
    assert "<= '2026-05-10 18:05:00'" in sql


# ---------------------------------------------------------------------------
# Regression tests for the prod/bt "oldest bar leaks into the result" bug.
#
# These execute the *actual* SQL the lookup functions generate against an
# embedded ClickHouse engine (chdb), with a fixture where the oldest in-window
# bar's position (-1) differs from the latest bar's first-revision position (1).
# A mock returning canned rows cannot catch this because the defect lives in the
# SQL aggregation semantics, not in the Python parsing.
# ---------------------------------------------------------------------------

chs = pytest.importorskip(
    "chdb.session", reason="chdb required for SQL-semantics regression tests"
)

_DATETIME_COLS = {"latest_ts", "bar_ts", "max_revision_ts"}


def _make_query_dicts(session):
    """Return a query_dicts replacement that runs SQL against the chdb session."""

    def _run(sql):
        raw = str(session.query(sql, "JSONEachRow"))
        rows = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            for col in _DATETIME_COLS:
                if col in row and isinstance(row[col], str):
                    row[col] = datetime.strptime(row[col], "%Y-%m-%d %H:%M:%S")
            rows.append(row)
        return rows

    return _run


@pytest.fixture
def history_session():
    session = chs.Session()
    session.query("CREATE DATABASE IF NOT EXISTS analytics")
    for table in ("strategy_output_history_v2", "strategy_output_history_bt_v2"):
        session.query(f"""
            CREATE TABLE analytics.{table} (
                strategy_table_name String,
                config_timeframe String,
                ts DateTime,
                revision_ts DateTime,
                strategy_id Int32,
                strategy_instance_id String,
                strategy_name String,
                underlying String,
                weighting Float64,
                row_json String
            ) ENGINE = MergeTree ORDER BY (strategy_instance_id, ts, revision_ts)
            """)
    yield session
    session.close()


def _seed_old_and_latest_bar(session, table):
    """Insert an old in-window bar (pos -1) and a latest bar with two revisions.

    Latest bar (ts=17:00) first revision has position 1.0; a later revision has
    position 0.5. Correct prod/bt semantics return the latest bar's *first*
    revision (1.0). The buggy argMin-over-window returns the oldest revision in
    the 2-day window (the -1.0 bar from two days earlier).
    """
    session.query(f"""INSERT INTO analytics.{table} VALUES
        ('t','1h','2026-05-25 18:00:00','2026-05-25 19:12:00',1,'sid','m','XRP',0.5,
         '{{"position":-1.0,"final_signal":-1.0,"benchmark":0.0}}'),
        ('t','1h','2026-05-27 17:00:00','2026-05-27 18:14:00',1,'sid','m','XRP',0.5,
         '{{"position":1.0,"final_signal":1.0,"benchmark":0.1}}'),
        ('t','1h','2026-05-27 17:00:00','2026-05-27 18:30:00',1,'sid','m','XRP',0.5,
         '{{"position":0.5,"final_signal":0.5,"benchmark":0.2}}')
        """)


@pytest.mark.unit
def test_prod_lookup_picks_latest_bar_first_revision_not_oldest_in_window(
    history_session,
):
    _seed_old_and_latest_bar(history_session, "strategy_output_history_v2")
    with patch(
        "libs.computation.candle_lookup.query_dicts",
        side_effect=_make_query_dicts(history_session),
    ):
        bars = fetch_strategies_for_candle(
            instrument="XRPUSDT",
            candle_ts=datetime(2026, 5, 27, 18, 0, 0),
        )
    assert len(bars) == 1
    bar = bars[0]
    # the latest closed bar is ts=17:00 — an older in-window bar must not leak in
    assert bar.bar_ts == datetime(2026, 5, 27, 17, 0, 0)
    # first revision of the latest bar (1.0), not the oldest-in-window bar (-1.0)
    # and not the latest bar's later revision (0.5)
    assert bar.position == 1.0
    assert bar.final_signal == 1.0


# ---------------------------------------------------------------------------
# BT lookup closing_ts gate — executes the real SQL against embedded ClickHouse.
# BT must match prod: a cum-table bar's pos_first takes effect at
# closing_ts (= ts + tf_minutes), not at the bar-open ts.
# ---------------------------------------------------------------------------

_BT_STN = "sid=9|sno=9|u=XRP|name=btstrat|inst=ib1"


@pytest.fixture
def bt_session():
    session = chs.Session()
    session.query("CREATE DATABASE IF NOT EXISTS analytics")
    session.query("""
        CREATE TABLE analytics.strategy_cum_pnl_bt_v2 (
            strategy_table_name String,
            config_timeframe String,
            ts DateTime,
            pnl_first Float64,
            pnl_latest Float64,
            pos_first Float64,
            pos_latest Float64,
            cum_pnl_first Float64,
            cum_pnl_latest Float64,
            computed_at DateTime,
            weighting Float64
        ) ENGINE = ReplacingMergeTree(computed_at)
        ORDER BY (strategy_table_name, config_timeframe, ts)
        """)
    session.query("""
        CREATE TABLE analytics.strategy_output_history_bt_v2 (
            strategy_table_name String,
            config_timeframe String,
            ts DateTime,
            revision_ts DateTime,
            strategy_id Int32,
            strategy_instance_id String,
            strategy_name String,
            underlying String,
            weighting Float64,
            row_json String
        ) ENGINE = MergeTree ORDER BY (strategy_instance_id, ts, revision_ts)
        """)
    session.query(f"""INSERT INTO analytics.strategy_cum_pnl_bt_v2 VALUES
        ('{_BT_STN}','1h','2026-05-27 17:00:00',0,0, 1.0,1.0, 0.10,0.10,
         '2026-05-27 18:05:00',0.5),
        ('{_BT_STN}','1h','2026-05-27 18:00:00',0,0,-1.0,-1.0, 0.20,0.20,
         '2026-05-27 19:05:00',0.5)
        """)
    session.query(f"""INSERT INTO analytics.strategy_output_history_bt_v2 VALUES
        ('{_BT_STN}','1h','2026-05-27 17:00:00','2026-05-27 18:05:00',9,'ib1','btstrat',
         'XRP',0.5,'{{"position":1.0,"final_signal":0.0,"benchmark":0.07}}'),
        ('{_BT_STN}','1h','2026-05-27 18:00:00','2026-05-27 19:05:00',9,'ib1','btstrat',
         'XRP',0.5,'{{"position":-1.0,"final_signal":0.0,"benchmark":0.09}}')
        """)
    yield session
    session.close()


@pytest.mark.unit
def test_bt_lookup_activates_bar_at_closing_ts(bt_session):
    """Mid-bar (18:30) the 18:00 bar has not closed — 17:00's position holds."""
    with patch(
        "libs.computation.candle_lookup.query_dicts",
        side_effect=_make_query_dicts(bt_session),
    ):
        anchors = fetch_bt_anchors_for_candle(
            instrument="XRPUSDT",
            candle_ts=datetime(2026, 5, 27, 18, 30, 0),
        )
    assert len(anchors) == 1
    a = anchors[0]
    assert a.anchor_ts == "2026-05-27 17:00:00"
    assert a.pos_first == 1.0
    assert a.benchmark == 0.07


@pytest.mark.unit
def test_bt_lookup_switches_bar_once_it_closes(bt_session):
    """At 19:00 the 18:00 bar closes (ts + 60m) and its position takes over."""
    with patch(
        "libs.computation.candle_lookup.query_dicts",
        side_effect=_make_query_dicts(bt_session),
    ):
        anchors = fetch_bt_anchors_for_candle(
            instrument="XRPUSDT",
            candle_ts=datetime(2026, 5, 27, 19, 0, 0),
        )
    assert len(anchors) == 1
    a = anchors[0]
    assert a.anchor_ts == "2026-05-27 18:00:00"
    assert a.pos_first == -1.0
    assert a.benchmark == 0.09
