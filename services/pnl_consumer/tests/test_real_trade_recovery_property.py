"""Property test: restoring AnchorState from checkpoint mid-stream produces the
same final state as running through without a restart.

Drives `AnchorState.should_apply_revision` + `compute_pnl` directly, mirroring
what the consumer does per Kafka message. The crash point is varied; the final
states (and final state hashes) must match for every crash point.
"""

from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.checkpoint_store import (
    compute_state_hash,
    read_checkpoint,
    write_checkpoint,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def fake_pg_conn():
    """In-memory shim that satisfies write_checkpoint/read_checkpoint."""
    store = {"checkpoint": {}, "commit_state": {}}

    def make_cursor():
        cur = mock.MagicMock()
        cur.__enter__ = mock.MagicMock(return_value=cur)
        cur.__exit__ = mock.MagicMock(return_value=False)

        def execute(sql, params=None):
            cur._last_sql = sql
            if params is None:
                return
            if "FROM streaming.pnl_commit_state" in sql:
                mode = params[0]
                cur.description = [
                    (c,)
                    for c in [
                        "mode",
                        "last_candle_ts",
                        "kafka_topic",
                        "kafka_partition",
                        "kafka_offset",
                        "state_hash",
                        "schema_version",
                    ]
                ]
                cur._rows = (
                    [store["commit_state"][mode]]
                    if mode in store["commit_state"]
                    else []
                )
            elif "FROM streaming.pnl_checkpoint" in sql:
                mode = params[0]
                cur.description = [
                    (c,)
                    for c in [
                        "strategy_table_name",
                        "pnl",
                        "price",
                        "position",
                        "bar_ts",
                        "revision_ts",
                        "strategy_id",
                        "strategy_name",
                        "underlying",
                        "config_timeframe",
                        "weighting",
                        "strategy_instance_id",
                        "final_signal",
                        "benchmark",
                    ]
                ]
                cur._rows = [
                    v[1:-1] for (m, _stn), v in store["checkpoint"].items() if m == mode
                ]
            elif "INTO streaming.pnl_commit_state" in sql:
                store["commit_state"][params[0]] = params

        def executemany(sql, params_list):
            for p in params_list:
                store["checkpoint"][(p[0], p[1])] = p + (None,)

        def fetchall():
            return cur._rows

        cur.execute = execute
        cur.executemany = executemany
        cur.fetchall = fetchall
        return cur

    conn = mock.MagicMock()
    conn.cursor.side_effect = make_cursor
    return conn


def _seed_state() -> AnchorState:
    s = AnchorState()
    s.set(
        "strat_a",
        AnchorRecord(
            pnl=0.0,
            price=100.0,
            position=0.0,
            bar_ts=datetime.min.replace(tzinfo=UTC),
            revision_ts=datetime.min.replace(tzinfo=UTC),
            strategy_id=1,
            strategy_name="s",
            underlying="BTC",
            config_timeframe="1m",
            weighting=1.0,
            strategy_instance_id="i",
            final_signal=0.0,
            benchmark=0.0,
        ),
    )
    return s


def _stream():
    """Deterministic real-trade revision stream: 6 revisions across 3 bars.

    Each tuple is (bar_ts, revision_ts, position, price).
    """
    base_bar = datetime(2024, 6, 1, 10, 0, tzinfo=UTC)
    base_rev = base_bar

    def td(s):
        return timedelta(seconds=s)

    return [
        (base_bar, base_rev, 1.0, 101.0),
        (base_bar, base_rev + td(30), 1.5, 102.0),
        (base_bar + td(60), base_rev + td(60), 2.0, 103.0),
        (base_bar, base_rev - td(10), 9.9, 999.0),  # late, rejected
        (base_bar + td(60), base_rev + td(90), 2.5, 104.0),
        (base_bar + td(120), base_rev + td(120), 3.0, 105.0),
    ]


def _apply_revision(state: AnchorState, bar_ts, revision_ts, position, price):
    if not state.should_apply_revision("strat_a", bar_ts, revision_ts):
        return
    state.compute_pnl(
        "strat_a",
        current_price=price,
        position=position,
        bar_ts=bar_ts,
        revision_ts=revision_ts,
    )


@pytest.mark.parametrize("crash_after", [1, 2, 3, 4, 5])
def test_crash_at_each_step_matches_fresh_run(fake_pg_conn, crash_after):
    """For each possible crash point, restoring matches a fresh run."""
    stream = _stream()

    # Reference run: no crash.
    fresh = _seed_state()
    for rev in stream:
        _apply_revision(fresh, *rev)
    fresh_hash = compute_state_hash(fresh)

    # Crash run: apply first crash_after revisions, checkpoint, restore, finish.
    crashed = _seed_state()
    for rev in stream[:crash_after]:
        _apply_revision(crashed, *rev)

    write_checkpoint(
        mode="real_trade",
        anchor_state=crashed,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=crash_after,
        last_candle_ts=stream[crash_after - 1][0],  # bar_ts of last applied
        client=fake_pg_conn,
    )

    loaded = read_checkpoint(mode="real_trade", client=fake_pg_conn)
    assert loaded is not None
    restored = loaded.anchor_state

    for rev in stream[crash_after:]:
        _apply_revision(restored, *rev)

    assert (
        compute_state_hash(restored) == fresh_hash
    ), f"crash_after={crash_after}: restored state diverged from fresh run"
