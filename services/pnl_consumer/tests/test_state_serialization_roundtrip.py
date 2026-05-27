"""Property test: write_checkpoint → read_checkpoint preserves AnchorState semantics.

Avoids needing a live Postgres by simulating the round-trip via in-memory
storage in mock cursors. Validates that compute_state_hash before write equals
compute_state_hash after read.
"""

from datetime import UTC, datetime
from unittest import mock

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.checkpoint_store import (
    compute_state_hash,
    read_checkpoint,
    write_checkpoint,
)


class _InMemoryCheckpointStore:
    """Tiny fake Postgres-shaped store. Stores tuples; replays them on SELECT."""

    def __init__(self):
        self.pnl_checkpoint: dict[tuple[str, str], tuple] = {}
        self.pnl_commit_state: dict[str, tuple] = {}

    def make_conn(self):
        store = self
        conn = mock.MagicMock()

        def make_cursor():
            cur = mock.MagicMock()
            cur.__enter__ = mock.MagicMock(return_value=cur)
            cur.__exit__ = mock.MagicMock(return_value=False)

            def execute(sql, params=None):
                cur._last_sql = sql
                cur._last_params = params
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
                        [store.pnl_commit_state[mode]]
                        if mode in store.pnl_commit_state
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
                        # drop the first element (mode) and the updated_at (last col)
                        v[1:-1]
                        for k, v in store.pnl_checkpoint.items()
                        if k[0] == mode
                    ]
                elif "INTO streaming.pnl_commit_state" in sql:
                    # params: (mode, last_candle_ts, kafka_topic, kafka_partition,
                    #          kafka_offset, state_hash, schema_version)
                    store.pnl_commit_state[params[0]] = params

            def executemany(sql, params_list):
                # INSERT INTO streaming.pnl_checkpoint
                # params: (mode, strategy_table_name, pnl, price, position,
                #   bar_ts, revision_ts, strategy_id, strategy_name,
                #   underlying, config_timeframe, weighting,
                #   strategy_instance_id, final_signal, benchmark)
                # Append updated_at=None slot for read-side symmetry.
                for p in params_list:
                    store.pnl_checkpoint[(p[0], p[1])] = p + (None,)

            def fetchall():
                return cur._rows

            cur.execute = execute
            cur.executemany = executemany
            cur.fetchall = fetchall
            return cur

        conn.cursor.side_effect = make_cursor
        return conn


@pytest.mark.unit
def test_write_then_read_preserves_hash():
    state = AnchorState()
    state.set(
        "s_a",
        AnchorRecord(
            pnl=1.5,
            price=100.25,
            position=2.0,
            bar_ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            revision_ts=datetime(2024, 1, 1, 12, 0, 30, tzinfo=UTC),
            strategy_id=7,
            strategy_name="alpha",
            underlying="BTC",
            config_timeframe="1m",
            weighting=0.5,
            strategy_instance_id="alpha-btc-1m",
            final_signal=1.0,
            benchmark=0.0,
        ),
    )
    state.set(
        "s_b",
        AnchorRecord(
            pnl=-2.5,
            price=50.5,
            position=-1.0,
            bar_ts=datetime(2024, 1, 1, 11, 0, tzinfo=UTC),
            revision_ts=datetime(2024, 1, 1, 11, 0, 45, tzinfo=UTC),
            strategy_id=9,
            strategy_name="beta",
            underlying="ETH",
            config_timeframe="5m",
            weighting=1.0,
            strategy_instance_id="beta-eth-5m",
            final_signal=-1.0,
            benchmark=0.0,
        ),
    )
    hash_before = compute_state_hash(state)

    fake = _InMemoryCheckpointStore()
    conn = fake.make_conn()

    write_checkpoint(
        mode="prod",
        anchor_state=state,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=99,
        last_candle_ts=datetime(2024, 1, 1, 12, 1, tzinfo=UTC),
        client=conn,
    )

    loaded = read_checkpoint(mode="prod", client=conn)
    assert loaded is not None
    assert loaded.commit_state.kafka_offset == 99
    hash_after = compute_state_hash(loaded.anchor_state)
    assert hash_before == hash_after
