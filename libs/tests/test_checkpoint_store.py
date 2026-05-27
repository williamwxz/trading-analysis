"""Unit tests for libs.computation.checkpoint_store."""

from datetime import UTC, datetime
from unittest import mock

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState


def _make_state(records: dict[str, AnchorRecord]) -> AnchorState:
    state = AnchorState()
    for k, rec in records.items():
        state.set(k, rec)
    return state


def _rec(
    pnl=0.0, price=100.0, position=1.0, bar_ts=None, revision_ts=None
) -> AnchorRecord:
    return AnchorRecord(
        pnl=pnl,
        price=price,
        position=position,
        bar_ts=bar_ts or datetime(2024, 1, 1, tzinfo=UTC),
        revision_ts=revision_ts or datetime(2024, 1, 1, tzinfo=UTC),
        strategy_id=1,
        strategy_name="s",
        underlying="BTC",
        config_timeframe="1m",
        weighting=1.0,
        strategy_instance_id="i",
        final_signal=0.0,
        benchmark=0.0,
    )


@pytest.mark.unit
def test_compute_state_hash_deterministic():
    from libs.computation.checkpoint_store import compute_state_hash

    state = _make_state({"a": _rec(pnl=1.5)})
    h1 = compute_state_hash(state)
    h2 = compute_state_hash(state)
    assert h1 == h2
    assert len(h1) == 64  # sha256 hex


@pytest.mark.unit
def test_compute_state_hash_insertion_order_independent():
    from libs.computation.checkpoint_store import compute_state_hash

    s1 = _make_state({"a": _rec(pnl=1.0), "b": _rec(pnl=2.0)})
    s2 = _make_state({"b": _rec(pnl=2.0), "a": _rec(pnl=1.0)})
    assert compute_state_hash(s1) == compute_state_hash(s2)


@pytest.mark.unit
def test_compute_state_hash_sensitive_to_pnl_change():
    from libs.computation.checkpoint_store import compute_state_hash

    s1 = _make_state({"a": _rec(pnl=1.5)})
    s2 = _make_state({"a": _rec(pnl=1.5000001)})
    assert compute_state_hash(s1) != compute_state_hash(s2)


@pytest.mark.unit
def test_compute_state_hash_insensitive_to_sub_picosecond_pnl_change():
    """Rounding to 12 decimal places means changes below that don't move the hash."""
    from libs.computation.checkpoint_store import compute_state_hash

    s1 = _make_state({"a": _rec(pnl=1.5)})
    s2 = _make_state({"a": _rec(pnl=1.5 + 1e-15)})
    assert compute_state_hash(s1) == compute_state_hash(s2)


@pytest.mark.unit
def test_compute_state_hash_sensitive_to_revision_ts():
    from libs.computation.checkpoint_store import compute_state_hash

    s1 = _make_state({"a": _rec(revision_ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC))})
    s2 = _make_state(
        {"a": _rec(revision_ts=datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC))}
    )
    assert compute_state_hash(s1) != compute_state_hash(s2)


@pytest.mark.unit
def test_schema_version_constant():
    from libs.computation import checkpoint_store

    assert isinstance(checkpoint_store.SCHEMA_VERSION, int)
    assert checkpoint_store.SCHEMA_VERSION >= 1


@pytest.mark.unit
def test_write_checkpoint_executes_two_upserts_in_transaction():
    from libs.computation.checkpoint_store import write_checkpoint

    state = _make_state({"strat_a": _rec(pnl=1.5, price=100.0, position=2.0)})

    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    write_checkpoint(
        mode="prod",
        anchor_state=state,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=42,
        last_candle_ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        client=fake_conn,
    )

    # Expect at least two executes: one for pnl_checkpoint UPSERT (executemany),
    # one for pnl_commit_state UPSERT (execute).
    total_calls = fake_cur.execute.call_count + fake_cur.executemany.call_count
    assert total_calls >= 2
    fake_conn.commit.assert_called_once()


@pytest.mark.unit
def test_write_checkpoint_rolls_back_on_failure():
    from libs.computation.checkpoint_store import write_checkpoint

    state = _make_state({"strat_a": _rec()})

    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_cur.executemany.side_effect = RuntimeError("simulated postgres failure")
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    with pytest.raises(RuntimeError, match="simulated"):
        write_checkpoint(
            mode="prod",
            anchor_state=state,
            kafka_topic="binance.price.ticks",
            kafka_partition=0,
            kafka_offset=42,
            last_candle_ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            client=fake_conn,
        )
    fake_conn.rollback.assert_called_once()
    fake_conn.commit.assert_not_called()


@pytest.mark.unit
def test_write_checkpoint_empty_state_writes_commit_state_only():
    """An empty AnchorState shouldn't crash — just no pnl_checkpoint rows."""
    from libs.computation.checkpoint_store import write_checkpoint

    state = AnchorState()

    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    write_checkpoint(
        mode="prod",
        anchor_state=state,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=42,
        last_candle_ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        client=fake_conn,
    )

    # No executemany since there are no checkpoint rows; one explicit execute
    # for pnl_commit_state.
    assert fake_cur.executemany.call_count == 0
    assert fake_cur.execute.call_count == 1
    fake_conn.commit.assert_called_once()


@pytest.mark.unit
def test_read_checkpoint_returns_none_when_no_commit_state():
    from libs.computation.checkpoint_store import read_checkpoint

    fake_cur = mock.MagicMock()
    fake_cur.__enter__ = mock.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mock.MagicMock(return_value=False)
    fake_cur.description = [
        ("mode",),
        ("last_candle_ts",),
        ("kafka_topic",),
        ("kafka_partition",),
        ("kafka_offset",),
        ("state_hash",),
        ("schema_version",),
    ]
    fake_cur.fetchall.return_value = []
    fake_conn = mock.MagicMock()
    fake_conn.cursor.return_value = fake_cur

    result = read_checkpoint(mode="prod", client=fake_conn)
    assert result is None


@pytest.mark.unit
def test_compute_state_hash_stable_across_naive_and_aware_datetimes():
    """Postgres TIMESTAMPTZ returns UTC-aware datetimes; in-memory may be naive.
    compute_state_hash must produce the same hash for both forms."""
    from libs.computation.anchor_state import AnchorRecord, AnchorState
    from libs.computation.checkpoint_store import compute_state_hash

    naive = AnchorState()
    naive.set("a", AnchorRecord(
        pnl=1.5, price=100.0, position=2.0,
        bar_ts=datetime(2024, 1, 1, 12, 0),  # naive
        revision_ts=datetime(2024, 1, 1, 12, 0, 30),  # naive
        strategy_id=1, strategy_name="s", underlying="BTC",
        config_timeframe="1m", weighting=1.0,
        strategy_instance_id="i", final_signal=0.0, benchmark=0.0,
    ))

    aware = AnchorState()
    aware.set("a", AnchorRecord(
        pnl=1.5, price=100.0, position=2.0,
        bar_ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),  # UTC-aware
        revision_ts=datetime(2024, 1, 1, 12, 0, 30, tzinfo=UTC),  # UTC-aware
        strategy_id=1, strategy_name="s", underlying="BTC",
        config_timeframe="1m", weighting=1.0,
        strategy_instance_id="i", final_signal=0.0, benchmark=0.0,
    ))

    # After the _strip_tz normalization in _canonical_record, both forms must
    # produce the same hash.
    assert compute_state_hash(naive) == compute_state_hash(aware)


@pytest.mark.unit
def test_read_checkpoint_returns_load_result():
    from libs.computation.checkpoint_store import CheckpointLoadResult, read_checkpoint

    # Two separate cursor uses: one for pnl_commit_state, one for pnl_checkpoint.
    cs_cur = mock.MagicMock()
    cs_cur.__enter__ = mock.MagicMock(return_value=cs_cur)
    cs_cur.__exit__ = mock.MagicMock(return_value=False)
    cs_cur.description = [
        ("mode",),
        ("last_candle_ts",),
        ("kafka_topic",),
        ("kafka_partition",),
        ("kafka_offset",),
        ("state_hash",),
        ("schema_version",),
    ]
    cs_cur.fetchall.return_value = [
        (
            "prod",
            datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            "binance.price.ticks",
            0,
            100,
            "deadbeef",
            1,
        )
    ]

    cp_cur = mock.MagicMock()
    cp_cur.__enter__ = mock.MagicMock(return_value=cp_cur)
    cp_cur.__exit__ = mock.MagicMock(return_value=False)
    cp_cur.description = [
        ("strategy_table_name",),
        ("pnl",),
        ("price",),
        ("position",),
        ("bar_ts",),
        ("revision_ts",),
        ("strategy_id",),
        ("strategy_name",),
        ("underlying",),
        ("config_timeframe",),
        ("weighting",),
        ("strategy_instance_id",),
        ("final_signal",),
        ("benchmark",),
    ]
    cp_cur.fetchall.return_value = [
        (
            "strat_a",
            1.5,
            100.0,
            2.0,
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, tzinfo=UTC),
            1,
            "s",
            "BTC",
            "1m",
            1.0,
            "i",
            0.0,
            0.0,
        )
    ]

    fake_conn = mock.MagicMock()
    fake_conn.cursor.side_effect = [cs_cur, cp_cur]

    result = read_checkpoint(mode="prod", client=fake_conn)
    assert isinstance(result, CheckpointLoadResult)
    assert result.commit_state.kafka_offset == 100
    assert result.commit_state.state_hash == "deadbeef"
    assert result.commit_state.schema_version == 1
    assert "strat_a" in result.anchor_state.keys()
    assert result.anchor_state.get("strat_a").pnl == 1.5
