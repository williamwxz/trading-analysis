"""Integration-style unit tests for load_or_bootstrap orchestration.

Mocks out checkpoint_store + bootstrap entry points; verifies the decision tree.
"""

from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest

from libs.computation.anchor_state import AnchorState
from libs.computation.checkpoint_store import (
    SCHEMA_VERSION,
    CheckpointLoadResult,
    CommitStateRow,
    compute_state_hash,
)

_NOW = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)


@pytest.mark.unit
def test_load_or_bootstrap_no_checkpoint_falls_back():
    """When read_checkpoint returns None, falls back to bootstrap."""
    from pnl_consumer.cold_start import load_or_bootstrap

    fake_bootstrap = mock.MagicMock(return_value=AnchorState())
    fake_read = mock.MagicMock(return_value=None)

    state, reason = load_or_bootstrap(
        mode="prod",
        pg_client=mock.MagicMock(),
        kafka_committed_offset=101,
        now=_NOW,
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=fake_read,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    fake_bootstrap.assert_called_once()
    assert reason.value == "no_checkpoint"


@pytest.mark.unit
def test_load_or_bootstrap_happy_path_returns_checkpoint_state():
    from pnl_consumer.cold_start import load_or_bootstrap

    state = AnchorState()
    from libs.computation.anchor_state import AnchorRecord

    state.set(
        "a",
        AnchorRecord(
            pnl=1.0,
            price=100.0,
            position=1.0,
            bar_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
            revision_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
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
    cs = CommitStateRow(
        mode="prod",
        last_candle_ts=_NOW - timedelta(minutes=1),
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=100,
        state_hash=compute_state_hash(state),
        schema_version=SCHEMA_VERSION,
    )
    result = CheckpointLoadResult(commit_state=cs, anchor_state=state)

    fake_bootstrap = mock.MagicMock()
    fake_read = mock.MagicMock(return_value=result)

    loaded_state, reason = load_or_bootstrap(
        mode="prod",
        pg_client=mock.MagicMock(),
        kafka_committed_offset=101,
        now=_NOW,
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=fake_read,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    fake_bootstrap.assert_not_called()
    assert reason is None
    assert loaded_state is state


@pytest.mark.unit
def test_load_or_bootstrap_stale_falls_back():
    from pnl_consumer.cold_start import load_or_bootstrap

    from libs.computation.anchor_state import AnchorRecord

    state = AnchorState()
    state.set(
        "a",
        AnchorRecord(
            pnl=1.0,
            price=100.0,
            position=1.0,
            bar_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
            revision_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
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
    cs = CommitStateRow(
        mode="prod",
        last_candle_ts=_NOW - timedelta(hours=25),  # stale
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=100,
        state_hash=compute_state_hash(state),
        schema_version=SCHEMA_VERSION,
    )
    result = CheckpointLoadResult(commit_state=cs, anchor_state=state)

    fake_bootstrap = mock.MagicMock(return_value=AnchorState())
    fake_read = mock.MagicMock(return_value=result)

    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=mock.MagicMock(),
        kafka_committed_offset=101,
        now=_NOW,
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=fake_read,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    fake_bootstrap.assert_called_once()
    assert reason.value == "stale"


@pytest.mark.unit
def test_load_or_bootstrap_invariant_failure_falls_back():
    """If invariant_check_fn returns INVARIANT_MISMATCH, fall back."""
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap

    from libs.computation.anchor_state import AnchorRecord

    state = AnchorState()
    state.set(
        "a",
        AnchorRecord(
            pnl=1.0,
            price=100.0,
            position=1.0,
            bar_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
            revision_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
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
    cs = CommitStateRow(
        mode="prod",
        last_candle_ts=_NOW - timedelta(minutes=1),
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=100,
        state_hash=compute_state_hash(state),
        schema_version=SCHEMA_VERSION,
    )
    result = CheckpointLoadResult(commit_state=cs, anchor_state=state)

    fake_bootstrap = mock.MagicMock(return_value=AnchorState())
    fake_read = mock.MagicMock(return_value=result)
    fake_invariant = mock.MagicMock(return_value=FallbackReason.INVARIANT_MISMATCH)

    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=mock.MagicMock(),
        kafka_committed_offset=101,
        now=_NOW,
        max_age_seconds=86400,
        invariant_check_enabled=True,
        read_checkpoint_fn=fake_read,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=fake_invariant,
    )
    fake_invariant.assert_called_once()
    fake_bootstrap.assert_called_once()
    assert reason == FallbackReason.INVARIANT_MISMATCH


@pytest.mark.unit
def test_load_or_bootstrap_read_exception_falls_back():
    """If read_checkpoint raises (e.g., Postgres down), fall back."""
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap

    fake_bootstrap = mock.MagicMock(return_value=AnchorState())
    fake_read = mock.MagicMock(side_effect=RuntimeError("postgres down"))

    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=mock.MagicMock(),
        kafka_committed_offset=101,
        now=_NOW,
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=fake_read,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason == FallbackReason.CHECKPOINT_READ_ERROR
    fake_bootstrap.assert_called_once()
