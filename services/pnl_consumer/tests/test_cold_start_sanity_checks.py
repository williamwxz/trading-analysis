"""Unit tests for the cold-start sanity check functions.

Each check is tested in isolation with fixture data. The orchestration of all
checks together is covered in test_cold_start_orchestration.py.
"""

from datetime import UTC, datetime, timedelta

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.checkpoint_store import (
    SCHEMA_VERSION,
    CheckpointLoadResult,
    CommitStateRow,
    compute_state_hash,
)

_NOW = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)


def _make_state_with_one() -> AnchorState:
    s = AnchorState()
    s.set(
        "strat_a",
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
    return s


def _make_result(
    *,
    schema_version=SCHEMA_VERSION,
    last_candle_ts=_NOW - timedelta(minutes=1),
    kafka_offset=100,
    state_hash_override=None,
) -> CheckpointLoadResult:
    state = _make_state_with_one()
    h = state_hash_override or compute_state_hash(state)
    cs = CommitStateRow(
        mode="prod",
        last_candle_ts=last_candle_ts,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=kafka_offset,
        state_hash=h,
        schema_version=schema_version,
    )
    return CheckpointLoadResult(commit_state=cs, anchor_state=state)


@pytest.mark.unit
def test_check_schema_version_passes_when_match():
    from pnl_consumer.cold_start import check_schema_version

    assert check_schema_version(_make_result()) is None


@pytest.mark.unit
def test_check_schema_version_fails_when_mismatch():
    from pnl_consumer.cold_start import FallbackReason, check_schema_version

    bad = _make_result(schema_version=SCHEMA_VERSION + 1)
    reason = check_schema_version(bad)
    assert reason == FallbackReason.SCHEMA_VERSION_MISMATCH


@pytest.mark.unit
def test_check_freshness_passes_within_window():
    from pnl_consumer.cold_start import check_freshness

    result = _make_result(last_candle_ts=_NOW - timedelta(hours=1))
    assert check_freshness(result, now=_NOW, max_age_seconds=86400) is None


@pytest.mark.unit
def test_check_freshness_fails_outside_window():
    from pnl_consumer.cold_start import FallbackReason, check_freshness

    result = _make_result(last_candle_ts=_NOW - timedelta(hours=25))
    reason = check_freshness(result, now=_NOW, max_age_seconds=86400)
    assert reason == FallbackReason.STALE


@pytest.mark.unit
def test_check_offset_match_passes_when_supabase_is_one_behind_committed():
    """Kafka offset+1 is next-to-read; checkpoint stores the offset of last processed.

    Match condition: checkpoint.kafka_offset == kafka_committed_offset - 1.
    """
    from pnl_consumer.cold_start import check_offset_match

    result = _make_result(kafka_offset=100)
    assert check_offset_match(result, kafka_committed_offset=101) is None


@pytest.mark.unit
def test_check_offset_match_fails_when_supabase_lags():
    from pnl_consumer.cold_start import FallbackReason, check_offset_match

    result = _make_result(kafka_offset=100)
    reason = check_offset_match(result, kafka_committed_offset=105)
    assert reason == FallbackReason.OFFSET_LAG


@pytest.mark.unit
def test_check_offset_match_fails_when_supabase_ahead():
    from pnl_consumer.cold_start import FallbackReason, check_offset_match

    result = _make_result(kafka_offset=100)
    reason = check_offset_match(result, kafka_committed_offset=99)
    assert reason == FallbackReason.OFFSET_LAG


@pytest.mark.unit
def test_check_hash_passes_on_clean_state():
    from pnl_consumer.cold_start import check_hash

    assert check_hash(_make_result()) is None


@pytest.mark.unit
def test_check_hash_fails_on_tampered_state():
    from pnl_consumer.cold_start import FallbackReason, check_hash

    result = _make_result(state_hash_override="0" * 64)
    reason = check_hash(result)
    assert reason == FallbackReason.HASH_MISMATCH


@pytest.mark.unit
def test_check_non_empty_strategy_set_fails_when_empty():
    from pnl_consumer.cold_start import FallbackReason, check_non_empty

    empty = CheckpointLoadResult(
        commit_state=_make_result().commit_state,
        anchor_state=AnchorState(),
    )
    reason = check_non_empty(empty)
    assert reason == FallbackReason.NO_STRATEGIES


@pytest.mark.unit
def test_real_trade_check_passes_with_real_revision_guard():
    from pnl_consumer.cold_start import check_real_trade_revision_guards

    result = _make_result()
    assert check_real_trade_revision_guards(result) is None


@pytest.mark.unit
def test_real_trade_check_fails_when_bar_ts_is_min():
    from pnl_consumer.cold_start import (
        FallbackReason,
        check_real_trade_revision_guards,
    )

    state = AnchorState()
    state.set("strat_a", AnchorRecord())  # all defaults — bar_ts == datetime.min
    cs = CommitStateRow(
        mode="real_trade",
        last_candle_ts=_NOW - timedelta(minutes=1),
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=100,
        state_hash=compute_state_hash(state),
        schema_version=SCHEMA_VERSION,
    )
    result = CheckpointLoadResult(commit_state=cs, anchor_state=state)
    reason = check_real_trade_revision_guards(result)
    assert reason == FallbackReason.REAL_TRADE_GUARD_UNINITIALIZED
