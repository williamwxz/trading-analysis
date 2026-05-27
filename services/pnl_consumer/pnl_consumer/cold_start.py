"""Cold-start orchestration for pnl_consumer.

On startup, attempts to restore AnchorState from streaming.pnl_checkpoint.
Runs cheap sanity checks; on any failure, falls back to the existing
_bootstrap_state / _bootstrap_bt_state code in pnl_consumer.py.

This module is the only place that ties together Supabase reads, Kafka offset
queries, ClickHouse invariant checks, and bootstrap fallback. Everything else
(checkpoint_store, postgres_client) is pure I/O / serialization.
"""

from datetime import UTC, datetime
from enum import Enum

from libs.computation.checkpoint_store import (
    SCHEMA_VERSION,
    CheckpointLoadResult,
    compute_state_hash,
)

_DATETIME_MIN_UTC = datetime.min.replace(tzinfo=UTC)


class FallbackReason(Enum):
    """Why cold-start fell back to bootstrap. Logged on every fallback."""

    NO_CHECKPOINT = "no_checkpoint"
    SCHEMA_VERSION_MISMATCH = "schema_version_mismatch"
    STALE = "stale"
    OFFSET_LAG = "offset_lag"
    HASH_MISMATCH = "hash_mismatch"
    NO_STRATEGIES = "no_strategies"
    REAL_TRADE_GUARD_UNINITIALIZED = "real_trade_guard_uninitialized"
    INVARIANT_MISMATCH = "invariant_mismatch"
    READ_DISABLED = "read_disabled"
    CHECKPOINT_READ_ERROR = "checkpoint_read_error"


def check_schema_version(result: CheckpointLoadResult) -> FallbackReason | None:
    if result.commit_state.schema_version != SCHEMA_VERSION:
        return FallbackReason.SCHEMA_VERSION_MISMATCH
    return None


def check_freshness(
    result: CheckpointLoadResult,
    *,
    now: datetime,
    max_age_seconds: int,
) -> FallbackReason | None:
    age = (now - result.commit_state.last_candle_ts).total_seconds()
    if age > max_age_seconds:
        return FallbackReason.STALE
    return None


def check_offset_match(
    result: CheckpointLoadResult,
    *,
    kafka_committed_offset: int,
) -> FallbackReason | None:
    """checkpoint.kafka_offset is the offset of the *last processed* message.
    Kafka's committed offset is the *next-to-read*, i.e. last_processed + 1.
    They match iff checkpoint.kafka_offset == kafka_committed_offset - 1.
    """
    if result.commit_state.kafka_offset != kafka_committed_offset - 1:
        return FallbackReason.OFFSET_LAG
    return None


def check_hash(result: CheckpointLoadResult) -> FallbackReason | None:
    recomputed = compute_state_hash(result.anchor_state)
    if recomputed != result.commit_state.state_hash:
        return FallbackReason.HASH_MISMATCH
    return None


def check_non_empty(result: CheckpointLoadResult) -> FallbackReason | None:
    if len(result.anchor_state) == 0:
        return FallbackReason.NO_STRATEGIES
    return None


def check_real_trade_revision_guards(
    result: CheckpointLoadResult,
) -> FallbackReason | None:
    """For real-trade mode: every record must have populated bar_ts and revision_ts.

    `_DATETIME_MIN_UTC` means the revision guard was never initialized; restoring
    from such a checkpoint would let any revision through, defeating the guard.
    """
    for key in result.anchor_state.keys():
        rec = result.anchor_state.get(key)
        # Normalize: rec.bar_ts may be naive (datetime.min from defaults) or aware.
        bar_ts = (
            rec.bar_ts.replace(tzinfo=UTC) if rec.bar_ts.tzinfo is None else rec.bar_ts
        )
        revision_ts = (
            rec.revision_ts.replace(tzinfo=UTC)
            if rec.revision_ts.tzinfo is None
            else rec.revision_ts
        )
        if bar_ts <= _DATETIME_MIN_UTC or revision_ts <= _DATETIME_MIN_UTC:
            return FallbackReason.REAL_TRADE_GUARD_UNINITIALIZED
    return None
