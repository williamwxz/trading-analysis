"""Cold-start orchestration for pnl_consumer.

On startup, attempts to restore AnchorState from streaming.pnl_checkpoint.
Runs cheap sanity checks; on any failure, falls back to the existing
_bootstrap_state / _bootstrap_bt_state code in pnl_consumer.py.

This module is the only place that ties together Supabase reads, Kafka offset
queries, ClickHouse invariant checks, and bootstrap fallback. Everything else
(checkpoint_store, postgres_client) is pure I/O / serialization.
"""

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from enum import Enum

from libs.computation.anchor_state import AnchorState
from libs.computation.checkpoint_store import (
    SCHEMA_VERSION,
    CheckpointLoadResult,
    compute_state_hash,
)

logger = logging.getLogger(__name__)

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


def load_or_bootstrap(
    *,
    mode: str,
    pg_client,
    kafka_committed_offset: int,
    now: datetime,
    max_age_seconds: int,
    invariant_check_enabled: bool,
    read_checkpoint_fn: Callable | None = None,
    bootstrap_fn: Callable[[], AnchorState] | None = None,
    invariant_check_fn: Callable | None = None,
) -> tuple[AnchorState, FallbackReason | None]:
    """Try to restore AnchorState from Supabase; fall back to bootstrap on any failure.

    Returns (state, reason). reason is None on success, the FallbackReason enum
    value otherwise. Caller logs the reason and proceeds either way.

    Function injection points (read_checkpoint_fn, bootstrap_fn, invariant_check_fn)
    exist for testability. In production, defaults are wired in pnl_consumer.run().
    """
    if read_checkpoint_fn is None or bootstrap_fn is None:
        raise ValueError(
            "load_or_bootstrap requires read_checkpoint_fn and bootstrap_fn "
            "(wired in pnl_consumer.run); they are injected for testing."
        )

    # 1. Read checkpoint.
    try:
        result = read_checkpoint_fn(mode=mode, client=pg_client)
    except Exception as exc:
        logger.warning(
            "cold-start: checkpoint read raised %s; falling back to bootstrap. mode=%s",
            type(exc).__name__,
            mode,
        )
        return bootstrap_fn(), FallbackReason.CHECKPOINT_READ_ERROR

    if result is None:
        logger.info("cold-start: no checkpoint row; falling back. mode=%s", mode)
        return bootstrap_fn(), FallbackReason.NO_CHECKPOINT

    # 2. Sanity checks (cheap, no ClickHouse).
    checks = [
        check_schema_version(result),
        check_non_empty(result),
        check_freshness(result, now=now, max_age_seconds=max_age_seconds),
        check_offset_match(result, kafka_committed_offset=kafka_committed_offset),
        check_hash(result),
    ]
    if mode == "real_trade":
        checks.append(check_real_trade_revision_guards(result))

    for reason in checks:
        if reason is not None:
            logger.warning(
                "cold-start: sanity check failed, falling back. mode=%s reason=%s",
                mode,
                reason.value,
            )
            return bootstrap_fn(), reason

    # 3. Optional ClickHouse invariant cross-check.
    if invariant_check_enabled and invariant_check_fn is not None:
        inv_reason = invariant_check_fn(mode=mode, result=result)
        if inv_reason is not None:
            logger.warning(
                "cold-start: invariant check failed, falling back. mode=%s reason=%s",
                mode,
                inv_reason.value,
            )
            return bootstrap_fn(), inv_reason

    logger.info(
        "cold-start: checkpoint restored. mode=%s strategies=%d offset=%d"
        " last_candle_ts=%s",
        mode,
        len(result.anchor_state),
        result.commit_state.kafka_offset,
        result.commit_state.last_candle_ts,
    )
    return result.anchor_state, None
