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

from libs.clickhouse_client import query_rows
from libs.computation.anchor_state import AnchorState
from libs.computation.checkpoint_store import (
    SCHEMA_VERSION,
    CheckpointLoadResult,
    _strip_tz,
    compute_state_hash,
)

logger = logging.getLogger(__name__)

_DATETIME_MIN_UTC = datetime.min.replace(tzinfo=UTC)
_POSITION_TOLERANCE = 1e-9


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
        try:
            inv_reason = invariant_check_fn(mode=mode, result=result)
        except Exception as exc:
            logger.warning(
                "cold-start: invariant check raised %s; falling back. mode=%s",
                type(exc).__name__,
                mode,
            )
            return bootstrap_fn(), FallbackReason.INVARIANT_MISMATCH
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


def clickhouse_invariant_check(
    *,
    mode: str,
    result: CheckpointLoadResult,
    ch_client,
) -> FallbackReason | None:
    """Verify checkpoint.position agrees with strategy_output_history_*.

    Single ClickHouse query per mode. Compares per-strategy_instance_id.

    Prod/bt: latest revision in [strategy_output_history_v2 | _bt_v2] as of
    result.commit_state.last_candle_ts.

    Real-trade: the *specific* (ts, revision_ts) pair stored on each checkpoint
    record. If that exact revision is missing, fall back.

    Returns None on agreement, FallbackReason.INVARIANT_MISMATCH otherwise.
    """
    history_table = {
        "prod": "analytics.strategy_output_history_v2",
        "bt": "analytics.strategy_output_history_bt_v2",
        "real_trade": "analytics.strategy_output_history_v2",
    }[mode]

    expected_by_iid: dict[str, float] = {}
    for key in result.anchor_state.keys():
        rec = result.anchor_state.get(key)
        if rec.strategy_instance_id:
            expected_by_iid[rec.strategy_instance_id] = rec.position

    if not expected_by_iid:
        # No instance_ids to check — caller should have caught NO_STRATEGIES already.
        return None

    if mode == "real_trade":
        # Verify the exact (ts, revision_ts) revisions referenced by checkpoint still
        # exist in history with matching position. Use ClickHouse tuple-IN form.
        tuples = []
        for key in result.anchor_state.keys():
            rec = result.anchor_state.get(key)
            if not rec.strategy_instance_id:
                continue
            tuples.append(
                (
                    rec.strategy_instance_id,
                    _strip_tz(rec.bar_ts).strftime('%Y-%m-%d %H:%M:%S'),
                    _strip_tz(rec.revision_ts).strftime('%Y-%m-%d %H:%M:%S'),
                )
            )
        if not tuples:
            return None
        tuples_sql = ", ".join(
            f"('{iid}', toDateTime('{ts}'), toDateTime('{rts}'))"
            for iid, ts, rts in tuples
        )
        sql = f"""
            SELECT
                strategy_instance_id,
                JSONExtractFloat(row_json, 'position') AS position
            FROM {history_table}
            WHERE (strategy_instance_id, ts, revision_ts) IN ({tuples_sql})
        """
    else:
        # Latest revision per strategy_instance_id, asof last_candle_ts.
        as_of = _strip_tz(result.commit_state.last_candle_ts).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        iid_list = ",".join(f"'{i}'" for i in expected_by_iid.keys())
        sql = f"""
            SELECT
                strategy_instance_id,
                JSONExtractFloat(argMin(row_json, revision_ts), 'position') AS position
            FROM {history_table}
            WHERE strategy_instance_id IN ({iid_list})
              AND ts <= toDateTime('{as_of}')
            GROUP BY strategy_instance_id
        """

    rows = query_rows(sql, client=ch_client)
    actual_by_iid = {r[0]: float(r[1]) for r in rows}

    for iid, expected_position in expected_by_iid.items():
        actual = actual_by_iid.get(iid)
        if actual is None or abs(actual - expected_position) > _POSITION_TOLERANCE:
            logger.warning(
                "invariant mismatch: iid=%s expected_position=%s actual_position=%s",
                iid,
                expected_position,
                actual,
            )
            return FallbackReason.INVARIANT_MISMATCH
    return None
