"""Supabase Postgres checkpoint store for pnl_consumer AnchorState.

Reads and writes streaming.pnl_checkpoint and streaming.pnl_commit_state.

SCHEMA_VERSION bumps on incompatible AnchorRecord changes (field added/removed,
type change, semantics change). Cold-start sanity check rejects a checkpoint
whose stored schema_version != SCHEMA_VERSION; the consumer falls back to the
existing 48h bootstrap and writes a fresh checkpoint at the new version.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg import Connection

from libs.computation.anchor_state import AnchorRecord, AnchorState

# Bump on any incompatible change to AnchorRecord fields or canonical encoding.
SCHEMA_VERSION = 1

# Float precision for hash + serialization. Below this is treated as noise.
_FLOAT_DECIMALS = 12


def _canonical_record(rec) -> dict[str, Any]:
    """One AnchorRecord → dict in canonical form (sorted keys, rounded floats)."""
    return {
        "pnl": round(rec.pnl, _FLOAT_DECIMALS),
        "price": round(rec.price, _FLOAT_DECIMALS),
        "position": round(rec.position, _FLOAT_DECIMALS),
        "bar_ts": rec.bar_ts.isoformat(),
        "revision_ts": rec.revision_ts.isoformat(),
        "strategy_id": int(rec.strategy_id),
        "strategy_name": rec.strategy_name,
        "underlying": rec.underlying,
        "config_timeframe": rec.config_timeframe,
        "weighting": round(rec.weighting, _FLOAT_DECIMALS),
        "strategy_instance_id": rec.strategy_instance_id,
        "final_signal": round(rec.final_signal, _FLOAT_DECIMALS),
        "benchmark": round(rec.benchmark, _FLOAT_DECIMALS),
    }


def compute_state_hash(state: AnchorState) -> str:
    """sha256 over the canonical, key-sorted encoding of all records in `state`.

    Hash is over state only — metadata fields (updated_at) are excluded.
    """
    canonical = [
        {"strategy_table_name": k, **_canonical_record(state.get(k))}
        for k in sorted(state.keys())
    ]
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


_UPSERT_CHECKPOINT_SQL = """
INSERT INTO streaming.pnl_checkpoint (
    mode, strategy_table_name,
    pnl, price, position,
    bar_ts, revision_ts,
    strategy_id, strategy_name, underlying, config_timeframe,
    weighting, strategy_instance_id, final_signal, benchmark,
    updated_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (mode, strategy_table_name) DO UPDATE SET
    pnl = EXCLUDED.pnl,
    price = EXCLUDED.price,
    position = EXCLUDED.position,
    bar_ts = EXCLUDED.bar_ts,
    revision_ts = EXCLUDED.revision_ts,
    strategy_id = EXCLUDED.strategy_id,
    strategy_name = EXCLUDED.strategy_name,
    underlying = EXCLUDED.underlying,
    config_timeframe = EXCLUDED.config_timeframe,
    weighting = EXCLUDED.weighting,
    strategy_instance_id = EXCLUDED.strategy_instance_id,
    final_signal = EXCLUDED.final_signal,
    benchmark = EXCLUDED.benchmark,
    updated_at = NOW();
"""

_UPSERT_COMMIT_STATE_SQL = """
INSERT INTO streaming.pnl_commit_state (
    mode, last_candle_ts, kafka_topic, kafka_partition,
    kafka_offset, state_hash, schema_version, updated_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (mode) DO UPDATE SET
    last_candle_ts  = EXCLUDED.last_candle_ts,
    kafka_topic     = EXCLUDED.kafka_topic,
    kafka_partition = EXCLUDED.kafka_partition,
    kafka_offset    = EXCLUDED.kafka_offset,
    state_hash      = EXCLUDED.state_hash,
    schema_version  = EXCLUDED.schema_version,
    updated_at      = NOW();
"""


def _checkpoint_row_params(mode: str, strategy_table_name: str, rec) -> tuple:
    return (
        mode,
        strategy_table_name,
        rec.pnl,
        rec.price,
        rec.position,
        rec.bar_ts,
        rec.revision_ts,
        int(rec.strategy_id),
        rec.strategy_name,
        rec.underlying,
        rec.config_timeframe,
        rec.weighting,
        rec.strategy_instance_id,
        rec.final_signal,
        rec.benchmark,
    )


def write_checkpoint(
    *,
    mode: str,
    anchor_state: AnchorState,
    kafka_topic: str,
    kafka_partition: int,
    kafka_offset: int,
    last_candle_ts: datetime,
    client: Connection,
) -> None:
    """Atomic UPSERT of checkpoint + commit_state for one mode in a single transaction.

    On any database error, rolls back and re-raises. Caller wraps in try/except
    for best-effort behavior — checkpoint_store does not silently swallow errors.

    The full AnchorState is written every call (UPSERT semantics), so a single
    successful call after an outage fully restores the checkpoint.
    """
    state_hash = compute_state_hash(anchor_state)
    try:
        with client.cursor() as cur:
            keys = sorted(anchor_state.keys())
            if keys:
                params_list: list[tuple] = [
                    _checkpoint_row_params(mode, k, anchor_state.get(k)) for k in keys
                ]
                cur.executemany(_UPSERT_CHECKPOINT_SQL, params_list)
            cur.execute(
                _UPSERT_COMMIT_STATE_SQL,
                (
                    mode,
                    last_candle_ts,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset,
                    state_hash,
                    SCHEMA_VERSION,
                ),
            )
        client.commit()
    except Exception:
        client.rollback()
        raise


@dataclass(frozen=True)
class CommitStateRow:
    mode: str
    last_candle_ts: datetime
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    state_hash: str
    schema_version: int


@dataclass(frozen=True)
class CheckpointLoadResult:
    commit_state: CommitStateRow
    anchor_state: AnchorState


_SELECT_COMMIT_STATE = """
SELECT mode, last_candle_ts, kafka_topic, kafka_partition,
       kafka_offset, state_hash, schema_version
FROM streaming.pnl_commit_state
WHERE mode = %s
"""

_SELECT_CHECKPOINT_ROWS = """
SELECT strategy_table_name, pnl, price, position,
       bar_ts, revision_ts,
       strategy_id, strategy_name, underlying, config_timeframe,
       weighting, strategy_instance_id, final_signal, benchmark
FROM streaming.pnl_checkpoint
WHERE mode = %s
"""


def read_checkpoint(*, mode: str, client: Connection) -> "CheckpointLoadResult | None":
    """Return the persisted checkpoint for `mode`, or None if no commit_state row.

    Reads both tables in the same connection but separate cursors. The two reads are
    not in a single transaction — the writer guarantees consistency via its own
    transaction; readers see whatever the last successful write produced.
    """
    with client.cursor() as cur:
        cur.execute(_SELECT_COMMIT_STATE, (mode,))
        commit_rows = cur.fetchall()
    if not commit_rows:
        return None
    row = commit_rows[0]
    commit_state = CommitStateRow(
        mode=row[0],
        last_candle_ts=row[1],
        kafka_topic=row[2],
        kafka_partition=row[3],
        kafka_offset=row[4],
        state_hash=row[5],
        schema_version=row[6],
    )

    with client.cursor() as cur:
        cur.execute(_SELECT_CHECKPOINT_ROWS, (mode,))
        checkpoint_rows = cur.fetchall()

    anchor_state = AnchorState()
    for r in checkpoint_rows:
        anchor_state.set(
            r[0],
            AnchorRecord(
                pnl=r[1],
                price=r[2],
                position=r[3],
                bar_ts=r[4],
                revision_ts=r[5],
                strategy_id=r[6],
                strategy_name=r[7],
                underlying=r[8],
                config_timeframe=r[9],
                weighting=r[10],
                strategy_instance_id=r[11],
                final_signal=r[12],
                benchmark=r[13],
            ),
        )
    return CheckpointLoadResult(commit_state=commit_state, anchor_state=anchor_state)
