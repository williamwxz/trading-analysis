"""Supabase Postgres checkpoint store for pnl_consumer AnchorState.

Reads and writes streaming.pnl_checkpoint and streaming.pnl_commit_state.

SCHEMA_VERSION bumps on incompatible AnchorRecord changes (field added/removed,
type change, semantics change). Cold-start sanity check rejects a checkpoint
whose stored schema_version != SCHEMA_VERSION; the consumer falls back to the
existing 48h bootstrap and writes a fresh checkpoint at the new version.
"""

import hashlib
import json
from typing import Any

from libs.computation.anchor_state import AnchorState

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
