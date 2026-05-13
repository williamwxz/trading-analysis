"""Cold-start bootstrap queries for the PnL streaming consumer.

Provides the data needed to seed AnchorState before the live streaming loop starts:
  1. fetch_bootstrap_seeds()      — per-strategy anchor state at start_ts
  2. fetch_walk_rows()            — stored PnL rows in [start_ts, reference_ts) for verification
  3. walk_and_verify()            — replay walk rows, update AnchorState, crash on deviation > tolerance

Supports all three modes (prod, bt, real_trade) via the pnl_table / history_table parameters.

Position resolution differs by mode:
  - prod/bt:        latest bar with ts <= cutoff, argMin(row_json, revision_ts) — first revision wins
  - real_trade:     latest revision with revision_ts <= cutoff per strategy_instance_id

Price comes from the stored price column in the PnL table — this ensures the verification
chain uses the same prices that Dagster/consumer used when writing the rows.
"""

import json
import logging
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from libs.clickhouse_client import query_dicts

_log = logging.getLogger(__name__)

_DATETIME_MIN = datetime.min


@dataclass
class BootstrapSeed:
    """Anchor state for one strategy at start_ts."""
    strategy_table_name: str
    strategy_instance_id: str
    pnl: float
    price: float
    position: float
    # bar_ts and revision_ts are used by real_trade's AnchorState revision guard.
    # For prod/bt these are unused — left at datetime.min so any real revision passes.
    bar_ts: datetime = field(default_factory=lambda: _DATETIME_MIN)
    revision_ts: datetime = field(default_factory=lambda: _DATETIME_MIN)


@dataclass
class WalkRow:
    """One stored PnL row used during the [start_ts, reference_ts) verification walk."""
    strategy_table_name: str
    strategy_instance_id: str
    ts: datetime
    cumulative_pnl: float
    price: float
    position: float
    # real_trade only: the bar_ts and revision_ts active at this minute.
    bar_ts: datetime = field(default_factory=lambda: _DATETIME_MIN)
    revision_ts: datetime = field(default_factory=lambda: _DATETIME_MIN)


def fetch_bootstrap_seeds(
    pnl_table: str,
    history_table: str,
    start_ts: datetime,
    real_trade: bool = False,
) -> list[BootstrapSeed]:
    """Return per-strategy anchor state to seed AnchorState at start_ts.

    For each strategy_instance_id:
      - cumulative_pnl: latest pnl_table row with ts < start_ts (0.0 if none)
      - price: from futures_price_1min at that same minute (0.0 if none)
      - position: from history_table
          prod/bt:     latest bar with ts <= start_ts, first revision (argMin)
          real_trade:  latest revision with revision_ts <= start_ts
      - bar_ts / revision_ts: populated for real_trade (used by revision guard); datetime.min for prod/bt

    Args:
        pnl_table:     e.g. 'analytics.strategy_pnl_1min_prod_v2'
        history_table: e.g. 'analytics.strategy_output_history_v2'
        start_ts:      reference_ts - 3 days
        real_trade:    True to use revision_ts-based position resolution
    """
    start_str = start_ts.strftime("%Y-%m-%d %H:%M:%S")

    # ── PnL + price baseline ──────────────────────────────────────────────────
    # Keyed by strategy_instance_id — each instance is an independent tracking unit.
    pnl_sql = f"""\
SELECT
    strategy_table_name,
    strategy_instance_id,
    cumulative_pnl,
    price
FROM {pnl_table} FINAL
WHERE ts < '{start_str}'
ORDER BY strategy_instance_id, ts DESC
LIMIT 1 BY strategy_instance_id
"""
    pnl_seeds: dict[str, dict] = {}
    for row in query_dicts(pnl_sql):
        pnl_seeds[row["strategy_instance_id"]] = {
            "strategy_table_name": row["strategy_table_name"],
            "pnl": float(row["cumulative_pnl"] or 0.0),
            "price": float(row["price"]),
        }

    # ── Position ──────────────────────────────────────────────────────────────
    if real_trade:
        # Latest revision whose revision_ts <= start_ts per strategy_instance_id.
        pos_sql = f"""\
SELECT
    strategy_table_name,
    strategy_instance_id,
    ts AS bar_ts,
    max(revision_ts) AS max_revision_ts,
    argMax(row_json, revision_ts) AS row_json
FROM {history_table}
WHERE revision_ts <= '{start_str}'
GROUP BY strategy_table_name, strategy_instance_id, ts
ORDER BY strategy_instance_id, ts DESC
LIMIT 1 BY strategy_instance_id
"""
        pos_rows: dict[str, dict] = {}
        for row in query_dicts(pos_sql):
            rj = json.loads(row["row_json"])
            pos_rows[row["strategy_instance_id"]] = {
                "strategy_table_name": row["strategy_table_name"],
                "position": float(rj.get("position", 0.0)),
                "bar_ts": row["bar_ts"],
                "revision_ts": row["max_revision_ts"],
            }
    else:
        # Prod/bt: latest bar ts <= start_ts, first revision only, per strategy_instance_id.
        pos_sql = f"""\
SELECT
    strategy_table_name,
    strategy_instance_id,
    argMin(row_json, revision_ts) AS row_json,
    max(ts) AS max_ts
FROM {history_table}
WHERE ts <= '{start_str}'
GROUP BY strategy_table_name, strategy_instance_id
ORDER BY strategy_instance_id, max_ts DESC
LIMIT 1 BY strategy_instance_id
"""
        pos_rows = {}
        for row in query_dicts(pos_sql):
            rj = json.loads(row["row_json"])
            pos_rows[row["strategy_instance_id"]] = {
                "strategy_table_name": row["strategy_table_name"],
                "position": float(rj.get("position", 0.0)),
                "bar_ts": _DATETIME_MIN,
                "revision_ts": _DATETIME_MIN,
            }

    # ── Merge — keyed by strategy_instance_id ─────────────────────────────────
    all_siids = set(pnl_seeds) | set(pos_rows)
    seeds: list[BootstrapSeed] = []
    for siid in all_siids:
        pnl_info = pnl_seeds.get(siid, {})
        pos_info = pos_rows.get(siid, {})
        stn = pnl_info.get("strategy_table_name") or pos_info.get("strategy_table_name", "")
        seeds.append(
            BootstrapSeed(
                strategy_table_name=stn,
                strategy_instance_id=siid,
                pnl=pnl_info.get("pnl", 0.0),
                price=pnl_info.get("price", 0.0),
                position=pos_info.get("position", 0.0),
                bar_ts=pos_info.get("bar_ts", _DATETIME_MIN),
                revision_ts=pos_info.get("revision_ts", _DATETIME_MIN),
            )
        )

    # ── Completeness check ────────────────────────────────────────────────────
    # Every strategy_instance_id with history before start_ts must appear in seeds.
    # Brand-new instances (first bar after start_ts) are excluded — they'll be
    # lazy-seeded on first appearance in the live loop.
    expected_sql = f"""\
SELECT count(DISTINCT strategy_instance_id) AS cnt
FROM {history_table}
WHERE ts < '{start_str}'
"""
    expected_count = int((query_dicts(expected_sql) or [{"cnt": 0}])[0]["cnt"])
    seed_count = len(seeds)
    if seed_count < expected_count:
        raise RuntimeError(
            f"Bootstrap completeness check failed: {seed_count} seeds < "
            f"{expected_count} strategy_instance_ids with history before {start_str} in {history_table}. "
            "Some strategies have no pnl rows in the walk window — cannot safely resume."
        )

    return seeds


def fetch_walk_rows(
    pnl_table: str,
    history_table: str,
    start_ts: datetime,
    reference_ts: datetime,
    real_trade: bool = False,
) -> list[WalkRow]:
    """Return stored PnL rows in [start_ts, reference_ts) ordered chronologically.

    Used during the bootstrap walk to verify stored cumulative_pnl values against:
        pnl = prev_pnl + position * (price - prev_price) / prev_price

    Price comes from the stored price column in the pnl table — same price used when the
    row was written — so verification uses an identical chain to what Dagster/consumer computed.
    reference_ts is EXCLUDED — the consumer will process that candle live from Kafka.

    Position resolution:
      prod/bt:     latest bar with ts <= row.ts, first revision only
      real_trade:  latest revision with revision_ts <= row.ts per strategy_instance_id

    For real_trade, WalkRow.bar_ts and WalkRow.revision_ts are also populated so the
    caller can advance AnchorState's revision guard fields after each walked row.

    Args:
        pnl_table:     e.g. 'analytics.strategy_pnl_1min_prod_v2'
        history_table: e.g. 'analytics.strategy_output_history_v2'
        start_ts:      start of walk window (inclusive)
        reference_ts:  end of walk window (exclusive)
        real_trade:    True to use revision_ts-based position resolution
    """
    start_str = start_ts.strftime("%Y-%m-%d %H:%M:%S")
    ref_str = reference_ts.strftime("%Y-%m-%d %H:%M:%S")

    # ── Fetch stored PnL rows — use stored price column for chain consistency ──
    pnl_sql = f"""\
SELECT
    strategy_table_name,
    strategy_instance_id,
    ts,
    cumulative_pnl,
    price
FROM {pnl_table} FINAL
WHERE ts >= '{start_str}'
  AND ts < '{ref_str}'
ORDER BY strategy_table_name, ts ASC
"""
    pnl_rows = query_dicts(pnl_sql)
    if not pnl_rows:
        return []

    # ── Fetch history bars/revisions for position resolution ──────────────────
    if real_trade:
        # All revisions with revision_ts in [start_ts, reference_ts) — sorted ASC.
        # We resolve per walk row in Python using bisect.
        # Group by (siid, revision_ts) taking the highest bar_ts per revision_ts.
        # Multiple bars can share the same revision_ts (batch revisions); taking the
        # latest bar_ts matches the AnchorState revision guard used in the live loop.
        hist_sql = f"""\
SELECT
    strategy_instance_id,
    revision_ts,
    argMax(ts, ts)         AS bar_ts,
    argMax(row_json, ts)   AS row_json
FROM {history_table}
WHERE revision_ts >= '{start_str}'::DateTime - INTERVAL 2 DAY
  AND revision_ts < '{ref_str}'
GROUP BY strategy_instance_id, revision_ts
ORDER BY strategy_instance_id, revision_ts
"""
        # Build per-strategy_instance_id sorted list of (revision_ts, bar_ts, position).
        rt_revisions: dict[str, list[tuple[datetime, datetime, float]]] = {}
        for row in query_dicts(hist_sql):
            siid = row["strategy_instance_id"]
            rj = json.loads(row["row_json"])
            entry = (row["revision_ts"], row["bar_ts"], float(rj.get("position", 0.0)))
            rt_revisions.setdefault(siid, []).append(entry)

        def _get_rt_position(
            siid: str, row_ts: datetime
        ) -> tuple[float, datetime, datetime]:
            revs = rt_revisions.get(siid)
            if not revs:
                return 0.0, _DATETIME_MIN, _DATETIME_MIN
            rev_ts_keys = [r[0] for r in revs]
            idx = bisect_right(rev_ts_keys, row_ts) - 1
            if idx < 0:
                return 0.0, _DATETIME_MIN, _DATETIME_MIN
            _, bar_ts, position = revs[idx]
            return position, bar_ts, rev_ts_keys[idx]

    else:
        # Prod/bt: bars in window, first revision per (strategy_table_name, bar_ts).
        hist_sql = f"""\
SELECT
    strategy_table_name,
    ts AS bar_ts,
    argMin(row_json, revision_ts) AS row_json
FROM {history_table}
WHERE ts >= '{start_str}'::DateTime - INTERVAL 2 DAY
  AND ts < '{ref_str}'
GROUP BY strategy_table_name, ts
ORDER BY strategy_table_name, ts
"""
        # Key on closing_ts (= bar_ts + cfg_bar_sec) so that a bar's position
        # only becomes active at bar close, matching iter_compute_prod_pnl.
        bar_positions: dict[str, list[tuple[datetime, float]]] = {}
        for row in query_dicts(hist_sql):
            stn = row["strategy_table_name"]
            rj = json.loads(row["row_json"])
            bar_sec = int(rj.get("cfg_bar_sec", 300))
            closing_ts = row["bar_ts"] + timedelta(seconds=bar_sec)
            bar_positions.setdefault(stn, []).append(
                (closing_ts, float(rj.get("position", 0.0)))
            )

        def _get_prod_position(stn: str, row_ts: datetime) -> float:
            bars = bar_positions.get(stn)
            if not bars:
                return 0.0
            ts_keys = [b[0] for b in bars]
            idx = bisect_right(ts_keys, row_ts) - 1
            return bars[idx][1] if idx >= 0 else 0.0

    # ── Build WalkRow list ────────────────────────────────────────────────────
    result: list[WalkRow] = []
    for row in pnl_rows:
        stn = row["strategy_table_name"]
        siid = row["strategy_instance_id"]
        row_ts = row["ts"]

        if real_trade:
            position, bar_ts, revision_ts = _get_rt_position(siid, row_ts)
        else:
            position = _get_prod_position(stn, row_ts)
            bar_ts = _DATETIME_MIN
            revision_ts = _DATETIME_MIN

        result.append(
            WalkRow(
                strategy_table_name=stn,
                strategy_instance_id=siid,
                ts=row_ts,
                cumulative_pnl=float(row["cumulative_pnl"] or 0.0),
                price=float(row["price"]),
                position=position,
                bar_ts=bar_ts,
                revision_ts=revision_ts,
            )
        )
    return result
