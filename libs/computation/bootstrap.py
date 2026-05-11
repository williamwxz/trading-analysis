"""Cold-start bootstrap queries for the PnL streaming consumer.

Provides the data needed to seed AnchorState before the live streaming loop starts:
  1. fetch_bootstrap_seeds() — per-strategy anchor state at start_ts
  2. fetch_walk_rows()       — stored PnL rows in [start_ts, reference_ts) for verification

These are called once at startup (not per-candle), so they query all underlyings at once.
Position always comes from strategy_output_history_*; price from futures_price_1min.
"""

import json
from dataclasses import dataclass
from datetime import datetime

from trading_dagster.utils.clickhouse_client import query_dicts

_TF_MINUTES_EXPR_NO_ALIAS = """\
multiIf(
        config_timeframe = '1m',  1,
        config_timeframe = '3m',  3,
        config_timeframe = '5m',  5,
        config_timeframe = '15m', 15,
        config_timeframe = '30m', 30,
        config_timeframe = '1h',  60,
        config_timeframe = '4h',  240,
        config_timeframe = '1d',  1440,
        5
    )"""


@dataclass
class BootstrapSeed:
    """Anchor state for one strategy at start_ts."""
    strategy_table_name: str
    strategy_instance_id: str
    pnl: float
    price: float
    position: float


@dataclass
class WalkRow:
    """One stored PnL row used during the [start_ts, reference_ts) verification walk."""
    strategy_table_name: str
    strategy_instance_id: str
    ts: datetime
    cumulative_pnl: float
    price: float
    position: float


def fetch_bootstrap_seeds(
    pnl_table: str,
    history_table: str,
    start_ts: datetime,
) -> list[BootstrapSeed]:
    """Return per-strategy anchor state to seed AnchorState at start_ts.

    For each strategy_instance_id:
      - cumulative_pnl: from the latest pnl_table row with ts < start_ts
        (0.0 if no prior row)
      - price: from futures_price_1min at the same minute as that pnl row
        (0.0 if no prior row — signals "no price yet")
      - position: from the latest bar in history_table with ts <= start_ts,
        first revision only (argMin by revision_ts)
        (0.0 if no bar has arrived yet)

    Args:
        pnl_table: fully-qualified table name, e.g. 'analytics.strategy_pnl_1min_prod_v2'
        history_table: e.g. 'analytics.strategy_output_history_v2'
        start_ts: reference_ts - 3 days; bootstrap anchor point
    """
    start_str = start_ts.strftime("%Y-%m-%d %H:%M:%S")

    # Step 1: get latest pnl row per strategy before start_ts to seed cumulative_pnl.
    # Price comes from futures_price_1min at the same minute, not from the pnl table.
    pnl_sql = f"""\
SELECT
    p.strategy_table_name,
    p.strategy_instance_id,
    p.cumulative_pnl,
    p.ts AS pnl_ts,
    coalesce(fp.open, 0.0) AS price
FROM (
    SELECT
        strategy_table_name,
        strategy_instance_id,
        cumulative_pnl,
        ts,
        underlying
    FROM {pnl_table}
    WHERE ts < '{start_str}'
    ORDER BY strategy_table_name, ts DESC, updated_at DESC
    LIMIT 1 BY strategy_table_name
) p
LEFT JOIN analytics.futures_price_1min fp
  ON fp.instrument = p.underlying || 'USDT'
 AND fp.ts = p.ts
"""
    pnl_seeds: dict[str, dict] = {}
    for row in query_dicts(pnl_sql):
        pnl_seeds[row["strategy_table_name"]] = {
            "strategy_instance_id": row["strategy_instance_id"],
            "pnl": float(row["cumulative_pnl"] or 0.0),
            "price": float(row["price"]),
        }

    # Step 2: get latest position per strategy_instance_id from history at start_ts.
    # ts <= start_ts (not revision_ts) because bars arrive late; we want what was
    # visible at start_ts, even if revision_ts is slightly later.
    pos_sql = f"""\
SELECT
    strategy_table_name,
    strategy_instance_id,
    argMin(row_json, revision_ts) AS row_json
FROM {history_table}
WHERE ts <= '{start_str}'
GROUP BY strategy_table_name, strategy_instance_id
"""
    positions: dict[str, float] = {}
    for row in query_dicts(pos_sql):
        rj = json.loads(row["row_json"])
        key = row["strategy_table_name"]
        positions[key] = float(rj.get("position", 0.0))

    # Merge: every strategy seen in either source gets a seed entry.
    all_keys = set(pnl_seeds) | set(positions)
    seeds: list[BootstrapSeed] = []
    for stn in all_keys:
        pnl_info = pnl_seeds.get(stn, {})
        seeds.append(
            BootstrapSeed(
                strategy_table_name=stn,
                strategy_instance_id=pnl_info.get("strategy_instance_id", ""),
                pnl=pnl_info.get("pnl", 0.0),
                price=pnl_info.get("price", 0.0),
                position=positions.get(stn, 0.0),
            )
        )
    return seeds


def fetch_walk_rows(
    pnl_table: str,
    history_table: str,
    start_ts: datetime,
    reference_ts: datetime,
) -> list[WalkRow]:
    """Return stored PnL rows in [start_ts, reference_ts) ordered chronologically.

    Used during the bootstrap walk to verify that stored cumulative_pnl values are
    consistent with the formula: pnl = prev_pnl + position * (price - prev_price) / prev_price

    Price for each row comes from futures_price_1min at that minute — never from the
    pnl table's price column.

    Position for each row is the latest bar in history_table with ts <= row.ts,
    first revision only (argMin by revision_ts).

    reference_ts is EXCLUDED — the consumer will process that candle live from Kafka.
    """
    start_str = start_ts.strftime("%Y-%m-%d %H:%M:%S")
    ref_str = reference_ts.strftime("%Y-%m-%d %H:%M:%S")

    sql = f"""\
SELECT
    p.strategy_table_name,
    p.strategy_instance_id,
    p.ts,
    p.cumulative_pnl,
    coalesce(fp.open, 0.0) AS price,
    p.underlying
FROM (
    SELECT
        strategy_table_name,
        strategy_instance_id,
        ts,
        cumulative_pnl,
        underlying
    FROM {pnl_table}
    WHERE ts >= '{start_str}'
      AND ts < '{ref_str}'
    ORDER BY strategy_table_name, ts ASC, updated_at DESC
    LIMIT 1 BY strategy_table_name, ts
) p
LEFT JOIN analytics.futures_price_1min fp
  ON fp.instrument = p.underlying || 'USDT'
 AND fp.ts = p.ts
ORDER BY p.strategy_table_name, p.ts
"""
    rows = query_dicts(sql)
    if not rows:
        return []

    # Fetch position for each (strategy_table_name, ts) pair in one batch.
    # For each row ts, the active position is the latest bar with ts <= row.ts.
    # We get the min ts and max ts from the walk window and fetch all bars in range,
    # then resolve per row in Python.
    max_ts_str = ref_str  # walk goes up to reference_ts exclusive
    pos_sql = f"""\
SELECT
    strategy_table_name,
    strategy_instance_id,
    ts AS bar_ts,
    argMin(row_json, revision_ts) AS row_json
FROM {history_table}
WHERE ts <= '{max_ts_str}'
  AND ts >= '{start_str}'
GROUP BY strategy_table_name, strategy_instance_id, ts
ORDER BY strategy_table_name, ts
"""
    # Build position lookup: stn -> sorted list of (bar_ts, position)
    from bisect import bisect_right
    bar_positions: dict[str, list[tuple[datetime, float]]] = {}
    for pos_row in query_dicts(pos_sql):
        stn = pos_row["strategy_table_name"]
        rj = json.loads(pos_row["row_json"])
        position = float(rj.get("position", 0.0))
        bar_ts = pos_row["bar_ts"]
        if stn not in bar_positions:
            bar_positions[stn] = []
        bar_positions[stn].append((bar_ts, position))

    def _get_position(stn: str, row_ts: datetime) -> float:
        bars = bar_positions.get(stn)
        if not bars:
            return 0.0
        ts_keys = [b[0] for b in bars]
        idx = bisect_right(ts_keys, row_ts) - 1
        return bars[idx][1] if idx >= 0 else 0.0

    result: list[WalkRow] = []
    for row in rows:
        result.append(
            WalkRow(
                strategy_table_name=row["strategy_table_name"],
                strategy_instance_id=row["strategy_instance_id"],
                ts=row["ts"],
                cumulative_pnl=float(row["cumulative_pnl"] or 0.0),
                price=float(row["price"]),
                position=_get_position(row["strategy_table_name"], row["ts"]),
            )
        )
    return result
