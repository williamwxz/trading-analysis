"""History bar and anchor fetchers from ClickHouse — used by Dagster's batch assets.

The pnl_consumer does not use these. It calls libs.computation.candle_lookup
for per-candle re-queries and libs.computation.bootstrap for cold-start.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from libs.clickhouse_client import query_dicts
from libs.computation.pnl_formula import TIMEFRAME_MAP

_TF_EXPR = """\
multiIf(
    config_timeframe = '1m',  1,
    config_timeframe = '3m',  3,
    config_timeframe = '5m',  5,
    config_timeframe = '10m', 10,
    config_timeframe = '15m', 15,
    config_timeframe = '30m', 30,
    config_timeframe = '1h',  60,
    config_timeframe = '4h',  240,
    config_timeframe = '1d',  1440,
    5
)"""


def _parse_ts(s: str) -> datetime:
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")


_ANCHOR_LOOKBACK_DAYS = 1


def fetch_anchors(
    target_table: str,
    underlying: str,
    before_ts: Optional[datetime] = None,
    client=None,
) -> Dict[str, Tuple[float, float, float]]:
    """Read the last committed PnL row per strategy_instance_id from the target table.

    Returns {strategy_table_name: (anchor_pnl, anchor_price, anchor_position)}.
    before_ts: if provided, only rows with ts < before_ts are considered.

    A 1-day lower bound on ts is always applied so ClickHouse can prune partitions
    via the (strategy_table_name, ts) sort key rather than scanning the full table.
    Strategies inactive for >1 day have no active position and will be zero-seeded
    by the caller's lazy-seed path when they reappear.
    """
    upper_ts = before_ts if before_ts is not None else datetime.utcnow()
    lower_ts = upper_ts - timedelta(days=_ANCHOR_LOOKBACK_DAYS)
    lower_str = lower_ts.strftime("%Y-%m-%d %H:%M:%S")

    ts_filter = f"  AND ts >= toDateTime('{lower_str}')\n"
    if before_ts is not None:
        ts_str = before_ts.strftime("%Y-%m-%d %H:%M:%S")
        ts_filter += f"  AND ts < toDateTime('{ts_str}')\n"
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_instance_id,
    argMax(cumulative_pnl, (ts, updated_at)) AS anchor_pnl,
    argMax(price,          (ts, updated_at)) AS anchor_price,
    argMax(position,       (ts, updated_at)) AS anchor_position
FROM analytics.{target_table}
WHERE underlying = '{underlying}'
{ts_filter}GROUP BY strategy_table_name, strategy_instance_id
"""
    result: Dict[str, Tuple[float, float, float]] = {}
    for r in query_dicts(sql, client):
        pnl = float(r["anchor_pnl"]) if r["anchor_pnl"] is not None else 0.0
        price = float(r["anchor_price"]) if r["anchor_price"] is not None else 0.0
        pos = float(r["anchor_position"]) if r["anchor_position"] is not None else 0.0
        result[r["strategy_table_name"]] = (pnl, price, pos)
    return result


def fetch_new_bars_prod(
    source_table: str,
    underlying: str,
    ts_start: str,
    ts_end: str,
    client=None,
) -> List[dict]:
    """Fetch bars for prod/bt batch recompute in [ts_start, ts_end).

    Returns first revision per (strategy, bar_ts) using LIMIT 1 BY ordered by
    revision_ts ascending — avoids materializing all row_json blobs for argMin.
    """
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    strategy_instance_id,
    weighting,
    toString(ts)                                                     AS ts,
    JSONExtractFloat(row_json, 'position')     AS position,
    JSONExtractFloat(row_json, 'final_signal') AS final_signal,
    JSONExtractFloat(row_json, 'benchmark')    AS bar_benchmark
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(ts) >= toDateTime('{ts_start}')
  AND toDateTime(ts) < toDateTime('{ts_end}')
ORDER BY strategy_table_name, ts, revision_ts
LIMIT 1 BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
"""
    return [
        {
            "strategy_table_name": r["strategy_table_name"],
            "strategy_id": int(r["strategy_id"]),
            "strategy_name": r["strategy_name"],
            "underlying": r["underlying"],
            "config_timeframe": r["config_timeframe"],
            "strategy_instance_id": str(r["strategy_instance_id"]),
            "weighting": float(r["weighting"]),
            "ts": str(r["ts"]),
            "position": float(r["position"]),
            "final_signal": float(r["final_signal"]),
            "bar_benchmark": float(r["bar_benchmark"]),
        }
        for r in query_dicts(sql, client)
    ]


def fetch_new_bars_bt(
    source_table: str,
    underlying: str,
    ts_start: str,
    ts_end: str,
    client=None,
) -> List[dict]:
    """Fetch bars for bt batch recompute in [ts_start, ts_end).

    Like fetch_new_bars_prod but also extracts cumulative_pnl (bt cold-start seed)
    and computes execution_ts = ts + tf_minutes (bar close time).

    Uses LIMIT 1 BY instead of argMin(row_json) to avoid materializing all row_json
    blobs for sorting — ClickHouse picks the first row per group using the sort key.
    """
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    strategy_instance_id,
    weighting,
    toString(ts)                                                         AS ts,
    JSONExtractFloat(row_json, 'position')       AS position,
    JSONExtractFloat(row_json, 'final_signal')   AS final_signal,
    JSONExtractFloat(row_json, 'benchmark')      AS bar_benchmark,
    JSONExtractFloat(row_json, 'cumulative_pnl') AS cumulative_pnl
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(ts) >= toDateTime('{ts_start}')
  AND toDateTime(ts) < toDateTime('{ts_end}')
ORDER BY strategy_table_name, ts, revision_ts
LIMIT 1 BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
"""
    result = []
    for r in query_dicts(sql, client):
        tf = str(r["config_timeframe"])
        if tf not in TIMEFRAME_MAP:
            raise ValueError(f"Unknown config_timeframe '{tf}' in {source_table}")
        bar_ts = _parse_ts(r["ts"])
        execution_ts = (bar_ts + timedelta(minutes=TIMEFRAME_MAP[tf])).strftime("%Y-%m-%d %H:%M:%S")
        result.append({
            "strategy_table_name": r["strategy_table_name"],
            "strategy_id": int(r["strategy_id"]),
            "strategy_name": r["strategy_name"],
            "underlying": r["underlying"],
            "config_timeframe": tf,
            "strategy_instance_id": str(r["strategy_instance_id"]),
            "weighting": float(r["weighting"]),
            "ts": str(r["ts"]),
            "execution_ts": execution_ts,
            "position": float(r["position"]),
            "final_signal": float(r["final_signal"]),
            "bar_benchmark": float(r["bar_benchmark"]),
            "cumulative_pnl": float(r["cumulative_pnl"]),
        })
    return result


_RT_BAR_LOOKBACK_MINUTES = max(TIMEFRAME_MAP.values())  # 1440 — covers 1d bars


def fetch_new_bars_real_trade(
    source_table: str,
    underlying: str,
    ts_start: str,
    ts_end: str,
    client=None,
) -> List[dict]:
    """Fetch real_trade revisions for bars whose execution window overlaps [ts_start, ts_end).

    The lower bound on bar ts is extended by _RT_BAR_LOOKBACK_MINUTES (1440 min) so that
    bars with a large revision lag (e.g. 1d strategies where execution_ts = revision_ts + 59s
    arrives ~24h after bar open) are included even when their bar ts falls just before ts_start.
    Without this, the bar covering [ts_start, ts_start+tf) is excluded from the fetch and
    creates a gap at the window boundary on every recompute run.

    The extra bars fetched before ts_start are harmless: _process_underlying_recent clamps
    minute_cur = max(first_active_minute, window_start), so no rows are written before ts_start.

    Each revision includes:
    - execution_ts = toStartOfMinute(revision_ts + 59s) — when position takes effect
    - closing_ts   = ts + tf_minutes — bar close time
    """
    fetch_start = (
        datetime.strptime(ts_start[:19], "%Y-%m-%d %H:%M:%S")
        - timedelta(minutes=_RT_BAR_LOOKBACK_MINUTES)
    ).strftime("%Y-%m-%d %H:%M:%S")
    tf_expr = _TF_EXPR
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    strategy_instance_id,
    weighting,
    toString(ts)                                                        AS ts,
    toString(ts + toIntervalMinute({tf_expr}))                          AS closing_ts,
    toString(toStartOfMinute(revision_ts + INTERVAL 59 SECOND))         AS execution_ts,
    toString(revision_ts)                                               AS revision_ts,
    JSONExtractFloat(row_json, 'position')    AS position,
    JSONExtractFloat(row_json, 'final_signal') AS final_signal,
    JSONExtractFloat(row_json, 'benchmark')   AS bar_benchmark
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(ts) >= toDateTime('{fetch_start}')
  AND toDateTime(ts) < toDateTime('{ts_end}')
ORDER BY strategy_table_name, ts, revision_ts
"""
    return [
        {
            "strategy_table_name": r["strategy_table_name"],
            "strategy_id": int(r["strategy_id"]),
            "strategy_name": r["strategy_name"],
            "underlying": r["underlying"],
            "config_timeframe": r["config_timeframe"],
            "strategy_instance_id": str(r["strategy_instance_id"]),
            "weighting": float(r["weighting"]),
            "ts": str(r["ts"]),
            "closing_ts": str(r["closing_ts"]),
            "execution_ts": str(r["execution_ts"]),
            "revision_ts": str(r["revision_ts"]),
            "position": float(r["position"]),
            "final_signal": float(r["final_signal"]),
            "bar_benchmark": float(r["bar_benchmark"]),
        }
        for r in query_dicts(sql, client)
    ]
