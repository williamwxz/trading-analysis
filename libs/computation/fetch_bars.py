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
    cumulative_pnl AS anchor_pnl,
    price          AS anchor_price,
    position       AS anchor_position
FROM analytics.{target_table}
WHERE underlying = '{underlying}'
{ts_filter}ORDER BY strategy_instance_id, ts DESC, updated_at DESC
LIMIT 1 BY strategy_instance_id
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

    Returns first revision per (strategy, bar_ts) via argMin(row_json, revision_ts).
    """
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    argMin(strategy_instance_id, revision_ts) AS strategy_instance_id,
    argMin(weighting, revision_ts)            AS weighting,
    toString(ts)                              AS ts,
    JSONExtractFloat(argMin(row_json, revision_ts), 'position')     AS position,
    JSONExtractFloat(argMin(row_json, revision_ts), 'final_signal') AS final_signal,
    JSONExtractFloat(argMin(row_json, revision_ts), 'benchmark')    AS bar_benchmark
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(ts) >= toDateTime('{ts_start}')
  AND toDateTime(ts) < toDateTime('{ts_end}')
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
ORDER BY strategy_table_name, ts
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
    """
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    argMin(strategy_instance_id, revision_ts) AS strategy_instance_id,
    argMin(weighting, revision_ts)            AS weighting,
    toString(ts)                              AS ts,
    JSONExtractFloat(argMin(row_json, revision_ts), 'position')       AS position,
    JSONExtractFloat(argMin(row_json, revision_ts), 'final_signal')   AS final_signal,
    JSONExtractFloat(argMin(row_json, revision_ts), 'benchmark')      AS bar_benchmark,
    JSONExtractFloat(argMin(row_json, revision_ts), 'cumulative_pnl') AS cumulative_pnl
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(ts) >= toDateTime('{ts_start}')
  AND toDateTime(ts) < toDateTime('{ts_end}')
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
ORDER BY strategy_table_name, ts
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


def fetch_new_bars_real_trade(
    source_table: str,
    underlying: str,
    ts_start: str,
    ts_end: str,
    client=None,
) -> List[dict]:
    """Fetch real_trade revisions for bars in [ts_start, ts_end).

    Each revision includes:
    - execution_ts = toStartOfMinute(revision_ts + 59s) — when position takes effect
    - closing_ts   = ts + tf_minutes — bar close time
    - next_bar_closing_ts — closing time of the next bar (sentinel = closing_ts if no next bar)

    compute_real_trade_pnl uses next_bar_closing_ts to decide whether to accept
    or discard each revision.
    """
    tf_expr = _TF_EXPR
    sql = f"""\
SELECT
    r.strategy_table_name,
    r.strategy_id,
    r.strategy_name,
    r.underlying,
    r.config_timeframe,
    r.strategy_instance_id,
    r.weighting,
    toString(r.ts)                                                          AS ts,
    toString(r.ts + toIntervalMinute({tf_expr}))                            AS closing_ts,
    toString(toStartOfMinute(r.revision_ts + INTERVAL 59 SECOND))           AS execution_ts,
    toString(r.revision_ts)                                                 AS revision_ts,
    toString(nb.next_bar_ts + toIntervalMinute({tf_expr}))                  AS next_bar_closing_ts,
    JSONExtractFloat(r.row_json, 'position')    AS position,
    JSONExtractFloat(r.row_json, 'final_signal') AS final_signal,
    JSONExtractFloat(r.row_json, 'benchmark')   AS bar_benchmark
FROM analytics.{source_table} r
LEFT JOIN (
    SELECT
        strategy_table_name,
        ts,
        leadInFrame(ts, 1, ts) OVER (
            PARTITION BY strategy_table_name ORDER BY ts
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_bar_ts
    FROM (
        SELECT strategy_table_name, ts
        FROM analytics.{source_table}
        WHERE underlying = '{underlying}'
          AND strategy_table_name NOT LIKE 'manual_probe%'
          AND toDateTime(ts) >= toDateTime('{ts_start}')
          AND toDateTime(ts) < toDateTime('{ts_end}')
        GROUP BY strategy_table_name, ts
    )
) nb ON r.strategy_table_name = nb.strategy_table_name AND r.ts = nb.ts
WHERE r.underlying = '{underlying}'
  AND r.strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(r.ts) >= toDateTime('{ts_start}')
  AND toDateTime(r.ts) < toDateTime('{ts_end}')
ORDER BY r.strategy_table_name, r.ts, r.revision_ts
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
            "next_bar_closing_ts": str(r["next_bar_closing_ts"]),
            "position": float(r["position"]),
            "final_signal": float(r["final_signal"]),
            "bar_benchmark": float(r["bar_benchmark"]),
        }
        for r in query_dicts(sql, client)
    ]
