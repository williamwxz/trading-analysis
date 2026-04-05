"""
Shared Python PnL computation for v2 ClickHouse assets.

Computes anchor-chained cumulative_pnl in Python, expanding each bar
to 1-min intervals. This replaces the SQL anchor CTE approach which
cannot chain between multiple bars within a single INSERT batch.

Used by:
- pnl_prod_v2_refresh
- pnl_bt_v2_refresh
- pnl_real_trade_v2_refresh

Migrated from falcon-lakehouse — uses clickhouse_client instead of raw urllib.
"""

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from .clickhouse_client import query_dicts, query_rows

TIMEFRAME_MAP = {
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}


# ─────────────────────────────────────────────────────────────────────────────
# Data fetchers
# ─────────────────────────────────────────────────────────────────────────────


def fetch_anchors(
    target_table: str, underlying: str,
) -> Dict[str, Tuple[float, float]]:
    """Read last committed PnL per strategy from target.

    Returns: {strategy_table_name: (anchor_pnl, anchor_price)}
    """
    sql = f"""\
WITH max_ts AS (
    SELECT strategy_table_name AS stn, max(ts) AS mts
    FROM analytics.{target_table}
    WHERE underlying = '{underlying}'
    GROUP BY strategy_table_name
)
SELECT
    m.stn AS strategy_table_name,
    argMax(t.cumulative_pnl, t.updated_at) AS anchor_pnl,
    argMax(t.price, t.updated_at) AS anchor_price
FROM max_ts m
INNER JOIN analytics.{target_table} t
    ON t.strategy_table_name = m.stn AND t.ts = m.mts
GROUP BY m.stn
"""
    rows = query_dicts(sql)
    result = {}
    for r in rows:
        pnl = float(r["anchor_pnl"]) if r["anchor_pnl"] is not None else 0.0
        price = float(r["anchor_price"]) if r["anchor_price"] is not None else 0.0
        result[r["strategy_table_name"]] = (pnl, price)
    return result


def fetch_new_bars_prod(
    source_table: str, underlying: str, since: str,
) -> List[dict]:
    """Fetch new bars for prod/bt: argMin(row_json, revision_ts) GROUP BY (strategy, ts)."""
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    argMin(weighting, revision_ts) AS weighting,
    toString(ts) AS ts,
    JSONExtractFloat(argMin(row_json, revision_ts), 'position') AS position,
    JSONExtractFloat(argMin(row_json, revision_ts), 'price') AS bar_price,
    JSONExtractFloat(argMin(row_json, revision_ts), 'final_signal') AS final_signal,
    JSONExtractFloat(argMin(row_json, revision_ts), 'benchmark') AS bar_benchmark
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND revision_ts >= toDateTime('{since}')
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
ORDER BY strategy_table_name, ts
"""
    rows = query_dicts(sql)
    return [
        {
            "strategy_table_name": r["strategy_table_name"],
            "strategy_id": int(r["strategy_id"]),
            "strategy_name": r["strategy_name"],
            "underlying": r["underlying"],
            "config_timeframe": r["config_timeframe"],
            "weighting": float(r["weighting"]),
            "ts": str(r["ts"]),
            "position": float(r["position"]),
            "bar_price": float(r["bar_price"]),
            "final_signal": float(r["final_signal"]),
            "bar_benchmark": float(r["bar_benchmark"]),
        }
        for r in rows
    ]


def fetch_new_bars_real_trade(
    source_table: str, underlying: str, since: str,
) -> List[dict]:
    """Fetch real_trade bars with execution_ts filtering. Multiple revisions per bar."""
    sql = f"""\
WITH
raw AS (
    SELECT
        strategy_table_name, config_timeframe, ts, revision_ts,
        strategy_id, strategy_name, underlying, weighting, row_json,
        multiIf(
            config_timeframe = '5m', 5, config_timeframe = '10m', 10,
            config_timeframe = '15m', 15, config_timeframe = '30m', 30,
            config_timeframe = '1h', 60, config_timeframe = '4h', 240,
            config_timeframe = '1d', 1440, 5
        ) AS timeframe_minutes,
        ts + toIntervalMinute(timeframe_minutes) AS closing_ts,
        toStartOfMinute(revision_ts + INTERVAL 59 SECOND) AS execution_ts
    FROM analytics.{source_table}
    WHERE underlying = '{underlying}'
      AND strategy_table_name NOT LIKE 'manual_probe%'
      AND revision_ts >= toDateTime('{since}')
),
with_flag AS (
    SELECT *,
        if(execution_ts != lagInFrame(execution_ts, 1, toDateTime('1970-01-01'))
            OVER (PARTITION BY strategy_table_name ORDER BY closing_ts, revision_ts),
            1, 0) AS is_new_group
    FROM raw
),
with_gid AS (
    SELECT *,
        sum(is_new_group) OVER (PARTITION BY strategy_table_name ORDER BY closing_ts, revision_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS gid
    FROM with_flag
),
group_next AS (
    SELECT strategy_table_name, gid,
        any(execution_ts) AS grp_exec_ts,
        leadInFrame(any(execution_ts), 1, toDateTime('9999-12-31'))
            OVER (PARTITION BY strategy_table_name ORDER BY gid
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_exec_ts
    FROM with_gid
    GROUP BY strategy_table_name, gid
),
filtered AS (
    SELECT w.*
    FROM with_gid w
    INNER JOIN group_next g
        ON w.strategy_table_name = g.strategy_table_name AND w.gid = g.gid
    WHERE w.execution_ts < g.next_exec_ts
)
SELECT
    strategy_table_name,
    strategy_id, strategy_name, underlying, config_timeframe, weighting,
    toString(ts) AS ts,
    toString(closing_ts) AS closing_ts,
    toString(execution_ts) AS execution_ts,
    JSONExtractFloat(row_json, 'position') AS position,
    JSONExtractFloat(row_json, 'price') AS bar_price,
    JSONExtractFloat(row_json, 'final_signal') AS final_signal,
    JSONExtractFloat(row_json, 'benchmark') AS bar_benchmark
FROM filtered
ORDER BY strategy_table_name, closing_ts, revision_ts
"""
    rows = query_dicts(sql)
    return [
        {
            "strategy_table_name": r["strategy_table_name"],
            "strategy_id": int(r["strategy_id"]),
            "strategy_name": r["strategy_name"],
            "underlying": r["underlying"],
            "config_timeframe": r["config_timeframe"],
            "weighting": float(r["weighting"]),
            "ts": str(r["ts"]),
            "closing_ts": str(r["closing_ts"]),
            "execution_ts": str(r["execution_ts"]),
            "position": float(r["position"]),
            "bar_price": float(r["bar_price"]),
            "final_signal": float(r["final_signal"]),
            "bar_benchmark": float(r["bar_benchmark"]),
        }
        for r in rows
    ]


def fetch_prices(
    underlying: str, ts_min: str, ts_max: str,
) -> Dict[str, float]:
    """Read 1-min close prices for a time window. Returns {ts_string: close}."""
    instrument = f"{underlying.upper()}USDT"
    sql = f"""\
SELECT toString(ts), close
FROM analytics.futures_price_1min
WHERE exchange = 'binance'
  AND instrument = '{instrument}'
  AND ts >= toDateTime('{ts_min}')
  AND ts < toDateTime('{ts_max}') + toIntervalMinute(2880)
"""
    rows = query_rows(sql)
    return {str(r[0]): float(r[1]) for r in rows}


# ─────────────────────────────────────────────────────────────────────────────
# PnL computation
# ─────────────────────────────────────────────────────────────────────────────


PROD_INSERT_COLUMNS = [
    "strategy_table_name", "strategy_id", "strategy_name",
    "underlying", "config_timeframe", "source", "version",
    "ts", "cumulative_pnl", "benchmark", "position", "price",
    "final_signal", "weighting", "updated_at",
]

REAL_TRADE_INSERT_COLUMNS = [
    "strategy_table_name", "strategy_id", "strategy_name",
    "underlying", "config_timeframe", "source", "version",
    "ts", "cumulative_pnl", "benchmark", "position", "price",
    "final_signal", "weighting", "updated_at",
    "closing_ts", "execution_ts", "traded",
]


def compute_prod_pnl(
    bars: List[dict],
    anchors: Dict[str, Tuple[float, float]],
    prices: Dict[str, float],
    source_label: str = "production",
) -> List[list]:
    """Compute anchor-chained PnL for prod/bt bars. Expand to 1-min intervals.

    Args:
        bars: List of bar dicts (one per strategy+ts, sorted by strategy, ts)
        anchors: {strategy_table_name: (anchor_pnl, anchor_price)} from target
        prices: {ts_string: close_price}
        source_label: "production" or "backtest"

    Returns: List of rows ready for INSERT (PROD_INSERT_COLUMNS order)
    """
    by_strategy: Dict[str, List[dict]] = defaultdict(list)
    for bar in bars:
        by_strategy[bar["strategy_table_name"]].append(bar)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_rows = []

    for stn, strategy_bars in by_strategy.items():
        strategy_bars.sort(key=lambda b: b["ts"])

        anchor_pnl, anchor_price = anchors.get(stn, (0.0, 0.0))

        for bar in strategy_bars:
            tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
            bar_ts = datetime.strptime(bar["ts"], "%Y-%m-%d %H:%M:%S")
            position = bar["position"]
            bar_price = bar["bar_price"]

            if anchor_price == 0.0:
                anchor_price = bar_price

            # Expand to 1-min intervals
            for n in range(tf_minutes):
                ts_1min = bar_ts + timedelta(minutes=tf_minutes + n)
                ts_str = ts_1min.strftime("%Y-%m-%d %H:%M:%S")
                live_price = prices.get(ts_str, bar_price)

                if anchor_price != 0.0:
                    cpnl = anchor_pnl + position * (live_price - anchor_price) / anchor_price
                else:
                    cpnl = 0.0

                output_rows.append([
                    stn, bar["strategy_id"], bar["strategy_name"],
                    bar["underlying"], bar["config_timeframe"],
                    source_label, "v2", ts_str, cpnl,
                    bar["bar_benchmark"], position, live_price,
                    bar["final_signal"], bar["weighting"], now_str,
                ])

            # Update anchor for next bar
            last_ts = bar_ts + timedelta(minutes=tf_minutes + tf_minutes - 1)
            last_ts_str = last_ts.strftime("%Y-%m-%d %H:%M:%S")
            last_price = prices.get(last_ts_str, bar_price)
            if anchor_price != 0.0:
                anchor_pnl = anchor_pnl + position * (last_price - anchor_price) / anchor_price
            anchor_price = last_price

    return output_rows


def compute_real_trade_pnl(
    bars: List[dict],
    anchors: Dict[str, Tuple[float, float]],
    prices: Dict[str, float],
) -> List[list]:
    """Compute anchor-chained PnL for real_trade bars. Expand to 1-min intervals.

    Args:
        bars: List of bar dicts (may have multiple revisions per bar, sorted by
              strategy, closing_ts, revision_ts)
        anchors: {strategy_table_name: (anchor_pnl, anchor_price)} from target
        prices: {ts_string: close_price}

    Returns: List of rows ready for INSERT (REAL_TRADE_INSERT_COLUMNS order)
    """
    by_strategy: Dict[str, List[dict]] = defaultdict(list)
    for bar in bars:
        by_strategy[bar["strategy_table_name"]].append(bar)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_rows = []

    for stn, strategy_bars in by_strategy.items():
        strategy_bars.sort(key=lambda b: (b["closing_ts"], b["ts"]))

        anchor_pnl, anchor_price = anchors.get(stn, (0.0, 0.0))

        for bar in strategy_bars:
            tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
            closing_ts = datetime.strptime(bar["closing_ts"], "%Y-%m-%d %H:%M:%S")
            position = bar["position"]
            bar_price = bar["bar_price"]

            if anchor_price == 0.0:
                anchor_price = bar_price

            for n in range(tf_minutes):
                ts_1min = closing_ts + timedelta(minutes=n)
                ts_str = ts_1min.strftime("%Y-%m-%d %H:%M:%S")
                live_price = prices.get(ts_str, bar_price)

                if anchor_price != 0.0:
                    cpnl = anchor_pnl + position * (live_price - anchor_price) / anchor_price
                else:
                    cpnl = 0.0

                output_rows.append([
                    stn, bar["strategy_id"], bar["strategy_name"],
                    bar["underlying"], bar["config_timeframe"],
                    "real_trade", "v2", ts_str, cpnl,
                    bar["bar_benchmark"], position, live_price,
                    bar["final_signal"], bar["weighting"], now_str,
                    bar["closing_ts"], bar["execution_ts"], "false",
                ])

            # Update anchor for next bar
            last_ts = closing_ts + timedelta(minutes=tf_minutes - 1)
            last_ts_str = last_ts.strftime("%Y-%m-%d %H:%M:%S")
            last_price = prices.get(last_ts_str, bar_price)
            if anchor_price != 0.0:
                anchor_pnl = anchor_pnl + position * (last_price - anchor_price) / anchor_price
            anchor_price = last_price

    return output_rows
