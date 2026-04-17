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
) -> Dict[str, Tuple[float, float, float]]:
    """Read last committed PnL per strategy from target.

    Returns: {strategy_table_name: (anchor_pnl, anchor_price, anchor_position)}
    """
    # Use LIMIT 1 BY to get the latest row per strategy without FINAL or a self-join.
    # ORDER BY ts DESC, updated_at DESC aligns with the sort key (strategy_table_name, ts)
    # so ClickHouse can skip to the last part per strategy rather than doing a full scan.
    sql = f"""\
SELECT
    strategy_table_name,
    cumulative_pnl AS anchor_pnl,
    price          AS anchor_price,
    position       AS anchor_position
FROM analytics.{target_table}
WHERE underlying = '{underlying}'
  AND ts >= now() - INTERVAL 2 HOUR
ORDER BY strategy_table_name, ts DESC, updated_at DESC
LIMIT 1 BY strategy_table_name
"""
    rows = query_dicts(sql)
    result = {}
    for r in rows:
        pnl = float(r["anchor_pnl"]) if r["anchor_pnl"] is not None else 0.0
        price = float(r["anchor_price"]) if r["anchor_price"] is not None else 0.0
        pos = float(r["anchor_position"]) if r["anchor_position"] is not None else 0.0
        result[r["strategy_table_name"]] = (pnl, price, pos)
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


def fetch_new_bars_bt(
    source_table: str, underlying: str, since: str,
) -> List[dict]:
    """Fetch new bars for bt: argMin(row_json, revision_ts) and extract cumulative_pnl."""
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
    JSONExtractFloat(argMin(row_json, revision_ts), 'benchmark') AS bar_benchmark,
    JSONExtractFloat(argMin(row_json, revision_ts), 'cumulative_pnl') AS cumulative_pnl
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
            "cumulative_pnl": float(r["cumulative_pnl"]),
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
      AND revision_ts < ts + toIntervalMinute(timeframe_minutes)
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


def _underlying_to_instrument(underlying: str) -> str:
    """Derive futures_price_1min instrument from underlying.

    Handles cases where underlying is already 'ADAUSDT', 'ada', 'ADA', etc.
    Always returns uppercase e.g. 'ADAUSDT'.
    """
    u = underlying.upper()
    if u.endswith("USDT"):
        return u
    return f"{u}USDT"


def fetch_prices(
    underlying: str, ts_min: str, ts_max: str,
    client=None,
) -> Dict[str, float]:
    """Read 1-min open prices for a time window. Returns {ts_string: open}.

    The window extends 2 timeframe-lengths (max 1d) past ts_max to cover
    the last bar's expansion into 1-min intervals.
    """
    instrument = _underlying_to_instrument(underlying)
    sql = f"""\
SELECT toString(ts), open
FROM analytics.futures_price_1min
WHERE exchange = 'binance'
  AND instrument = '{instrument}'
  AND ts >= toDateTime('{ts_min}')
  AND ts < toDateTime('{ts_max}') + toIntervalMinute(1440)
"""
    rows = query_rows(sql, client)
    return {str(r[0]): float(r[1]) for r in rows}


def fetch_prices_multi(
    underlyings: List[str], ts_min: str, ts_max: str,
    client=None,
    extend_minutes: int = 1440,
) -> Dict[str, Dict[str, float]]:
    """Fetch 1-min open prices for multiple underlyings in a single query.

    Returns {underlying: {ts_string: open}} so the per-underlying loop can
    slice its own dict without firing a separate ClickHouse query each time.
    This avoids N sequential scans on futures_price_1min (one per underlying)
    which can exceed ClickHouse Cloud's memory limit for large partition runs.

    extend_minutes: extra minutes past ts_max to fetch (default 1440 = 1 day,
    for the live path where ts_max is the last bar open time). Pass 0 for
    the daily partition path where ts_max is already the exclusive end boundary.
    """
    if not underlyings:
        return {}
    instruments = [_underlying_to_instrument(u) for u in underlyings]
    instrument_list = ", ".join(f"'{i}'" for i in instruments)
    extend_clause = f" + toIntervalMinute({extend_minutes})" if extend_minutes > 0 else ""
    sql = f"""\
SELECT instrument, toString(ts), open
FROM analytics.futures_price_1min
WHERE exchange = 'binance'
  AND instrument IN ({instrument_list})
  AND ts >= toDateTime('{ts_min}')
  AND ts < toDateTime('{ts_max}'){extend_clause}
"""
    rows = query_rows(sql, client)
    # Build reverse map: instrument → underlying (handles lowercase/no-USDT inputs)
    instr_to_underlying = {_underlying_to_instrument(u): u for u in underlyings}
    result: Dict[str, Dict[str, float]] = {u: {} for u in underlyings}
    for row in rows:
        instrument, ts_str, open_price = row[0], str(row[1]), float(row[2])
        u = instr_to_underlying.get(instrument)
        if u is not None:
            result[u][ts_str] = open_price
    return result


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
        prices: {ts_string: open_price}
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

        anchor_pnl, anchor_open_price, active_position = anchors.get(stn, (0.0, 0.0, 0.0))

        for bar in strategy_bars:
            tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
            bar_ts = datetime.strptime(bar["ts"], "%Y-%m-%d %H:%M:%S")
            position = bar["position"]
            bar_price = bar["bar_price"] # JSON price for initial fallback

            for n in range(tf_minutes):
                ts_1min = bar_ts + timedelta(minutes=n)
                ts_str = ts_1min.strftime("%Y-%m-%d %H:%M:%S")
                
                live_open_price = prices.get(ts_str, anchor_open_price if anchor_open_price != 0.0 else bar_price)

                if anchor_open_price == 0.0:
                    anchor_open_price = live_open_price

                if anchor_open_price != 0.0:
                    cpnl = anchor_pnl + position * (live_open_price - anchor_open_price) / anchor_open_price
                else:
                    cpnl = anchor_pnl

                output_rows.append([
                    stn, bar["strategy_id"], bar["strategy_name"],
                    bar["underlying"], bar["config_timeframe"],
                    source_label, "v2", ts_str, cpnl,
                    bar["bar_benchmark"], position, live_open_price,
                    bar["final_signal"], bar["weighting"], now_str,
                ])

                # Advance for next minute
                anchor_pnl = cpnl
                anchor_open_price = live_open_price

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
        prices: {ts_string: open_price}

    Returns: List of rows ready for INSERT (REAL_TRADE_INSERT_COLUMNS order)
    """
    by_strategy: Dict[str, List[dict]] = defaultdict(list)
    for bar in bars:
        by_strategy[bar["strategy_table_name"]].append(bar)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_rows = []

    for stn, strategy_bars in by_strategy.items():
        anchor_pnl, anchor_open_price, active_position = anchors.get(stn, (0.0, 0.0, 0.0))

        # We must generate a timeline for each unique closing_ts block.
        # Store latest bar dictionary per closing_ts block for formatting fallbacks.
        closing_blocks = {}
        for b in strategy_bars:
            cts = b["closing_ts"]
            if cts not in closing_blocks:
                closing_blocks[cts] = b
            else:
                # keep the latest revision as the base metadata for this hour
                if b.get("revision_ts", "") > closing_blocks[cts].get("revision_ts", ""):
                    closing_blocks[cts] = b

        sorted_closing = sorted(closing_blocks.keys())
        sorted_revs = sorted(strategy_bars, key=lambda b: b["execution_ts"])

        rev_idx = 0
        n_revs = len(sorted_revs)

        for cts in sorted_closing:
            base_bar = closing_blocks[cts]
            tf_minutes = TIMEFRAME_MAP.get(base_bar["config_timeframe"], 5)
            closing_ts_dt = datetime.strptime(cts, "%Y-%m-%d %H:%M:%S")

            for n in range(tf_minutes):
                ts_1min = closing_ts_dt + timedelta(minutes=n)
                ts_str = ts_1min.strftime("%Y-%m-%d %H:%M:%S")

                # Advance active_position if we reach a new execution_ts
                while rev_idx < n_revs and sorted_revs[rev_idx]["execution_ts"] <= ts_str:
                    active_position = sorted_revs[rev_idx]["position"]
                    # If this revision provides updated metadata we can use it
                    base_bar = sorted_revs[rev_idx]
                    rev_idx += 1

                live_open_price = prices.get(ts_str, anchor_open_price if anchor_open_price != 0.0 else base_bar["bar_price"])

                if anchor_open_price == 0.0:
                    anchor_open_price = live_open_price

                if anchor_open_price != 0.0:
                    cpnl = anchor_pnl + active_position * (live_open_price - anchor_open_price) / anchor_open_price
                else:
                    cpnl = anchor_pnl

                output_rows.append([
                    stn, base_bar["strategy_id"], base_bar["strategy_name"],
                    base_bar["underlying"], base_bar["config_timeframe"],
                    "real_trade", "v2", ts_str, cpnl,
                    base_bar["bar_benchmark"], active_position, live_open_price,
                    base_bar["final_signal"], base_bar["weighting"], now_str,
                    base_bar["closing_ts"], base_bar["execution_ts"], "false",
                ])

                # Advance strictly chronological state
                anchor_pnl = cpnl
                anchor_open_price = live_open_price

    return output_rows


def compute_bt_pnl(bars: List[dict]) -> List[list]:
    """Compute backtest PnL directly from strategy JSON cumulative_pnl. Expand to 1-min intervals."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_rows = []

    for bar in bars:
        tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
        bar_ts = datetime.strptime(bar["ts"], "%Y-%m-%d %H:%M:%S")

        for n in range(tf_minutes):
            ts_1min = bar_ts + timedelta(minutes=tf_minutes + n)
            ts_str = ts_1min.strftime("%Y-%m-%d %H:%M:%S")

            output_rows.append([
                bar["strategy_table_name"], bar["strategy_id"], bar["strategy_name"],
                bar["underlying"], bar["config_timeframe"],
                "backtest", "v2", ts_str, 
                bar["cumulative_pnl"],
                bar["bar_benchmark"], bar["position"], bar["bar_price"],
                bar["final_signal"], bar["weighting"], now_str,
            ])

    return output_rows
