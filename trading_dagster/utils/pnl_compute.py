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
    """Fetch new bars for bt: argMin(row_json, revision_ts) and extract cumulative_pnl.

    execution_ts = ts + tf_minutes (bar close), which is when the position takes effect
    and PnL starts being counted.
    """
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
    result = []
    for r in rows:
        tf = str(r["config_timeframe"])
        if tf not in TIMEFRAME_MAP:
            raise ValueError(f"Unknown config_timeframe '{tf}' in {source_table} — add it to TIMEFRAME_MAP")
        tf_minutes = TIMEFRAME_MAP[tf]
        bar_ts = datetime.strptime(str(r["ts"]), "%Y-%m-%d %H:%M:%S")
        execution_ts = (bar_ts + timedelta(minutes=tf_minutes)).strftime("%Y-%m-%d %H:%M:%S")
        result.append({
            "strategy_table_name": r["strategy_table_name"],
            "strategy_id": int(r["strategy_id"]),
            "strategy_name": r["strategy_name"],
            "underlying": r["underlying"],
            "config_timeframe": tf,
            "weighting": float(r["weighting"]),
            "ts": str(r["ts"]),
            "execution_ts": execution_ts,
            "position": float(r["position"]),
            "bar_price": float(r["bar_price"]),
            "final_signal": float(r["final_signal"]),
            "bar_benchmark": float(r["bar_benchmark"]),
            "cumulative_pnl": float(r["cumulative_pnl"]),
        })
    return result


def fetch_new_bars_real_trade(
    source_table: str, underlying: str, since: str,
) -> List[dict]:
    """Fetch real_trade revisions. Each revision that arrives before bar close is a separate bar."""
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_id, strategy_name, underlying, config_timeframe, weighting,
    toString(ts) AS ts,
    toString(ts + toIntervalMinute(multiIf(
        config_timeframe = '5m', 5, config_timeframe = '10m', 10,
        config_timeframe = '15m', 15, config_timeframe = '30m', 30,
        config_timeframe = '1h', 60, config_timeframe = '4h', 240,
        config_timeframe = '1d', 1440, 5
    ))) AS closing_ts,
    toString(toStartOfMinute(revision_ts + INTERVAL 59 SECOND)) AS execution_ts,
    JSONExtractFloat(row_json, 'position') AS position,
    JSONExtractFloat(row_json, 'price') AS bar_price,
    JSONExtractFloat(row_json, 'final_signal') AS final_signal,
    JSONExtractFloat(row_json, 'benchmark') AS bar_benchmark
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND revision_ts >= toDateTime('{since}')
  AND revision_ts < ts + toIntervalMinute(multiIf(
        config_timeframe = '5m', 5, config_timeframe = '10m', 10,
        config_timeframe = '15m', 15, config_timeframe = '30m', 30,
        config_timeframe = '1h', 60, config_timeframe = '4h', 240,
        config_timeframe = '1d', 1440, 5
    ))
ORDER BY strategy_table_name, ts, revision_ts
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
    output_rows = []
    for _, strategy_rows in iter_compute_prod_pnl(bars, anchors, prices, source_label):
        output_rows.extend(strategy_rows)
    return output_rows


def iter_compute_prod_pnl(
    bars: List[dict],
    anchors: Dict[str, Tuple[float, float]],
    prices: Dict[str, float],
    source_label: str = "production",
):
    """Same as compute_prod_pnl but yields (strategy_table_name, rows) per strategy.

    Use this in memory-constrained paths (e.g. large backfills) so each
    strategy's rows can be inserted and freed before processing the next one.
    """
    by_strategy: Dict[str, List[dict]] = defaultdict(list)
    for bar in bars:
        by_strategy[bar["strategy_table_name"]].append(bar)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for stn, strategy_bars in by_strategy.items():
        strategy_bars.sort(key=lambda b: b["ts"])

        anchor_pnl, anchor_open_price, active_position = anchors.get(stn, (0.0, 0.0, 0.0))
        strategy_rows = []

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

                strategy_rows.append([
                    stn, bar["strategy_id"], bar["strategy_name"],
                    bar["underlying"], bar["config_timeframe"],
                    source_label, "v2", ts_str, cpnl,
                    bar["bar_benchmark"], position, live_open_price,
                    bar["final_signal"], bar["weighting"], now_str,
                ])

                # Advance for next minute
                anchor_pnl = cpnl
                anchor_open_price = live_open_price

        yield stn, strategy_rows


def compute_real_trade_pnl(
    bars: List[dict],
    anchors: Dict[str, Tuple[float, float]],
    prices: Dict[str, float],
) -> List[list]:
    """Compute anchor-chained PnL for real_trade bars. Expand to 1-min intervals.

    Same logic as compute_prod_pnl but bars are sorted by execution_ts (not ts)
    and output includes closing_ts, execution_ts, traded columns.

    Args:
        bars: List of bar dicts (one per revision, sorted by strategy, ts, revision_ts)
        anchors: {strategy_table_name: (anchor_pnl, anchor_price, anchor_position)}
        prices: {ts_string: open_price}

    Returns: List of rows ready for INSERT (REAL_TRADE_INSERT_COLUMNS order)
    """
    by_strategy: Dict[str, List[dict]] = defaultdict(list)
    for bar in bars:
        by_strategy[bar["strategy_table_name"]].append(bar)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_rows = []

    for stn, strategy_bars in by_strategy.items():
        strategy_bars.sort(key=lambda b: b["execution_ts"])

        anchor_pnl, anchor_open_price, active_position = anchors.get(stn, (0.0, 0.0, 0.0))

        for bar in strategy_bars:
            tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
            exec_ts = datetime.strptime(bar["execution_ts"], "%Y-%m-%d %H:%M:%S")
            position = bar["position"]
            bar_price = bar["bar_price"]

            for n in range(tf_minutes):
                ts_1min = exec_ts + timedelta(minutes=n)
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
                    "real_trade", "v2", ts_str, cpnl,
                    bar["bar_benchmark"], position, live_open_price,
                    bar["final_signal"], bar["weighting"], now_str,
                    bar["closing_ts"], bar["execution_ts"], "false",
                ])

                anchor_pnl = cpnl
                anchor_open_price = live_open_price

    return output_rows


def compute_bt_pnl(
    bars: List[dict],
    prices: Dict[str, float],
) -> List[list]:
    """Expand bt bars to 1-min intervals starting from execution_ts (bar close).

    The backtester executes at bar close (execution_ts = ts + tf_minutes).
    cumulative_pnl from row_json is the anchor at execution_ts. From there,
    PnL is recomputed minute-by-minute using live prices until the next bar's
    execution_ts.

    Formula per minute:
        cpnl = anchor_pnl + position * (live_price - anchor_price) / anchor_price
    """
    by_strategy: Dict[str, List[dict]] = defaultdict(list)
    for bar in bars:
        by_strategy[bar["strategy_table_name"]].append(bar)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_rows = []

    for stn, strategy_bars in by_strategy.items():
        strategy_bars.sort(key=lambda b: b["execution_ts"])

        for i, bar in enumerate(strategy_bars):
            exec_ts = datetime.strptime(bar["execution_ts"], "%Y-%m-%d %H:%M:%S")
            # Hold position until next bar's execution_ts (or tf_minutes if last bar)
            if i + 1 < len(strategy_bars):
                next_exec_ts = datetime.strptime(strategy_bars[i + 1]["execution_ts"], "%Y-%m-%d %H:%M:%S")
            else:
                tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
                next_exec_ts = exec_ts + timedelta(minutes=tf_minutes)

            anchor_pnl = bar["cumulative_pnl"]
            anchor_price = prices.get(bar["execution_ts"], bar["bar_price"])

            ts_cur = exec_ts
            while ts_cur < next_exec_ts:
                ts_str = ts_cur.strftime("%Y-%m-%d %H:%M:%S")
                live_price = prices.get(ts_str, anchor_price)

                if anchor_price != 0.0:
                    cpnl = anchor_pnl + bar["position"] * (live_price - anchor_price) / anchor_price
                else:
                    cpnl = anchor_pnl

                output_rows.append([
                    stn, bar["strategy_id"], bar["strategy_name"],
                    bar["underlying"], bar["config_timeframe"],
                    "backtest", "v2", ts_str,
                    cpnl,
                    bar["bar_benchmark"], bar["position"], live_price,
                    bar["final_signal"], bar["weighting"], now_str,
                ])

                ts_cur += timedelta(minutes=1)

    return output_rows
