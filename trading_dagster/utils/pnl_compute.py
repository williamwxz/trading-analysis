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


def _parse_ts(s: str) -> datetime:
    """Parse a datetime string with or without fractional seconds."""
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")

# Partition start dates — strategies whose first bar falls on this date are
# allowed to cold-start with no prior anchor (genuinely the first data point).
PROD_REAL_TRADE_START_DATE = "2026-02-27"
BT_START_DATE = "2020-06-14"

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


def fetch_strategy_start_dates(source_table: str) -> Dict[str, str]:
    """Return {strategy_table_name: first_bar_date_str} from the source table.

    Used by assert_anchors_present to allow cold-starts only on a strategy's
    actual first bar, rather than a hardcoded global start date.
    Result is a YYYY-MM-DD prefix so callers can do ts.startswith(date).
    """
    sql = f"""\
SELECT strategy_table_name, toString(toDate(min(ts))) AS first_date
FROM analytics.{source_table}
WHERE strategy_table_name NOT LIKE 'manual_probe%'
GROUP BY strategy_table_name
"""
    rows = query_dicts(sql)
    return {r["strategy_table_name"]: str(r["first_date"]) for r in rows}


def assert_anchors_present(
    anchors: Dict[str, Tuple[float, float, float]],
    bars: List[dict],
    source_table: str,
    bar_ts_key: str = "ts",
) -> None:
    """Raise if any strategy is missing an anchor, unless today is its very first bar.

    Queries the source table for each strategy's first-bar date dynamically,
    so no hardcoded start date constant is needed.

    Args:
        anchors: result of fetch_anchors()
        bars: bars about to be computed (must have strategy_table_name + bar_ts_key)
        source_table: source table name (without schema prefix) — used to look up first-bar dates
        bar_ts_key: dict key for the bar timestamp ("ts" for prod/bt, "execution_ts" for real_trade)
    """
    earliest: Dict[str, str] = {}
    for bar in bars:
        stn = bar["strategy_table_name"]
        ts = bar[bar_ts_key]
        if stn not in earliest or ts < earliest[stn]:
            earliest[stn] = ts

    missing_stns = [
        stn for stn, ts in earliest.items()
        if stn not in anchors and not stn.startswith("manual_probe_")
    ]
    if not missing_stns:
        return

    start_dates = fetch_strategy_start_dates(source_table)
    bad = [
        stn for stn in missing_stns
        if not earliest[stn].startswith(start_dates.get(stn, ""))
    ]
    if bad:
        raise RuntimeError(
            f"Missing PnL anchor for strategies: {bad}. "
            f"This would silently reset cumulative_pnl to zero. "
            f"Check that the previous partition ran successfully and has committed rows in the target table."
        )


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
    source_table: str, underlying: str, since: str, ts_end: str | None = None,
) -> List[dict]:
    """Fetch new bars for bt: argMin(row_json, revision_ts) and extract cumulative_pnl.

    execution_ts = ts + tf_minutes (bar close), which is when the position takes effect
    and PnL starts being counted.

    Daily path: since=start_ts, ts_end=end_ts — filter on ts in [since, ts_end).
    Full-recompute path: since=chunk_start, ts_end=None — filter on revision_ts >= since.
    """
    if ts_end is not None:
        ts_filter = f"toDateTime(ts) >= toDateTime('{since}') AND toDateTime(ts) < toDateTime('{ts_end}')"
    else:
        ts_filter = f"revision_ts >= toDateTime('{since}')"
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
  AND {ts_filter}
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
        bar_ts = _parse_ts(r["ts"])
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
    source_table: str, underlying: str, since: str, ts_end: str | None = None,
) -> List[dict]:
    """Fetch real_trade revisions for bars in the given window.

    Daily path: since=start_ts, ts_end=end_ts — filter on bar ts in [since, ts_end).
    Full-recompute path: since=chunk_start, ts_end=None — filter on revision_ts >= since.

    Each revision includes next_bar_closing_ts (the closing time of the *following*
    bar for that strategy). compute_real_trade_pnl uses this to decide whether to
    accept or discard the revision: accepted if revision_ts < next_bar_closing_ts,
    discarded otherwise (position from the previous accepted revision holds instead).
    When there is no next bar, next_bar_closing_ts equals closing_ts (sentinel).
    """
    tf_expr = """multiIf(
        config_timeframe = '5m', 5, config_timeframe = '10m', 10,
        config_timeframe = '15m', 15, config_timeframe = '30m', 30,
        config_timeframe = '1h', 60, config_timeframe = '4h', 240,
        config_timeframe = '1d', 1440, 5
    )"""
    if ts_end is not None:
        ts_filter = f"toDateTime(ts) >= toDateTime('{since}') AND toDateTime(ts) < toDateTime('{ts_end}')"
        nb_ts_filter = ts_filter  # same window for the distinct-bar subquery
    else:
        ts_filter = f"revision_ts >= toDateTime('{since}')"
        nb_ts_filter = f"toDateTime(ts) >= toDateTime('{since}') - INTERVAL 1 DAY"
    sql = f"""\
SELECT
    r.strategy_table_name,
    r.strategy_id, r.strategy_name, r.underlying, r.config_timeframe, r.weighting,
    toString(r.ts) AS ts,
    toString(r.ts + toIntervalMinute({tf_expr})) AS closing_ts,
    toString(toStartOfMinute(r.revision_ts + INTERVAL 59 SECOND)) AS execution_ts,
    toString(r.revision_ts) AS revision_ts,
    toString(nb.next_bar_ts + toIntervalMinute({tf_expr})) AS next_bar_closing_ts,
    JSONExtractFloat(r.row_json, 'position') AS position,
    JSONExtractFloat(r.row_json, 'price') AS bar_price,
    JSONExtractFloat(r.row_json, 'final_signal') AS final_signal,
    JSONExtractFloat(r.row_json, 'benchmark') AS bar_benchmark
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
          AND {nb_ts_filter}
        GROUP BY strategy_table_name, ts
    )
) nb ON r.strategy_table_name = nb.strategy_table_name AND r.ts = nb.ts
WHERE r.underlying = '{underlying}'
  AND r.strategy_table_name NOT LIKE 'manual_probe%'
  AND {ts_filter}
ORDER BY r.strategy_table_name, r.ts, r.revision_ts
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
            "revision_ts": str(r["revision_ts"]),
            "next_bar_closing_ts": str(r["next_bar_closing_ts"]),
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
    price_column: str = "open",
) -> Dict[str, Dict[str, float]]:
    """Fetch 1-min prices for multiple underlyings in a single query.

    Returns {underlying: {ts_string: price}} so the per-underlying loop can
    slice its own dict without firing a separate ClickHouse query each time.
    This avoids N sequential scans on futures_price_1min (one per underlying)
    which can exceed ClickHouse Cloud's memory limit for large partition runs.

    extend_minutes: extra minutes past ts_max to fetch (default 1440 = 1 day,
    for the live path where ts_max is the last bar open time). Pass 0 for
    the daily partition path where ts_max is already the exclusive end boundary.
    price_column: "open" (default, used by all paths).
    """
    if not underlyings:
        return {}
    instruments = [_underlying_to_instrument(u) for u in underlyings]
    instrument_list = ", ".join(f"'{i}'" for i in instruments)
    extend_clause = f" + toIntervalMinute({extend_minutes})" if extend_minutes > 0 else ""
    sql = f"""\
SELECT instrument, toString(ts), {price_column}
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
    "closing_ts", "execution_ts",
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

        for i, bar in enumerate(strategy_bars):
            tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
            # Position is active from closing_ts (bar close = ts + tf) onward.
            closing_ts = _parse_ts(bar["ts"]) + timedelta(minutes=tf_minutes)
            position = bar["position"]

            # Hold until next bar's closing_ts; last bar holds for tf_minutes.
            if i + 1 < len(strategy_bars):
                next_tf = TIMEFRAME_MAP.get(strategy_bars[i + 1]["config_timeframe"], 5)
                next_closing_ts = _parse_ts(strategy_bars[i + 1]["ts"]) + timedelta(minutes=next_tf)
            else:
                next_closing_ts = closing_ts + timedelta(minutes=tf_minutes)

            ts_cur = closing_ts
            while ts_cur < next_closing_ts:
                ts_str = ts_cur.strftime("%Y-%m-%d %H:%M:%S")

                # Price must come from futures_price_1min (Redpanda → pnl_consumer).
                # Fall back to last known price on gaps; skip until first price arrives.
                live_open_price = prices.get(ts_str, anchor_open_price) if anchor_open_price != 0.0 else prices.get(ts_str)

                if live_open_price is None:
                    # No price from our source yet — skip until we have one.
                    ts_cur += timedelta(minutes=1)
                    continue

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

                anchor_pnl = cpnl
                anchor_open_price = live_open_price
                ts_cur += timedelta(minutes=1)

        yield stn, strategy_rows


def compute_real_trade_pnl(
    bars: List[dict],
    anchors: Dict[str, Tuple[float, float]],
    prices: Dict[str, float],
) -> List[list]:
    """Compute anchor-chained PnL for real_trade bars. Expand to 1-min intervals.

    Acceptance rule: a revision is accepted only if
        revision_ts < next_bar.closing_ts
    A discarded revision is skipped — the previous accepted position continues
    holding until the next accepted revision fires.

    The last accepted revision holds for one extra tf_minutes past its closing_ts
    to cover the window until the next partition's first revision.

    Example — 1h bars arriving ~70min after bar open:
      Bar A ts=00:00, closing=01:00, rev_ts=01:32 → execution_ts=01:33, ACCEPT
        (01:32 < next_bar_closing = bar_B.closing = 02:00)
      Bar B ts=01:00, closing=02:00, rev_ts=02:13 → execution_ts=02:14, ACCEPT
        (02:13 < next_bar_closing = bar_C.closing = 03:00)
      Rev(01:33) holds rows 01:33..02:13; Rev(02:14) holds from 02:14 onward.

      If bar A's rev_ts were 02:12 (>= bar_B.closing 02:00) → DISCARD,
      previous position continues until bar B's revision fires.

    Args:
        bars: revision dicts with keys: strategy_table_name, ts, closing_ts,
              execution_ts, revision_ts, next_bar_closing_ts, position, ...
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
        strategy_bars.sort(key=lambda b: (b["ts"], b["revision_ts"]))

        anchor_pnl, anchor_open_price, active_position = anchors.get(stn, (0.0, 0.0, 0.0))
        # active_rev tracks the currently accepted revision (for metadata on output rows).
        active_rev = None

        # Build the accepted list first, then expand to 1-min rows.
        # A revision is accepted if revision_ts < next_bar_closing_ts.
        # next_bar_closing_ts == closing_ts means no next bar (sentinel from SQL LEAD).
        accepted: List[dict] = []
        for rev in strategy_bars:
            no_next_bar = rev["next_bar_closing_ts"] == rev["closing_ts"]
            if no_next_bar or rev["revision_ts"] < rev["next_bar_closing_ts"]:
                accepted.append(rev)

        for i, rev in enumerate(accepted):
            exec_ts = _parse_ts(rev["execution_ts"])

            if i + 1 < len(accepted):
                end_ts = _parse_ts(accepted[i + 1]["execution_ts"])
            else:
                tf_minutes = TIMEFRAME_MAP.get(rev["config_timeframe"], 5)
                end_ts = _parse_ts(rev["closing_ts"]) + timedelta(minutes=tf_minutes)

            ts_cur = exec_ts
            while ts_cur < end_ts:
                ts_str = ts_cur.strftime("%Y-%m-%d %H:%M:%S")
                live_open_price = prices.get(ts_str, anchor_open_price) if anchor_open_price != 0.0 else prices.get(ts_str)

                if live_open_price is None:
                    ts_cur += timedelta(minutes=1)
                    continue

                if anchor_open_price == 0.0:
                    anchor_open_price = live_open_price

                if anchor_open_price != 0.0:
                    cpnl = anchor_pnl + rev["position"] * (live_open_price - anchor_open_price) / anchor_open_price
                else:
                    cpnl = anchor_pnl

                output_rows.append([
                    stn, rev["strategy_id"], rev["strategy_name"],
                    rev["underlying"], rev["config_timeframe"],
                    "real_trade", "v2", ts_str, cpnl,
                    rev["bar_benchmark"], rev["position"], live_open_price,
                    rev["final_signal"], rev["weighting"], now_str,
                    rev["closing_ts"], rev["execution_ts"],
                ])

                anchor_pnl = cpnl
                anchor_open_price = live_open_price
                ts_cur += timedelta(minutes=1)

    return output_rows


def compute_bt_pnl(
    bars: List[dict],
    prices: Dict[str, float],
    anchors: Dict[str, Tuple[float, float, float]] | None = None,
) -> List[list]:
    """Expand bt bars to 1-min intervals starting from execution_ts (bar close).

    The backtester executes at bar close (execution_ts = ts + tf_minutes).

    Anchor priority per strategy:
      1. anchors dict (previous day's tail from target table) — used when present
      2. bar["cumulative_pnl"] from source JSON — cold-start only for the very
         first bar of a strategy's history (no prior anchor in target table)

    All subsequent bars within the same call chain from the last computed minute,
    so an upstream backtest rerun that changes cumulative_pnl absolutely cannot
    create discontinuities in our 1-min series.

    Formula per minute:
        cpnl = anchor_pnl + position * (live_price - anchor_price) / anchor_price
    """
    if anchors is None:
        anchors = {}
    by_strategy: Dict[str, List[dict]] = defaultdict(list)
    for bar in bars:
        by_strategy[bar["strategy_table_name"]].append(bar)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_rows = []

    for stn, strategy_bars in by_strategy.items():
        strategy_bars.sort(key=lambda b: b["execution_ts"])

        # Seed from cross-day anchor if available; otherwise use source cumulative_pnl
        # on the very first bar (genuine cold-start for this strategy's history).
        if stn in anchors:
            running_anchor_pnl: float | None = anchors[stn][0]
            running_anchor_price: float | None = anchors[stn][1]
        else:
            running_anchor_pnl = None
            running_anchor_price = None

        for i, bar in enumerate(strategy_bars):
            exec_ts = _parse_ts(bar["execution_ts"])
            # Hold position until next bar's execution_ts (or tf_minutes if last bar)
            if i + 1 < len(strategy_bars):
                next_exec_ts = _parse_ts(strategy_bars[i + 1]["execution_ts"])
            else:
                tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
                next_exec_ts = exec_ts + timedelta(minutes=tf_minutes)

            if running_anchor_pnl is None:
                # Genuine cold-start: seed from source JSON cumulative_pnl.
                # Anchor price must come from our pricing source only.
                anchor_pnl = bar["cumulative_pnl"]
                anchor_price = prices.get(bar["execution_ts"])
            else:
                anchor_pnl = running_anchor_pnl
                anchor_price = running_anchor_price

            ts_cur = exec_ts
            while ts_cur < next_exec_ts:
                ts_str = ts_cur.strftime("%Y-%m-%d %H:%M:%S")
                live_price = prices.get(ts_str, anchor_price) if anchor_price is not None else prices.get(ts_str)

                if live_price is None:
                    ts_cur += timedelta(minutes=1)
                    continue

                if anchor_price is None:
                    anchor_price = live_price

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

                anchor_pnl = cpnl
                anchor_price = live_price
                ts_cur += timedelta(minutes=1)

            running_anchor_pnl = anchor_pnl
            running_anchor_price = anchor_price

    return output_rows
