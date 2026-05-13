"""PnL computation: anchor-chained 1-min expansion for prod, bt, and real_trade.

No I/O — takes pre-fetched bars and prices dicts. Both pnl_consumer (price from
Redpanda candle.open) and Dagster (price from futures_price_1min via ClickHouse)
pass prices into these functions; the source of price is the only difference
between the two callers.

Formula:
    cumulative_pnl = anchor_pnl + position * (current_price - anchor_price) / anchor_price
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, Generator, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from libs.computation.anchor_state import AnchorRecord

TIMEFRAME_MAP: Dict[str, int] = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}

# Column order for INSERT into strategy_pnl_1min_{prod,bt,real_trade}_v2.
# Indices: ts=7, updated_at=14 — used by _prepare_rows_for_clickhouse in Dagster.
INSERT_COLUMNS = [
    "strategy_table_name",   # 0
    "strategy_id",           # 1
    "strategy_name",         # 2
    "underlying",            # 3
    "config_timeframe",      # 4
    "source",                # 5
    "version",               # 6
    "ts",                    # 7
    "cumulative_pnl",        # 8
    "benchmark",             # 9
    "position",              # 10
    "price",                 # 11
    "final_signal",          # 12
    "weighting",             # 13
    "updated_at",            # 14
    "strategy_instance_id",  # 15
]

# Legacy aliases used by existing callers in pnl_strategy_v2.py and pnl_consumer.py.
PROD_INSERT_COLUMNS = INSERT_COLUMNS
REAL_TRADE_INSERT_COLUMNS = INSERT_COLUMNS


def _parse_ts(s: str) -> datetime:
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")


def build_pnl_row(
    strategy_table_name: str,
    bar: dict,
    price: float,
    cumulative_pnl: float,
    source_label: str,
    ts: Union[str, datetime],
    now: Union[str, datetime],
) -> list:
    """Build one INSERT_COLUMNS-ordered row from a bar dict and pre-computed pnl.

    bar must have: strategy_id, strategy_name, underlying, config_timeframe,
    weighting, strategy_instance_id, final_signal, bar_benchmark, position.
    ts and now accept str or datetime — the insert path for each caller handles
    the required type (Dagster: _prepare_rows_for_clickhouse converts str→datetime;
    pnl_consumer: passes datetime objects directly).
    """
    return [
        strategy_table_name,
        bar["strategy_id"],
        bar["strategy_name"],
        bar["underlying"],
        bar["config_timeframe"],
        source_label,
        "v2",
        ts,
        cumulative_pnl,
        bar["bar_benchmark"],
        bar["position"],
        price,
        bar["final_signal"],
        bar["weighting"],
        now,
        bar.get("strategy_instance_id", ""),
    ]


def build_carry_forward_row(
    strategy_table_name: str,
    rec: AnchorRecord,
    price: float,
    cumulative_pnl: float,
    source_label: str,
    ts: Union[str, datetime],
    now: Union[str, datetime],
) -> list:
    """Build one INSERT_COLUMNS-ordered row from an AnchorRecord for carry-forward.

    Used when there is no active bar — the record supplies all bar-metadata fields
    (populated by the last seen bar). Returns the same column layout as build_pnl_row.
    """
    return [
        strategy_table_name,
        rec.strategy_id,
        rec.strategy_name,
        rec.underlying,
        rec.config_timeframe,
        source_label,
        "v2",
        ts,
        cumulative_pnl,
        rec.benchmark,
        rec.position,
        price,
        rec.final_signal,
        rec.weighting,
        now,
        rec.strategy_instance_id,
    ]


def iter_compute_prod_pnl(
    bars: List[dict],
    anchors: Dict[str, Tuple[float, float, float]],
    prices: Dict[str, float],
    source_label: str = "production",
) -> Generator[Tuple[str, List[list]], None, None]:
    """Yield (strategy_table_name, rows) for each strategy in prod/bt bars.

    bars: sorted by (strategy_table_name, ts). Each bar has:
        strategy_table_name, strategy_id, strategy_name, underlying,
        config_timeframe, weighting, ts (str), position, final_signal,
        bar_benchmark, strategy_instance_id.
    anchors: {strategy_table_name: (anchor_pnl, anchor_price, anchor_position)}
    prices: {ts_str: open_price} — source is caller's choice (Redpanda or ClickHouse).
    """
    by_strategy: Dict[str, List[dict]] = defaultdict(list)
    for bar in bars:
        by_strategy[bar["strategy_table_name"]].append(bar)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for stn, strategy_bars in by_strategy.items():
        strategy_bars.sort(key=lambda b: b["ts"])
        anchor_pnl, anchor_price, _active_pos = anchors.get(stn, (0.0, 0.0, 0.0))
        rows: List[list] = []

        for i, bar in enumerate(strategy_bars):
            tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
            closing_ts = _parse_ts(bar["ts"]) + timedelta(minutes=tf_minutes)
            position = bar["position"]

            if i + 1 < len(strategy_bars):
                next_tf = TIMEFRAME_MAP.get(strategy_bars[i + 1]["config_timeframe"], 5)
                next_closing_ts = _parse_ts(strategy_bars[i + 1]["ts"]) + timedelta(minutes=next_tf)
            else:
                next_closing_ts = closing_ts + timedelta(minutes=tf_minutes)

            ts_cur = closing_ts
            while ts_cur < next_closing_ts:
                ts_str = ts_cur.strftime("%Y-%m-%d %H:%M:%S")
                live_price = (
                    prices.get(ts_str, anchor_price)
                    if anchor_price != 0.0
                    else prices.get(ts_str)
                )
                if live_price is None:
                    ts_cur += timedelta(minutes=1)
                    continue
                if anchor_price == 0.0:
                    anchor_price = live_price
                cpnl = (
                    anchor_pnl + position * (live_price - anchor_price) / anchor_price
                    if anchor_price != 0.0
                    else anchor_pnl
                )
                rows.append([
                    stn,
                    bar["strategy_id"],
                    bar["strategy_name"],
                    bar["underlying"],
                    bar["config_timeframe"],
                    source_label,
                    "v2",
                    ts_str,
                    cpnl,
                    bar["bar_benchmark"],
                    position,
                    live_price,
                    bar["final_signal"],
                    bar["weighting"],
                    now_str,
                    bar.get("strategy_instance_id", ""),
                ])
                anchor_pnl = cpnl
                anchor_price = live_price
                ts_cur += timedelta(minutes=1)

        yield stn, rows


def compute_prod_pnl(
    bars: List[dict],
    anchors: Dict[str, Tuple[float, float, float]],
    prices: Dict[str, float],
    source_label: str = "production",
) -> List[list]:
    """Flatten iter_compute_prod_pnl into a single list."""
    result: List[list] = []
    for _, rows in iter_compute_prod_pnl(bars, anchors, prices, source_label):
        result.extend(rows)
    return result


def compute_bt_pnl(
    bars: List[dict],
    prices: Dict[str, float],
    anchors: Optional[Dict[str, Tuple[float, float, float]]] = None,
) -> List[list]:
    """Expand bt bars to 1-min rows starting from execution_ts (= ts + tf_minutes).

    At the start of each bar, cumulative_pnl resets to bar["cumulative_pnl"] from
    raw_json and prev_price resets to the price at execution_ts. Each minute then rolls:
        cpnl = prev_cpnl + position * (current_price - prev_price) / prev_price
    No cross-bar anchor chaining — the raw_json cumulative_pnl is always authoritative.
    """
    by_strategy: Dict[str, List[dict]] = defaultdict(list)
    for bar in bars:
        by_strategy[bar["strategy_table_name"]].append(bar)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output: List[list] = []

    for stn, strategy_bars in by_strategy.items():
        strategy_bars.sort(key=lambda b: b["execution_ts"])

        for i, bar in enumerate(strategy_bars):
            exec_ts = _parse_ts(bar["execution_ts"])
            if i + 1 < len(strategy_bars):
                next_exec_ts = _parse_ts(strategy_bars[i + 1]["execution_ts"])
            else:
                tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
                next_exec_ts = exec_ts + timedelta(minutes=tf_minutes)

            # Reset to this bar's raw_json cumulative_pnl; price anchors at execution_ts.
            running_pnl: float = bar["cumulative_pnl"]
            running_price: Optional[float] = prices.get(bar["execution_ts"])

            ts_cur = exec_ts
            while ts_cur < next_exec_ts:
                ts_str = ts_cur.strftime("%Y-%m-%d %H:%M:%S")
                live_price = (
                    prices.get(ts_str, running_price)
                    if running_price is not None
                    else prices.get(ts_str)
                )
                if live_price is None:
                    ts_cur += timedelta(minutes=1)
                    continue
                if running_price is None:
                    # First price seen — anchor here, emit with no pnl change.
                    running_price = live_price
                    cpnl = running_pnl
                else:
                    cpnl = (
                        running_pnl + bar["position"] * (live_price - running_price) / running_price
                        if running_price != 0.0
                        else running_pnl
                    )
                output.append([
                    stn,
                    bar["strategy_id"],
                    bar["strategy_name"],
                    bar["underlying"],
                    bar["config_timeframe"],
                    "backtest",
                    "v2",
                    ts_str,
                    cpnl,
                    bar["bar_benchmark"],
                    bar["position"],
                    live_price,
                    bar["final_signal"],
                    bar["weighting"],
                    now_str,
                    bar.get("strategy_instance_id", ""),
                ])
                running_pnl = cpnl
                running_price = live_price
                ts_cur += timedelta(minutes=1)

    return output


def compute_real_trade_pnl(
    bars: List[dict],
    anchors: Dict[str, Tuple[float, float, float]],
    prices: Dict[str, float],
) -> List[list]:
    """Expand real_trade revisions to 1-min rows.

    Acceptance rule: a revision is accepted if revision_ts < next_bar_closing_ts.
    When next_bar_closing_ts == closing_ts (no next bar sentinel), always accepted.
    Accepted revisions expand from their execution_ts until the next accepted
    revision's execution_ts. The last accepted revision holds for tf_minutes past
    its closing_ts.
    """
    by_strategy: Dict[str, List[dict]] = defaultdict(list)
    for bar in bars:
        by_strategy[bar["strategy_table_name"]].append(bar)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output: List[list] = []

    for stn, strategy_bars in by_strategy.items():
        strategy_bars.sort(key=lambda b: (b["ts"], b["revision_ts"]))
        anchor_pnl, anchor_price, _active_pos = anchors.get(stn, (0.0, 0.0, 0.0))

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
                live_price = (
                    prices.get(ts_str, anchor_price)
                    if anchor_price != 0.0
                    else prices.get(ts_str)
                )
                if live_price is None:
                    ts_cur += timedelta(minutes=1)
                    continue
                if anchor_price == 0.0:
                    anchor_price = live_price
                cpnl = (
                    anchor_pnl + rev["position"] * (live_price - anchor_price) / anchor_price
                    if anchor_price != 0.0
                    else anchor_pnl
                )
                output.append([
                    stn,
                    rev["strategy_id"],
                    rev["strategy_name"],
                    rev["underlying"],
                    rev["config_timeframe"],
                    "real_trade",
                    "v2",
                    ts_str,
                    cpnl,
                    rev["bar_benchmark"],
                    rev["position"],
                    live_price,
                    rev["final_signal"],
                    rev["weighting"],
                    now_str,
                    rev.get("strategy_instance_id", ""),
                ])
                anchor_pnl = cpnl
                anchor_price = live_price
                ts_cur += timedelta(minutes=1)

    return output


def extract_row_anchor(row: list) -> Tuple[float, float, float]:
    """Extract (cumulative_pnl, price, position) from a completed INSERT_COLUMNS row.

    Used by Dagster's chunk loop to seed the next chunk's anchor from the last row
    of the previous chunk. Indices: cumulative_pnl=8, price=11, position=10.
    """
    return (float(row[8]), float(row[11]), float(row[10]))
