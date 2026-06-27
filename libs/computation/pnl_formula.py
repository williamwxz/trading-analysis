"""PnL computation: anchor-chained 1-min expansion for prod, bt, and real_trade.

No I/O — takes pre-fetched bars and prices dicts. Both pnl_consumer (price from
Redpanda candle.open) and the batch recompute (price from futures_price_1min via ClickHouse)
pass prices into these functions; the source of price is the only difference
between the two callers.

Formula:
    cumulative_pnl = anchor_pnl + position * (current_price - anchor_price) / anchor_price
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, Generator, List, Tuple, Union

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
# Indices: ts=7, updated_at=14 — used by _prepare_rows_for_clickhouse in the batch recompute.
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


@dataclass
class BtAnchor:
    """One cum-table anchor: per (strategy, native-timeframe bar) cumulative PnL."""
    strategy_table_name: str
    config_timeframe: str
    ts: str            # bar open time, "YYYY-MM-DD HH:MM:SS"
    cum_pnl_first: float
    pos_first: float
    weighting: float


def parse_strategy_table_name(stn: str) -> Tuple[int, str, str, str]:
    """Parse 'sid=N|sno=N|u=X|name=Y|inst=Z' into
    (strategy_id, strategy_name, underlying, strategy_instance_id).

    Pipe-delimited key=value tokens. 'name' values never contain a pipe
    (verified against live data), so a plain split is safe. Missing keys
    default to (0, '', '', '').
    """
    parts: Dict[str, str] = {}
    for tok in stn.split("|"):
        key, sep, val = tok.partition("=")
        if sep:
            parts[key] = val
    try:
        strategy_id = int(parts.get("sid", ""))
    except ValueError:
        strategy_id = 0
    return (
        strategy_id,
        parts.get("name", ""),
        parts.get("u", ""),
        parts.get("inst", ""),
    )


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
    the required type (batch recompute: _prepare_rows_for_clickhouse converts str→datetime;
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


def compute_bt_live_cpnl(
    cum_pnl_first: float,
    pos_first: float,
    current_price: float,
    anchor_price: float,
) -> float:
    """Anchor-reset cumulative PnL for one minute.

    cpnl = cum_pnl_first + pos_first * (current_price - anchor_price) / anchor_price
    Holds cum_pnl_first when anchor_price is 0 (no reference price available).
    """
    if anchor_price == 0.0:
        return cum_pnl_first
    return cum_pnl_first + pos_first * (current_price - anchor_price) / anchor_price


def compute_bt_pnl(
    anchors: List[BtAnchor],
    seed_anchors: Dict[str, Tuple[float, float]],
    prices: Dict[str, float],
    benchmarks: Dict[Tuple[str, str], float],
    window_start: str,
    window_end: str,
) -> List[list]:
    """Minute-chain cum-table bars to 1-min rows over [window_start, window_end).

    The active bar at minute m is the most-recent BtAnchor with ts <= m; its
    pos_first is the position held that minute. PnL chains minute to minute:
        cpnl(m) = cpnl(m-1) + pos(m) * (price(m) - price(m-1)) / price(m-1)

    seed_anchors[stn] = (cpnl_prev, price_prev) is the target-table row just before
    window_start (warm start). A strategy absent from seed_anchors (or with
    price_prev == 0) cold-starts: cpnl seeds from the first active bar's
    cum_pnl_first and the first emitted minute holds that value, establishing the
    price reference. Missing price at minute m reuses the carried price (cpnl
    unchanged) — identical to iter_compute_prod_pnl.
    """
    by_strategy: Dict[str, List[BtAnchor]] = defaultdict(list)
    for a in anchors:
        by_strategy[a.strategy_table_name].append(a)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_dt = _parse_ts(window_start)
    end_dt = _parse_ts(window_end)
    output: List[list] = []

    for stn, strat_anchors in by_strategy.items():
        strat_anchors.sort(key=lambda a: a.ts)
        ts_list = [a.ts for a in strat_anchors]
        strategy_id, strategy_name, underlying, siid = parse_strategy_table_name(stn)

        seed = seed_anchors.get(stn)
        if seed is not None and seed[1] != 0.0:
            anchor_pnl, anchor_price = seed
        else:
            anchor_pnl = strat_anchors[0].cum_pnl_first
            anchor_price = 0.0

        first_anchor_dt = _parse_ts(strat_anchors[0].ts)
        m = max(start_dt, first_anchor_dt).replace(second=0, microsecond=0)
        idx = 0
        while m < end_dt:
            m_str = m.strftime("%Y-%m-%d %H:%M:%S")
            while idx + 1 < len(ts_list) and ts_list[idx + 1] <= m_str:
                idx += 1
            live_price = (
                prices.get(m_str, anchor_price)
                if anchor_price != 0.0
                else prices.get(m_str)
            )
            if live_price is None:
                m += timedelta(minutes=1)
                continue
            if anchor_price == 0.0:
                anchor_price = live_price
            anchor = strat_anchors[idx]
            position = anchor.pos_first
            cpnl = (
                anchor_pnl + position * (live_price - anchor_price) / anchor_price
                if anchor_price != 0.0
                else anchor_pnl
            )
            output.append([
                stn,
                strategy_id,
                strategy_name,
                underlying,
                anchor.config_timeframe,
                "backtest",
                "v2",
                m_str,
                cpnl,
                benchmarks.get((stn, anchor.ts), 0.0),
                position,
                live_price,
                0.0,
                anchor.weighting,
                now_str,
                siid,
            ])
            anchor_pnl = cpnl
            anchor_price = live_price
            m += timedelta(minutes=1)

    return output


def compute_real_trade_pnl(
    bars: List[dict],
    anchors: Dict[str, Tuple[float, float, float]],
    prices: Dict[str, float],
) -> List[list]:
    """Expand real_trade revisions to 1-min rows.

    Acceptance rule: mirrors AnchorState.should_apply_revision in the pnl_consumer.
    A revision is accepted iff (bar_ts, revision_ts) > (prev_accepted.ts, prev_accepted.revision_ts).
    Revisions are processed in (ts, revision_ts) ascending order; any revision that is
    not strictly greater than the last accepted one is discarded (duplicate or stale).
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
        last_bar_ts: str = ""
        last_revision_ts: str = ""
        for rev in strategy_bars:
            if (rev["ts"], rev["revision_ts"]) > (last_bar_ts, last_revision_ts):
                accepted.append(rev)
                last_bar_ts = rev["ts"]
                last_revision_ts = rev["revision_ts"]

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

    Used by the batch recompute's chunk loop to seed the next chunk's anchor from the last row
    of the previous chunk. Indices: cumulative_pnl=8, price=11, position=10.
    """
    return (float(row[8]), float(row[11]), float(row[10]))
