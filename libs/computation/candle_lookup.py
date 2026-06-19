"""Live-candle strategy lookups for the PnL streaming consumer.

These functions re-query strategy_output_history_* on every candle so late-arriving
bars are picked up automatically. Position is always read from the history tables;
never from the PnL result tables.
"""

import json
from dataclasses import dataclass
from datetime import datetime

from libs.clickhouse_client import query_dicts
from libs.computation.pnl_formula import parse_strategy_table_name

_BT_STREAM_LOOKBACK = "3 DAY"

# 2-day lookback: a 1d bar stays the active bar up to ~2 days after its ts
# (execution_ts ≈ next midnight, held until the following day's bar). A 1-day
# window dropped 1d strategies from the live stream. argMax / LIMIT 1 BY still
# pick the latest bar, so widening only grows the candidate set — safe for all tf.
_LOOKBACK = "2 DAY"

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
class StrategyBar:
    """Active bar for prod/bt: first revision, closing_ts gate applied."""

    strategy_table_name: str
    strategy_instance_id: str
    strategy_id: int
    strategy_name: str
    underlying: str
    config_timeframe: str
    weighting: float
    position: float
    final_signal: float
    benchmark: float
    bar_ts: datetime
    # bt-only: row_json's authoritative cumulative_pnl at the bar boundary. The
    # bt consumer resets its anchor to this value on each new bar so the live
    # chain matches the offline compute_bt_pnl. Prod leaves it at the 0.0 default
    # (never read by the prod branch).
    cumulative_pnl: float = 0.0


@dataclass
class StrategyRevision:
    """Active revision for real_trade: latest revision_ts <= candle_ts."""

    strategy_table_name: str
    strategy_instance_id: str
    strategy_id: int
    strategy_name: str
    underlying: str
    config_timeframe: str
    weighting: float
    position: float
    final_signal: float
    benchmark: float
    bar_ts: datetime  # strategy_output_history_v2.ts (bar open time)
    revision_ts: datetime


def _parse_strategy_bar(row: dict) -> StrategyBar:
    rj = json.loads(row["row_json"])
    return StrategyBar(
        strategy_table_name=row["strategy_table_name"],
        strategy_instance_id=row["strategy_instance_id"],
        strategy_id=row["strategy_id"],
        strategy_name=row["strategy_name"],
        underlying=row["underlying"],
        config_timeframe=row["config_timeframe"],
        weighting=row["weighting"],
        position=float(rj.get("position", 0.0)),
        final_signal=float(rj.get("final_signal", 0.0)),
        benchmark=float(rj.get("benchmark", 0.0)),
        bar_ts=row["latest_ts"],
    )


def _parse_revision(row: dict) -> StrategyRevision:
    rj = json.loads(row["row_json"])
    return StrategyRevision(
        strategy_table_name=row["strategy_table_name"],
        strategy_instance_id=row["strategy_instance_id"],
        strategy_id=row["strategy_id"],
        strategy_name=row["strategy_name"],
        underlying=row["underlying"],
        config_timeframe=row["config_timeframe"],
        weighting=row["weighting"],
        position=float(rj.get("position", 0.0)),
        final_signal=float(rj.get("final_signal", 0.0)),
        benchmark=float(rj.get("benchmark", 0.0)),
        bar_ts=row["bar_ts"],
        revision_ts=row["max_revision_ts"],
    )


def fetch_strategies_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyBar]:
    """Return active prod bar per strategy_instance_id for instrument at candle_ts.

    Two steps:
      1. Inner query: per strategy_instance_id, find the *latest* bar whose
         closing_ts (= ts + tf_minutes) <= candle_ts, within the lookback window.
      2. Outer query: for that latest bar only, take its *first* revision
         (argMin by revision_ts).

    The two steps must be kept separate. Grouping per strategy and taking
    argMin(row_json, revision_ts) over the whole lookback window is WRONG: it
    returns the row_json of the oldest revision in the window (a bar up to
    `_LOOKBACK` old), not the first revision of the latest bar — so a stale
    position leaks in while latest_ts points at the current bar.
    """
    underlying = instrument.removesuffix("USDT")
    ts_str = candle_ts.strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_instance_id,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    weighting,
    ts AS latest_ts,
    argMin(row_json, revision_ts) AS row_json
FROM analytics.strategy_output_history_v2
WHERE underlying = '{underlying}'
  AND (strategy_instance_id, ts) IN (
      SELECT strategy_instance_id, max(ts)
      FROM analytics.strategy_output_history_v2
      WHERE underlying = '{underlying}'
        AND ts + toIntervalMinute({_TF_MINUTES_EXPR_NO_ALIAS}) <= '{ts_str}'
        AND ts >= '{ts_str}'::DateTime - INTERVAL {_LOOKBACK}
      GROUP BY strategy_instance_id
  )
GROUP BY
    strategy_table_name, strategy_instance_id, strategy_id, strategy_name,
    underlying, config_timeframe, weighting, ts
"""
    return [_parse_strategy_bar(r) for r in query_dicts(sql)]


@dataclass
class BtLiveAnchor:
    """Resolved BT anchor for one strategy at a live candle (stateless compute)."""

    strategy_table_name: str
    strategy_instance_id: str
    strategy_id: int
    strategy_name: str
    underlying: str
    config_timeframe: str
    weighting: float
    cum_pnl_first: float
    pos_first: float
    anchor_ts: str
    anchor_price: float
    benchmark: float


def fetch_bt_anchors_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[BtLiveAnchor]:
    """Latest cum-table anchor per strategy with ts <= candle_ts, fully resolved.

    Resolves anchor_price from futures_price_1min at anchor_ts and benchmark from
    strategy_output_history_bt_v2 at anchor_ts. Stateless: the consumer computes
    cpnl = compute_bt_live_cpnl(cum_pnl_first, pos_first, candle.open, anchor_price).
    """
    underlying = instrument.removesuffix("USDT")
    ts_str = candle_ts.strftime("%Y-%m-%d %H:%M:%S")
    like = f"%|u={underlying}|%"
    anchor_sql = f"""\
SELECT
    strategy_table_name,
    config_timeframe,
    toString(ts)  AS anchor_ts,
    cum_pnl_first,
    pos_first,
    weighting
FROM analytics.strategy_cum_pnl_bt_v2
WHERE strategy_table_name LIKE '{like}'
  AND ts <= '{ts_str}'
  AND ts >  '{ts_str}'::DateTime - INTERVAL {_BT_STREAM_LOOKBACK}
ORDER BY strategy_table_name, ts DESC, computed_at DESC
LIMIT 1 BY strategy_table_name
"""
    anchor_rows = query_dicts(anchor_sql)
    if not anchor_rows:
        return []

    anchor_ts_set = {str(r["anchor_ts"]) for r in anchor_rows}
    ts_in = ", ".join(f"'{t}'" for t in sorted(anchor_ts_set))

    price_sql = f"""\
SELECT toString(ts) AS ts, open
FROM analytics.futures_price_1min
WHERE exchange = 'binance' AND instrument = '{instrument}'
  AND ts IN ({ts_in})
"""
    price_map = {r["ts"]: float(r["open"]) for r in query_dicts(price_sql)}

    bench_sql = f"""\
SELECT strategy_table_name, toString(ts) AS ts,
       argMin(JSONExtractFloat(row_json, 'benchmark'), revision_ts) AS benchmark
FROM analytics.strategy_output_history_bt_v2
WHERE underlying = '{underlying}'
  AND ts IN ({ts_in})
GROUP BY strategy_table_name, ts
"""
    bench_map = {
        (r["strategy_table_name"], str(r["ts"])): float(r["benchmark"])
        for r in query_dicts(bench_sql)
    }

    out: list[BtLiveAnchor] = []
    for r in anchor_rows:
        stn = r["strategy_table_name"]
        anchor_ts = str(r["anchor_ts"])
        sid, name, u, siid = parse_strategy_table_name(stn)
        out.append(
            BtLiveAnchor(
                strategy_table_name=stn,
                strategy_instance_id=siid,
                strategy_id=sid,
                strategy_name=name,
                underlying=u,
                config_timeframe=str(r["config_timeframe"]),
                weighting=float(r["weighting"]),
                cum_pnl_first=float(r["cum_pnl_first"]),
                pos_first=float(r["pos_first"]),
                anchor_ts=anchor_ts,
                anchor_price=price_map.get(anchor_ts, 0.0),
                benchmark=bench_map.get((stn, anchor_ts), 0.0),
            )
        )
    return out


def fetch_real_trade_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyRevision]:
    """Return the latest revision per strategy_instance_id where revision_ts <= candle_ts.

    No closing_ts gate — a revision becomes active as soon as revision_ts <= candle_ts,
    regardless of whether the bar has closed. This reflects real-trade semantics: positions
    change the moment a revision is written to strategy_output_history_v2.

    The caller applies the AnchorState revision guard:
        apply only if (bar_ts, revision_ts) > (anchor.bar_ts, anchor.revision_ts)
    This prevents stale late revisions for an old bar from overwriting a newer bar's position.

    bar_ts = strategy_output_history_v2.ts (bar open time, NOT closing_ts or revision_ts).
    """
    underlying = instrument.removesuffix("USDT")
    ts_str = candle_ts.strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_instance_id,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    weighting,
    argMax(ts, (ts, revision_ts))           AS bar_ts,
    argMax(revision_ts, (ts, revision_ts))  AS max_revision_ts,
    argMax(row_json, (ts, revision_ts))     AS row_json
FROM analytics.strategy_output_history_v2
PREWHERE underlying = '{underlying}'
WHERE ts >= '{ts_str}'::DateTime - INTERVAL {_LOOKBACK}
  AND ts <= '{ts_str}'::DateTime
  AND revision_ts <= '{ts_str}'
GROUP BY
    strategy_table_name, strategy_instance_id, strategy_id, strategy_name,
    underlying, config_timeframe, weighting
"""
    return [_parse_revision(r) for r in query_dicts(sql)]
