"""Live-candle strategy lookups for the PnL streaming consumer.

These functions re-query strategy_output_history_* on every candle so late-arriving
bars are picked up automatically. Position is always read from the history tables;
never from the PnL result tables.
"""

import json
from dataclasses import dataclass
from datetime import datetime

from libs.clickhouse_client import query_dicts

_LOOKBACK = "1 DAY"

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
    bar_ts: datetime       # strategy_output_history_v2.ts (bar open time)
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
        revision_ts=row["revision_ts"],
    )


def fetch_strategies_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyBar]:
    """Return active prod bar per strategy_instance_id for instrument at candle_ts.

    Latest bar whose closing_ts (= ts + tf_minutes) <= candle_ts. First revision only
    (argMin by revision_ts). Groups by strategy_instance_id so each logical strategy
    instance is independently tracked.
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
    max(ts) AS latest_ts,
    argMin(row_json, revision_ts) AS row_json
FROM analytics.strategy_output_history_v2
WHERE underlying = '{underlying}'
  AND ts + toIntervalMinute({_TF_MINUTES_EXPR_NO_ALIAS}) <= '{ts_str}'
  AND ts >= '{ts_str}'::DateTime - INTERVAL {_LOOKBACK}
GROUP BY
    strategy_table_name, strategy_instance_id, strategy_id, strategy_name,
    underlying, config_timeframe, weighting
ORDER BY strategy_instance_id, latest_ts DESC
LIMIT 1 BY strategy_instance_id
"""
    return [_parse_strategy_bar(r) for r in query_dicts(sql)]


def fetch_bt_strategies_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyBar]:
    """Return active bt bar per strategy_instance_id for instrument at candle_ts.

    Same closing_ts gate as fetch_strategies_for_candle, but queries _bt_v2.
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
    max(ts) AS latest_ts,
    argMin(row_json, revision_ts) AS row_json
FROM analytics.strategy_output_history_bt_v2
WHERE underlying = '{underlying}'
  AND ts + toIntervalMinute({_TF_MINUTES_EXPR_NO_ALIAS}) <= '{ts_str}'
  AND ts >= '{ts_str}'::DateTime - INTERVAL {_LOOKBACK}
GROUP BY
    strategy_table_name, strategy_instance_id, strategy_id, strategy_name,
    underlying, config_timeframe, weighting
ORDER BY strategy_instance_id, latest_ts DESC
LIMIT 1 BY strategy_instance_id
"""
    return [_parse_strategy_bar(r) for r in query_dicts(sql)]


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
    ts AS bar_ts,
    max(revision_ts) AS revision_ts,
    argMax(row_json, revision_ts) AS row_json
FROM analytics.strategy_output_history_v2
WHERE underlying = '{underlying}'
  AND revision_ts <= '{ts_str}'
  AND ts >= '{ts_str}'::DateTime - INTERVAL {_LOOKBACK}
GROUP BY
    strategy_table_name, strategy_instance_id, strategy_id, strategy_name,
    underlying, config_timeframe, weighting, ts
ORDER BY strategy_instance_id, ts DESC
LIMIT 1 BY strategy_instance_id
"""
    return [_parse_revision(r) for r in query_dicts(sql)]
