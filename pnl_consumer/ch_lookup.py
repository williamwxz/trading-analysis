import json
from dataclasses import dataclass
from datetime import datetime

from trading_dagster.utils.clickhouse_client import query_dicts

_LOOKBACK = "1 DAY"


@dataclass
class StrategyBar:
    strategy_table_name: str
    strategy_id: int
    strategy_name: str
    underlying: str
    config_timeframe: str
    weighting: float
    position: float
    final_signal: float
    benchmark: float


@dataclass
class StrategyRevision:
    strategy_table_name: str
    strategy_id: int
    strategy_name: str
    underlying: str
    config_timeframe: str
    weighting: float
    position: float
    final_signal: float
    benchmark: float
    revision_ts: datetime
    closing_ts: datetime


@dataclass
class BtStrategyBar:
    strategy_table_name: str
    strategy_id: int
    strategy_name: str
    underlying: str
    config_timeframe: str
    weighting: float
    position: float
    final_signal: float
    benchmark: float
    cumulative_pnl: float


def fetch_strategies_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyBar]:
    """Return latest strategy bar per strategy for instrument at candle_ts.

    Uses LIMIT 1 BY to get the most recent bar per strategy without FINAL.
    Looks back up to _LOOKBACK to handle gaps in strategy data.
    """
    # strategy_output_history_v2 uses short names (BTC, ETH) not BTCUSDT
    underlying = instrument.removesuffix("USDT")
    ts_str = candle_ts.strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    weighting,
    max(ts) AS latest_ts,
    argMin(row_json, revision_ts) AS row_json
FROM analytics.strategy_output_history_v2
WHERE underlying = '{underlying}'
  AND ts <= '{ts_str}'
  AND ts >= '{ts_str}'::DateTime - INTERVAL {_LOOKBACK}
GROUP BY
    strategy_table_name, strategy_id, strategy_name,
    underlying, config_timeframe, weighting
ORDER BY strategy_table_name, latest_ts DESC
LIMIT 1 BY strategy_table_name
"""
    rows = query_dicts(sql)
    result = []
    for row in rows:
        rj = json.loads(row["row_json"])
        result.append(
            StrategyBar(
                strategy_table_name=row["strategy_table_name"],
                strategy_id=row["strategy_id"],
                strategy_name=row["strategy_name"],
                underlying=row["underlying"],
                config_timeframe=row["config_timeframe"],
                weighting=row["weighting"],
                position=float(rj.get("position", 0.0)),
                final_signal=float(rj.get("final_signal", 0.0)),
                benchmark=float(rj.get("benchmark", 0.0)),
            )
        )
    return result


def fetch_bt_strategies_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[BtStrategyBar]:
    """Return latest bt strategy bar per strategy for instrument at candle_ts.

    cumulative_pnl is extracted from row_json — no anchor-chain computation needed.
    """
    underlying = instrument.removesuffix("USDT")
    ts_str = candle_ts.strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    weighting,
    argMin(row_json, revision_ts) AS row_json
FROM analytics.strategy_output_history_bt_v2
WHERE underlying = '{underlying}'
  AND ts = toDateTime('{ts_str}')
GROUP BY
    strategy_table_name, strategy_id, strategy_name,
    underlying, config_timeframe, weighting
ORDER BY strategy_table_name
"""
    rows = query_dicts(sql)
    result = []
    for row in rows:
        rj = json.loads(row["row_json"])
        result.append(
            BtStrategyBar(
                strategy_table_name=row["strategy_table_name"],
                strategy_id=row["strategy_id"],
                strategy_name=row["strategy_name"],
                underlying=row["underlying"],
                config_timeframe=row["config_timeframe"],
                weighting=row["weighting"],
                position=float(rj.get("position", 0.0)),
                final_signal=float(rj.get("final_signal", 0.0)),
                benchmark=float(rj.get("benchmark", 0.0)),
                cumulative_pnl=float(rj.get("cumulative_pnl", 0.0)),
            )
        )
    return result


def fetch_real_trade_revisions_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyRevision]:
    """Return all revisions for real_trade strategies at candle_ts.

    Only revisions where revision_ts <= closing_ts are included.
    Revisions are ordered by revision_ts ASC (one per position update within the bar).
    """
    underlying = instrument.removesuffix("USDT")
    ts_str = candle_ts.strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    weighting,
    revision_ts,
    closing_ts,
    row_json
FROM (
    SELECT
        strategy_table_name,
        strategy_id,
        strategy_name,
        underlying,
        config_timeframe,
        weighting,
        revision_ts,
        updated_at,
        row_json,
        toDateTime(toDateTime(ts) + toIntervalMinute(multiIf(
            config_timeframe = '1m',  1,
            config_timeframe = '3m',  3,
            config_timeframe = '5m',  5,
            config_timeframe = '15m', 15,
            config_timeframe = '30m', 30,
            config_timeframe = '1h',  60,
            config_timeframe = '4h',  240,
            config_timeframe = '1d',  1440,
            5
        ))) AS closing_ts
    FROM analytics.strategy_output_history_v2
    WHERE underlying = '{underlying}'
      AND toDateTime(ts) = toDateTime('{ts_str}')
    ORDER BY strategy_table_name, config_timeframe, revision_ts, updated_at DESC
    LIMIT 1 BY strategy_table_name, config_timeframe, revision_ts
)
WHERE revision_ts <= closing_ts
ORDER BY strategy_table_name, revision_ts
"""
    rows = query_dicts(sql)
    result = []
    for row in rows:
        rj = json.loads(row["row_json"])
        result.append(
            StrategyRevision(
                strategy_table_name=row["strategy_table_name"],
                strategy_id=row["strategy_id"],
                strategy_name=row["strategy_name"],
                underlying=row["underlying"],
                config_timeframe=row["config_timeframe"],
                weighting=row["weighting"],
                position=float(rj.get("position", 0.0)),
                final_signal=float(rj.get("final_signal", 0.0)),
                benchmark=float(rj.get("benchmark", 0.0)),
                revision_ts=row["revision_ts"],
                closing_ts=row["closing_ts"],
            )
        )
    return result
