"""
ClickHouse synchronous lookup for strategy positions.

Queries strategy_output_history_v2 for the most recent bar per strategy
for a given instrument at or before candle_ts. Returns all active strategies.
Called once per closed candle from the PyFlink job.
"""
import json
from dataclasses import dataclass
from datetime import datetime

from trading_dagster.utils.clickhouse_client import query_dicts

_LOOKBACK = "1 DAY"  # how far back to search for the most recent strategy bar


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


def fetch_strategies_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyBar]:
    """Return latest strategy bar per strategy for instrument at candle_ts.

    Uses LIMIT 1 BY to get the most recent bar per strategy without FINAL.
    Looks back up to _LOOKBACK to handle gaps in strategy data.
    """
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
WHERE underlying = '{instrument}'
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
        result.append(StrategyBar(
            strategy_table_name=row["strategy_table_name"],
            strategy_id=row["strategy_id"],
            strategy_name=row["strategy_name"],
            underlying=row["underlying"],
            config_timeframe=row["config_timeframe"],
            weighting=row["weighting"],
            position=float(rj.get("position", 0.0)),
            final_signal=float(rj.get("final_signal", 0.0)),
            benchmark=float(rj.get("benchmark", 0.0)),
        ))
    return result
