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
    Falls back to the most recent bar within _LOOKBACK when the strategy service lags.
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
    max(ts) AS latest_ts,
    argMin(row_json, revision_ts) AS row_json
FROM analytics.strategy_output_history_bt_v2
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


def fetch_anchor_for_strategy(strategy_table_name: str) -> AnchorRecord | None:
    """One-shot anchor lookup for a single strategy, searching up to 48 hours back.

    Returns None when no rows exist (brand-new or long-inactive strategy).
    Used by process_candle to lazy-seed a missing anchor without crashing.
    """
    from pnl_consumer.anchor_state import AnchorRecord

    for table in (
        "analytics.strategy_pnl_1min_prod_v2",
        "analytics.strategy_pnl_1min_real_trade_v2",
    ):
        sql = f"""\
SELECT
    cumulative_pnl  AS anchor_pnl,
    price           AS anchor_price,
    position        AS anchor_position
FROM {table}
WHERE strategy_table_name = '{strategy_table_name}'
ORDER BY ts DESC, updated_at DESC
LIMIT 1
"""
        rows = query_dicts(sql)
        if rows:
            r = rows[0]
            return AnchorRecord(
                anchor_pnl=r["anchor_pnl"],
                anchor_price=r["anchor_price"],
                anchor_position=r["anchor_position"],
            )
    return None


def fetch_real_trade_revisions_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyRevision]:
    """Return all revisions for the most recent real_trade bar per strategy at or before candle_ts.

    Falls back to the most recent bar within _LOOKBACK when the strategy service lags.
    Revisions where revision_ts < ts + 2x timeframe are kept — allows for post-close execution lag.
    Revisions are ordered by revision_ts ASC (one per position update within the bar).
    """
    underlying = instrument.removesuffix("USDT")
    ts_str = candle_ts.strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""\
WITH latest AS (
    SELECT
        strategy_table_name,
        config_timeframe,
        max(ts) AS latest_ts
    FROM analytics.strategy_output_history_v2
    WHERE underlying = '{underlying}'
      AND ts <= '{ts_str}'
      AND ts >= '{ts_str}'::DateTime - INTERVAL {_LOOKBACK}
    GROUP BY strategy_table_name, config_timeframe
)
SELECT
    h.strategy_table_name,
    h.strategy_id,
    h.strategy_name,
    h.underlying,
    h.config_timeframe,
    h.weighting,
    h.revision_ts,
    h.ts + toIntervalMinute(multiIf(
        h.config_timeframe = '1m',  1,
        h.config_timeframe = '3m',  3,
        h.config_timeframe = '5m',  5,
        h.config_timeframe = '15m', 15,
        h.config_timeframe = '30m', 30,
        h.config_timeframe = '1h',  60,
        h.config_timeframe = '4h',  240,
        h.config_timeframe = '1d',  1440,
        5
    )) AS closing_ts,
    h.row_json
FROM analytics.strategy_output_history_v2 h
JOIN latest l
  ON h.strategy_table_name = l.strategy_table_name
 AND h.config_timeframe = l.config_timeframe
 AND h.ts = l.latest_ts
WHERE h.underlying = '{underlying}'
  AND h.revision_ts < h.ts + toIntervalMinute(2 * multiIf(
        h.config_timeframe = '1m',  1,
        h.config_timeframe = '3m',  3,
        h.config_timeframe = '5m',  5,
        h.config_timeframe = '15m', 15,
        h.config_timeframe = '30m', 30,
        h.config_timeframe = '1h',  60,
        h.config_timeframe = '4h',  240,
        h.config_timeframe = '1d',  1440,
        5
    ))
ORDER BY h.strategy_table_name, h.revision_ts
LIMIT 1 BY h.strategy_table_name, h.config_timeframe, h.revision_ts
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
