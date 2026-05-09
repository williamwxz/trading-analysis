import json
from dataclasses import dataclass
from datetime import datetime

from trading_dagster.utils.clickhouse_client import query_dicts

_LOOKBACK = "1 DAY"

# ClickHouse multiIf expression mapping config_timeframe → bar width in minutes.
_TF_MINUTES_EXPR = """\
multiIf(
        h.config_timeframe = '1m',  1,
        h.config_timeframe = '3m',  3,
        h.config_timeframe = '5m',  5,
        h.config_timeframe = '15m', 15,
        h.config_timeframe = '30m', 30,
        h.config_timeframe = '1h',  60,
        h.config_timeframe = '4h',  240,
        h.config_timeframe = '1d',  1440,
        5
    )"""


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


def _parse_strategy_bar(row: dict) -> StrategyBar:
    rj = json.loads(row["row_json"])
    return StrategyBar(
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


def _parse_revision(row: dict) -> StrategyRevision:
    rj = json.loads(row["row_json"])
    return StrategyRevision(
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


def fetch_strategies_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyBar]:
    """Return latest strategy bar per strategy for instrument at candle_ts."""
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
    return [_parse_strategy_bar(r) for r in query_dicts(sql)]


def fetch_bt_strategies_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyBar]:
    """Return latest bt strategy bar per strategy for instrument at candle_ts."""
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
    return [_parse_strategy_bar(r) for r in query_dicts(sql)]


def fetch_anchor_for_strategy(strategy_table_name: str) -> "AnchorRecord | None":
    """One-shot anchor lookup for a single strategy, searching all of history.

    Returns None only when the strategy has never appeared in any pnl table —
    i.e. it is truly brand-new. Long-inactive strategies (last row > 48h ago)
    are also handled: the most recent row ever is returned so the PnL chain
    resumes from the correct baseline rather than restarting from zero.
    """
    from pnl_consumer.anchor_state import AnchorRecord

    for table in (
        "analytics.strategy_pnl_1min_prod_v2",
        "analytics.strategy_pnl_1min_real_trade_v2",
        "analytics.strategy_pnl_1min_bt_v2",
    ):
        sql = f"""\
SELECT cumulative_pnl AS pnl, price, position
FROM {table}
WHERE strategy_table_name = '{strategy_table_name}'
ORDER BY ts DESC, updated_at DESC
LIMIT 1
"""
        rows = query_dicts(sql)
        if rows:
            r = rows[0]
            return AnchorRecord(pnl=r["pnl"], price=r["price"], position=r["position"])
    return None


def fetch_last_active_revisions() -> "dict[str, StrategyRevision]":
    """Return the last active revision per strategy as of now, across all underlyings.

    Used at cold-start to seed last_real_trade_revisions so carry-forward works
    from the first candle — strategies whose revision hasn't fired yet still hold
    their previous bar's position.
    """
    sql = f"""\
WITH latest AS (
    SELECT
        strategy_table_name,
        config_timeframe,
        max(ts) AS latest_ts
    FROM analytics.strategy_output_history_v2
    WHERE ts <= now()
      AND ts >= now() - INTERVAL {_LOOKBACK}
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
    h.ts + toIntervalMinute({_TF_MINUTES_EXPR}) AS closing_ts,
    h.row_json
FROM analytics.strategy_output_history_v2 h
JOIN latest l
  ON h.strategy_table_name = l.strategy_table_name
 AND h.config_timeframe = l.config_timeframe
 AND h.ts = l.latest_ts
WHERE toStartOfMinute(h.revision_ts + INTERVAL 59 SECOND) <= now()
ORDER BY h.strategy_table_name, h.revision_ts
LIMIT 1 BY h.strategy_table_name, h.config_timeframe, h.revision_ts
"""
    result: dict[str, StrategyRevision] = {}
    for row in query_dicts(sql):
        # last one wins (rows ordered ASC by revision_ts — last is most recent active)
        result[row["strategy_table_name"]] = _parse_revision(row)
    return result


def fetch_real_trade_revisions_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyRevision]:
    """Return all revisions for the most recent real_trade bar per strategy at or before candle_ts.

    All revisions for the latest bar are returned ordered by revision_ts ASC.
    The caller filters by execution_ts <= candle_ts to find the active revision.
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
    h.ts + toIntervalMinute({_TF_MINUTES_EXPR}) AS closing_ts,
    h.row_json
FROM analytics.strategy_output_history_v2 h
JOIN latest l
  ON h.strategy_table_name = l.strategy_table_name
 AND h.config_timeframe = l.config_timeframe
 AND h.ts = l.latest_ts
WHERE h.underlying = '{underlying}'
ORDER BY h.strategy_table_name, h.revision_ts
LIMIT 1 BY h.strategy_table_name, h.config_timeframe, h.revision_ts
"""
    return [_parse_revision(r) for r in query_dicts(sql)]
