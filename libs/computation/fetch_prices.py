"""Price fetchers from analytics.futures_price_1min (ClickHouse).

Used by Dagster's batch PnL assets. The pnl_consumer gets price directly from
the Redpanda candle's open field — it does not use these functions.
"""

from typing import Dict, List, Optional

from libs.clickhouse_client import query_rows


def _underlying_to_instrument(underlying: str) -> str:
    u = underlying.upper()
    return u if u.endswith("USDT") else f"{u}USDT"


def fetch_prices_multi(
    underlyings: List[str],
    ts_min: str,
    ts_max: str,
    client=None,
    extend_minutes: int = 1440,
) -> Dict[str, Dict[str, float]]:
    """Fetch 1-min open prices for multiple underlyings in a single ClickHouse query.

    Returns {underlying: {ts_str: open_price}}.

    extend_minutes: extra minutes past ts_max to fetch (default 1440 = 1 day, covering
    the last bar's expansion into 1-min rows). Pass 0 when ts_max is already the
    exclusive end boundary (daily partition path).
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
    instr_to_underlying = {_underlying_to_instrument(u): u for u in underlyings}
    result: Dict[str, Dict[str, float]] = {u: {} for u in underlyings}
    for row in rows:
        instrument, ts_str, open_price = row[0], str(row[1]), float(row[2])
        u = instr_to_underlying.get(instrument)
        if u is not None:
            result[u][ts_str] = open_price
    return result
