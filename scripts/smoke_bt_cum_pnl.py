"""Live smoke test for the BT cum-pnl fetchers and batch compute.

Runs the new query strings against ClickHouse Cloud and a tiny end-to-end compute
for one strategy. Requires `source .env`. NOT a unit test — exercises real SQL.
"""

import sys
from datetime import datetime, timedelta

from libs.clickhouse_client import get_client
from libs.computation.fetch_bars import fetch_bt_anchors, fetch_bt_benchmarks
from libs.computation.fetch_prices import fetch_prices_multi
from libs.computation.pnl_formula import compute_bt_pnl

UNDERLYING = "SOL"


def main() -> int:
    client = get_client()
    end = datetime.utcnow().replace(second=0, microsecond=0)
    start = end - timedelta(hours=6)
    start_ts = start.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end.strftime("%Y-%m-%d %H:%M:%S")

    anchors = fetch_bt_anchors(UNDERLYING, start_ts, end_ts, client)
    print(f"fetch_bt_anchors: {len(anchors)} strategies")
    assert anchors, "no anchors returned"

    benchmarks = fetch_bt_benchmarks(UNDERLYING, start_ts, end_ts, client)
    print(f"fetch_bt_benchmarks: {len(benchmarks)} (stn,ts) keys")

    price_lo = (start - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    prices = fetch_prices_multi(
        [UNDERLYING], price_lo, end_ts, client, extend_minutes=0
    )[UNDERLYING]
    print(f"prices: {len(prices)} minutes")
    assert prices, "no prices returned"

    stn = next(iter(anchors))
    rows = compute_bt_pnl(
        anchors[stn], prices, benchmarks, start_ts, end_ts, price_keys=sorted(prices)
    )
    print(f"compute_bt_pnl({stn[:40]}...): {len(rows)} rows")
    assert rows and len(rows[0]) == 16, "bad row shape"
    from libs.computation.candle_lookup import fetch_bt_anchors_for_candle

    live = fetch_bt_anchors_for_candle(f"{UNDERLYING}USDT", end)
    print(f"fetch_bt_anchors_for_candle: {len(live)} strategies")
    assert live, "no live anchors"
    a = live[0]
    assert a.anchor_price > 0, "anchor_price not resolved"
    print(
        f"  sample: sid={a.strategy_id} u={a.underlying} anchor_ts={a.anchor_ts} "
        f"price={a.anchor_price} bench={a.benchmark}"
    )
    print("SMOKE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
