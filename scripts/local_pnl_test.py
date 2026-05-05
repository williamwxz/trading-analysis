"""
Local smoke test for pnl_prod, pnl_real_trade, and pnl_bt daily backfill logic.

Runs one day of each against ClickHouse, inserts to target tables,
then queries back to verify the chain is smooth and timestamps are correct.

Usage:
    source .venv/bin/activate
    export CLICKHOUSE_HOST=... CLICKHOUSE_PORT=8443 CLICKHOUSE_USER=... \
           CLICKHOUSE_PASSWORD=... CLICKHOUSE_SECURE=true
    python scripts/local_pnl_test.py [--date YYYY-MM-DD] [--underlying BTC]
"""

import argparse
import sys
from datetime import UTC, datetime, timedelta

sys.path.insert(0, ".")

from trading_dagster.utils.clickhouse_client import execute, get_client, insert_rows, query_dicts, query_rows
from trading_dagster.utils.pnl_compute import (
    PROD_INSERT_COLUMNS,
    REAL_TRADE_INSERT_COLUMNS,
    assert_anchors_present,
    compute_bt_pnl,
    compute_real_trade_pnl,
    fetch_anchors,
    fetch_new_bars_bt,
    fetch_new_bars_real_trade,
    fetch_prices_multi,
    iter_compute_prod_pnl,
)

# ── helpers ──────────────────────────────────────────────────────────────────

def _parse_ts(s: str) -> datetime:
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")


def _prepare_rows(rows):
    for r in rows:
        if isinstance(r[7], str):
            r[7] = _parse_ts(r[7])
        if isinstance(r[14], str):
            r[14] = _parse_ts(r[14])
        if len(r) > 15 and isinstance(r[15], str):
            r[15] = _parse_ts(r[15])
        if len(r) > 16 and isinstance(r[16], str):
            r[16] = _parse_ts(r[16])
    return rows


def _section(title):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


# ── verification queries ──────────────────────────────────────────────────────

def verify_prod(client, date_str: str, underlying: str):
    _section(f"VERIFY PROD — {date_str} / {underlying}")

    # 1. Row count
    rows = query_rows(
        f"SELECT count() FROM analytics.strategy_pnl_1min_prod_v2 "
        f"WHERE toDate(ts) = '{date_str}' AND underlying = '{underlying}' AND source='production'",
        client
    )
    print(f"  Rows inserted:  {rows[0][0]}")

    # 2. Timestamps start at closing_ts of first bar (not bar open)
    #    For a 5m bar at 00:00, closing_ts = 00:05. First ts in target should be >= 00:05.
    rows = query_rows(
        f"SELECT toString(min(ts)), toString(max(ts)) "
        f"FROM analytics.strategy_pnl_1min_prod_v2 "
        f"WHERE toDate(ts) = '{date_str}' AND underlying = '{underlying}' AND source='production'",
        client
    )
    print(f"  ts range:       {rows[0][0]}  →  {rows[0][1]}")

    # 3. Sample one strategy — show first 3 and last 3 rows
    strats = query_rows(
        f"SELECT DISTINCT strategy_table_name FROM analytics.strategy_pnl_1min_prod_v2 "
        f"WHERE toDate(ts) = '{date_str}' AND underlying = '{underlying}' AND source='production' LIMIT 1",
        client
    )
    if not strats:
        print("  No strategies found!")
        return
    stn = strats[0][0]
    sample = query_dicts(
        f"SELECT toString(ts) AS ts_str, cumulative_pnl, price, position "
        f"FROM analytics.strategy_pnl_1min_prod_v2 "
        f"WHERE strategy_table_name = '{stn}' AND toDate(ts) = '{date_str}' AND source='production' "
        f"ORDER BY ts LIMIT 3",
        client
    )
    print(f"  Strategy:       {stn}")
    print(f"  First 3 rows:")
    for r in sample:
        print(f"    ts={r['ts_str']}  cpnl={r['cumulative_pnl']:.6f}  price={r['price']:.2f}  pos={r['position']}")

    # 4. Check for large jumps (> 5% in a single minute) — sign of anchor corruption
    jump_rows = query_rows(
        f"""
        SELECT count() FROM (
            SELECT ts, cumulative_pnl,
                   lagInFrame(cumulative_pnl) OVER (PARTITION BY strategy_table_name ORDER BY ts) AS prev_cpnl
            FROM analytics.strategy_pnl_1min_prod_v2
            WHERE toDate(ts) = '{date_str}' AND underlying = '{underlying}' AND source='production'
        ) WHERE abs(cumulative_pnl - prev_cpnl) > 0.05
        """,
        client
    )
    jumps = jump_rows[0][0]
    print(f"  Large jumps (>5% single-min): {jumps}" + ("  ✓" if jumps == 0 else "  ✗ CHECK THIS"))


def verify_real_trade(client, date_str: str, underlying: str):
    _section(f"VERIFY REAL_TRADE — {date_str} / {underlying}")

    rows = query_rows(
        f"SELECT count() FROM analytics.strategy_pnl_1min_real_trade_v2 "
        f"WHERE toDate(ts) = '{date_str}' AND underlying = '{underlying}' AND source='real_trade'",
        client
    )
    print(f"  Rows inserted:  {rows[0][0]}")

    rows = query_rows(
        f"SELECT toString(min(ts)), toString(max(ts)) "
        f"FROM analytics.strategy_pnl_1min_real_trade_v2 "
        f"WHERE toDate(ts) = '{date_str}' AND underlying = '{underlying}' AND source='real_trade'",
        client
    )
    print(f"  ts range:       {rows[0][0]}  →  {rows[0][1]}")

    strats = query_rows(
        f"SELECT DISTINCT strategy_table_name FROM analytics.strategy_pnl_1min_real_trade_v2 "
        f"WHERE toDate(ts) = '{date_str}' AND underlying = '{underlying}' AND source='real_trade' LIMIT 1",
        client
    )
    if not strats:
        print("  No strategies found (real_trade may have no data for this date)")
        return
    stn = strats[0][0]
    sample = query_dicts(
        f"SELECT toString(ts) AS ts_str, cumulative_pnl, price, position, toString(execution_ts) AS exec_ts_str "
        f"FROM analytics.strategy_pnl_1min_real_trade_v2 "
        f"WHERE strategy_table_name = '{stn}' AND toDate(ts) = '{date_str}' AND source='real_trade' "
        f"ORDER BY ts LIMIT 3",
        client
    )
    print(f"  Strategy:       {stn}")
    print(f"  First 3 rows (ts=expanded minute, execution_ts=trade time):")
    for r in sample:
        print(f"    ts={r['ts_str']}  cpnl={r['cumulative_pnl']:.6f}  price={r['price']:.2f}  exec_ts={r['exec_ts_str']}")


def verify_bt(client, date_str: str, underlying: str):
    _section(f"VERIFY BT — {date_str} / {underlying}")

    rows = query_rows(
        f"SELECT count() FROM analytics.strategy_pnl_1min_bt_v2 "
        f"WHERE toDate(ts) = '{date_str}' AND underlying = '{underlying}' AND source='backtest'",
        client
    )
    print(f"  Rows inserted:  {rows[0][0]}")

    rows = query_rows(
        f"SELECT toString(min(ts)), toString(max(ts)) "
        f"FROM analytics.strategy_pnl_1min_bt_v2 "
        f"WHERE toDate(ts) = '{date_str}' AND underlying = '{underlying}' AND source='backtest'",
        client
    )
    print(f"  ts range:       {rows[0][0]}  →  {rows[0][1]}")

    strats = query_rows(
        f"SELECT DISTINCT strategy_table_name FROM analytics.strategy_pnl_1min_bt_v2 "
        f"WHERE toDate(ts) = '{date_str}' AND underlying = '{underlying}' AND source='backtest' LIMIT 1",
        client
    )
    if not strats:
        print("  No strategies found!")
        return
    stn = strats[0][0]
    sample = query_dicts(
        f"SELECT toString(ts) AS ts_str, cumulative_pnl, price, position "
        f"FROM analytics.strategy_pnl_1min_bt_v2 "
        f"WHERE strategy_table_name = '{stn}' AND toDate(ts) = '{date_str}' AND source='backtest' "
        f"ORDER BY ts LIMIT 3",
        client
    )
    print(f"  Strategy:       {stn}")
    print(f"  First 3 rows:")
    for r in sample:
        print(f"    ts={r['ts_str']}  cpnl={r['cumulative_pnl']:.6f}  price={r['price']:.2f}  pos={r['position']}")

    # BT: check that ts values start at closing_ts (> bar ts)
    # For a 1h bar, ts in target should be >= bar_ts + 60min
    jump_rows = query_rows(
        f"""
        SELECT count() FROM (
            SELECT ts, cumulative_pnl,
                   lagInFrame(cumulative_pnl) OVER (PARTITION BY strategy_table_name ORDER BY ts) AS prev_cpnl
            FROM analytics.strategy_pnl_1min_bt_v2
            WHERE toDate(ts) = '{date_str}' AND underlying = '{underlying}' AND source='backtest'
        ) WHERE abs(cumulative_pnl - prev_cpnl) > 0.05
        """,
        client
    )
    jumps = jump_rows[0][0]
    print(f"  Large jumps (>5% single-min): {jumps}" + ("  ✓" if jumps == 0 else "  ✗ CHECK THIS"))


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2026-04-01", help="Partition date YYYY-MM-DD")
    parser.add_argument("--underlying", default="BTC", help="Underlying (BTC, ETH, etc.)")
    args = parser.parse_args()

    date_str = args.date
    underlying = args.underlying
    start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = start_dt + timedelta(days=1)
    start_ts = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"\nLocal PnL smoke test — date={date_str}  underlying={underlying}")
    client = get_client()

    # ── PROD ──────────────────────────────────────────────────────────────────
    _section(f"RUNNING PROD — {date_str} / {underlying}")
    source_prod = "strategy_output_history_v2"
    target_prod = "strategy_pnl_1min_prod_v2"

    # Delete this day's rows so the run is idempotent
    execute(
        f"DELETE FROM analytics.{target_prod} "
        f"WHERE toDateTime(ts) >= toDateTime('{start_ts}') AND toDateTime(ts) < toDateTime('{end_ts}') "
        f"AND source='production' AND underlying='{underlying}'",
        client
    )

    sql_prod = f"""\
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe,
    argMin(weighting, revision_ts) AS weighting, toString(ts) AS ts_str,
    JSONExtractFloat(argMin(row_json, revision_ts), 'position') AS position,
    JSONExtractFloat(argMin(row_json, revision_ts), 'price') AS bar_price,
    JSONExtractFloat(argMin(row_json, revision_ts), 'final_signal') AS final_signal,
    JSONExtractFloat(argMin(row_json, revision_ts), 'benchmark') AS bar_benchmark
FROM analytics.{source_prod}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND ts >= toDateTime('{start_ts}') AND ts < toDateTime('{end_ts}')
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
ORDER BY strategy_table_name, ts
"""
    bars_prod = query_dicts(sql_prod, client)
    for r in bars_prod:
        r["ts"] = r.pop("ts_str")
    print(f"  Source bars fetched: {len(bars_prod)}")

    if bars_prod:
        all_prices = fetch_prices_multi([underlying], start_ts, end_ts, client, extend_minutes=120)
        prices = all_prices.get(underlying, {})
        anchors_prod = fetch_anchors(target_prod, underlying)
        assert_anchors_present(anchors_prod, bars_prod, source_table=source_prod)

        total = 0
        for stn, strategy_rows in iter_compute_prod_pnl(bars_prod, anchors_prod, prices, source_label="production"):
            _prepare_rows(strategy_rows)
            n = insert_rows(f"analytics.{target_prod}", PROD_INSERT_COLUMNS, strategy_rows, client)
            total += n
        print(f"  Inserted: {total} rows")
    else:
        print("  No bars for this date/underlying — skipping insert")

    verify_prod(client, date_str, underlying)

    # ── REAL_TRADE ────────────────────────────────────────────────────────────
    _section(f"RUNNING REAL_TRADE — {date_str} / {underlying}")
    target_rt = "strategy_pnl_1min_real_trade_v2"

    execute(
        f"DELETE FROM analytics.{target_rt} "
        f"WHERE toDateTime(ts) >= toDateTime('{start_ts}') AND toDateTime(ts) < toDateTime('{end_ts}') "
        f"AND source='real_trade' AND underlying='{underlying}'",
        client
    )

    bars_rt = fetch_new_bars_real_trade(source_prod, underlying, start_ts, ts_end=end_ts)
    print(f"  Source revisions fetched: {len(bars_rt)}")

    if bars_rt:
        ts_min = min(b["ts"] for b in bars_rt)
        ts_max = max(b["ts"] for b in bars_rt)
        from trading_dagster.utils.pnl_compute import fetch_prices
        prices_rt = fetch_prices(underlying, ts_min, ts_max, client)
        anchors_rt = fetch_anchors(target_rt, underlying)
        assert_anchors_present(anchors_rt, bars_rt, source_table=source_prod, bar_ts_key="execution_ts")
        rows_rt = compute_real_trade_pnl(bars_rt, anchors_rt, prices_rt)
        _prepare_rows(rows_rt)
        n = insert_rows(f"analytics.{target_rt}", REAL_TRADE_INSERT_COLUMNS, rows_rt, client)
        print(f"  Inserted: {n} rows")
    else:
        print("  No revisions for this date/underlying — skipping insert")

    verify_real_trade(client, date_str, underlying)

    # ── BT ────────────────────────────────────────────────────────────────────
    _section(f"RUNNING BT — {date_str} / {underlying}")
    source_bt = "strategy_output_history_bt_v2"
    target_bt = "strategy_pnl_1min_bt_v2"

    execute(
        f"DELETE FROM analytics.{target_bt} "
        f"WHERE toDateTime(ts) >= toDateTime('{start_ts}') AND toDateTime(ts) < toDateTime('{end_ts}') "
        f"AND source='backtest' AND underlying='{underlying}'",
        client
    )

    bars_bt = fetch_new_bars_bt(source_bt, underlying, start_ts, ts_end=end_ts)
    print(f"  Source bars fetched: {len(bars_bt)}")

    if bars_bt:
        all_prices_bt = fetch_prices_multi([underlying], start_ts, end_ts, client, extend_minutes=120, price_column="close")
        prices_bt = all_prices_bt.get(underlying, {})
        anchors_bt = fetch_anchors(target_bt, underlying)
        assert_anchors_present(anchors_bt, bars_bt, source_table=source_bt, bar_ts_key="execution_ts")
        rows_bt = compute_bt_pnl(bars_bt, prices_bt, anchors=anchors_bt)
        _prepare_rows(rows_bt)
        n = insert_rows(f"analytics.{target_bt}", PROD_INSERT_COLUMNS, rows_bt, client)
        print(f"  Inserted: {n} rows")
    else:
        print("  No bars for this date/underlying — skipping insert")

    verify_bt(client, date_str, underlying)

    print(f"\n{'─'*60}")
    print("  Done. Check the output above for any ✗ markers.")
    print(f"{'─'*60}\n")


if __name__ == "__main__":
    main()
