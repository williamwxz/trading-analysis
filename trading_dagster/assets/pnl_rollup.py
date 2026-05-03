"""
ClickHouse PnL Hourly Rollup Assets

Daily-partitioned assets that roll up analytics.strategy_pnl_1min_*_v2
into analytics.strategy_pnl_1hour_*_v2.

One asset per variant (prod, bt, real_trade), each with the same
DailyPartitionsDefinition as its upstream 1min asset.
"""

from datetime import datetime, timedelta, timezone

from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    DailyPartitionsDefinition,
    MaterializeResult,
    asset,
)

from ..utils.clickhouse_client import execute, get_client, query_scalar
from ..utils.pnl_compute import BT_START_DATE, PROD_REAL_TRADE_START_DATE

_prod_real_trade_partitions = DailyPartitionsDefinition(
    start_date=PROD_REAL_TRADE_START_DATE
)
_bt_partitions = DailyPartitionsDefinition(start_date=BT_START_DATE)


def _rollup_day(
    context: AssetExecutionContext,
    source_table: str,
    target_table: str,
    extra_agg_cols: str = "",
) -> MaterializeResult:
    date_str = context.partition_key
    start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)
    start_ts = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    # Build extra column fragments for inner SELECT, outer SELECT, and INSERT column list
    extra_cols = [c.strip() for c in extra_agg_cols.split(",")] if extra_agg_cols else []
    inner_extra = (", " + ", ".join(extra_cols)) if extra_cols else ""
    outer_extra = ""
    if extra_cols:
        col_aggs = []
        for col in extra_cols:
            if col == "traded":
                col_aggs.append(f"any({col}) AS {col}")
            else:
                col_aggs.append(f"argMax({col}, src_ts) AS {col}")
        outer_extra = ",\n    " + ",\n    ".join(col_aggs)
    col_list = (
        "strategy_table_name, strategy_id, strategy_name, underlying, "
        "config_timeframe, source, version, ts, cumulative_pnl, benchmark, "
        "position, price, final_signal, weighting, updated_at"
        + (", " + ", ".join(extra_cols) if extra_cols else "")
    )

    client = get_client()

    execute(
        f"DELETE FROM {target_table} "
        f"WHERE toDateTime(ts) >= toDateTime('{start_ts}') "
        f"AND toDateTime(ts) < toDateTime('{end_ts}')",
        client=client,
    )

    sql = f"""\
INSERT INTO {target_table} ({col_list})
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    source,
    version,
    toStartOfHour(src_ts)           AS ts,
    argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
    argMax(benchmark, src_ts)       AS benchmark,
    argMax(position, src_ts)        AS position,
    argMax(price, src_ts)           AS price,
    argMax(final_signal, src_ts)    AS final_signal,
    argMax(weighting, src_ts)       AS weighting,
    now()                           AS updated_at{outer_extra}
FROM (
    SELECT
        strategy_table_name, strategy_id, strategy_name, underlying,
        config_timeframe, source, version,
        ts AS src_ts,
        cumulative_pnl, benchmark, position, price, final_signal, weighting{inner_extra}
    FROM {source_table} FINAL
    WHERE toDateTime(ts) >= toDateTime('{start_ts}')
      AND toDateTime(ts) < toDateTime('{end_ts}')
)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, toStartOfHour(src_ts)
"""
    execute(sql, client=client)

    rows_inserted = int(
        query_scalar(
            f"SELECT count() FROM {target_table} FINAL "
            f"WHERE toDateTime(ts) >= toDateTime('{start_ts}') AND toDateTime(ts) < toDateTime('{end_ts}')",
            client=client,
        )
        or 0
    )
    return MaterializeResult(
        metadata={"partition": date_str, "rows_inserted": rows_inserted}
    )


@asset(
    name="pnl_1hour_prod_rollup",
    group_name="strategy_pnl",
    deps=["pnl_prod_v2_daily"],
    partitions_def=_prod_real_trade_partitions,
    automation_condition=AutomationCondition.eager(),
    compute_kind="clickhouse",
    description="Rolls up strategy_pnl_1min_prod_v2 → strategy_pnl_1hour_prod_v2, daily partition.",
    op_tags={"dagster/timeout": 300},
)
def pnl_1hour_prod_rollup_asset(context: AssetExecutionContext) -> MaterializeResult:
    return _rollup_day(
        context,
        source_table="analytics.strategy_pnl_1min_prod_v2",
        target_table="analytics.strategy_pnl_1hour_prod_v2",
    )


@asset(
    name="pnl_1hour_real_trade_rollup",
    group_name="strategy_pnl",
    deps=["pnl_real_trade_v2_daily"],
    partitions_def=_prod_real_trade_partitions,
    automation_condition=AutomationCondition.eager(),
    compute_kind="clickhouse",
    description="Rolls up strategy_pnl_1min_real_trade_v2 → strategy_pnl_1hour_real_trade_v2, daily partition.",
    op_tags={"dagster/timeout": 300},
)
def pnl_1hour_real_trade_rollup_asset(context: AssetExecutionContext) -> MaterializeResult:
    return _rollup_day(
        context,
        source_table="analytics.strategy_pnl_1min_real_trade_v2",
        target_table="analytics.strategy_pnl_1hour_real_trade_v2",
        extra_agg_cols="closing_ts, execution_ts, traded",
    )


@asset(
    name="pnl_1hour_bt_rollup",
    group_name="strategy_pnl",
    deps=["pnl_bt_v2_daily"],
    partitions_def=_bt_partitions,
    automation_condition=AutomationCondition.eager(),
    compute_kind="clickhouse",
    description="Rolls up strategy_pnl_1min_bt_v2 → strategy_pnl_1hour_bt_v2, daily partition.",
    op_tags={"dagster/timeout": 300},
)
def pnl_1hour_bt_rollup_asset(context: AssetExecutionContext) -> MaterializeResult:
    return _rollup_day(
        context,
        source_table="analytics.strategy_pnl_1min_bt_v2",
        target_table="analytics.strategy_pnl_1hour_bt_v2",
    )


__all__ = [
    "pnl_1hour_prod_rollup_asset",
    "pnl_1hour_real_trade_rollup_asset",
    "pnl_1hour_bt_rollup_asset",
]
