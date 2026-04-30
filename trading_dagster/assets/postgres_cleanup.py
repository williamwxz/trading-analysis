"""
Dagster Postgres Cleanup

Deletes rows older than 14 days from high-churn tables to keep the DB small
and minimize Supabase network egress. The main offenders are event_logs
(~1k rows/run) and job_ticks (written every sensor tick).

Runs daily at 03:00 UTC, after the safety scan.
"""

import psycopg2

from dagster import (
    AutomationCondition,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

_RETENTION_DAYS = 14

_CLEANUP_STATEMENTS = [
    # Largest table — one row per op event per run
    (
        "event_logs",
        "DELETE FROM event_logs WHERE timestamp < now() - interval '{days} days'",
    ),
    # Sensor/schedule tick history
    (
        "job_ticks",
        "DELETE FROM job_ticks WHERE timestamp < now() - interval '{days} days'",
    ),
    # Asset automation evaluation history (written every sensor tick)
    (
        "asset_daemon_asset_evaluations",
        "DELETE FROM asset_daemon_asset_evaluations WHERE evaluation_id IN ("
        "  SELECT evaluation_id FROM asset_daemon_asset_evaluations"
        "  ORDER BY evaluation_id DESC OFFSET {keep_rows}"
        ")",
    ),
    # Run tags for old completed/failed runs
    (
        "run_tags",
        "DELETE FROM run_tags WHERE run_id IN ("
        "  SELECT run_id FROM runs"
        "  WHERE end_time IS NOT NULL"
        "    AND to_timestamp(end_time) < now() - interval '{days} days'"
        ")",
    ),
    # Completed/failed runs older than retention window
    (
        "runs",
        "DELETE FROM runs"
        "  WHERE end_time IS NOT NULL"
        "    AND to_timestamp(end_time) < now() - interval '{days} days'",
    ),
]

# Keep at most this many asset_daemon rows (no timestamp column available)
_ASSET_EVAL_KEEP_ROWS = 500


def _get_pg_conn(context: AssetExecutionContext):
    import os
    return psycopg2.connect(
        host=os.environ["DAGSTER_PG_HOST"],
        user=os.environ["DAGSTER_PG_USER"],
        password=os.environ["DAGSTER_PG_PASSWORD"],
        dbname=os.environ.get("DAGSTER_PG_DB", "postgres"),
        sslmode="require",
    )


@asset(
    name="postgres_cleanup",
    group_name="infra",
    automation_condition=(
        AutomationCondition.on_cron("0 3 * * *") & ~AutomationCondition.in_progress()
    ),
    description=(
        f"Daily Postgres cleanup: deletes rows older than {_RETENTION_DAYS} days "
        "from event_logs, job_ticks, run_tags, runs, and asset_daemon_asset_evaluations "
        "to keep DB size and Supabase egress low."
    ),
    compute_kind="postgres",
)
def postgres_cleanup_asset(context: AssetExecutionContext) -> MaterializeResult:
    deleted: dict[str, int] = {}

    conn = _get_pg_conn(context)
    try:
        conn.autocommit = False
        cur = conn.cursor()

        for table, sql_template in _CLEANUP_STATEMENTS:
            sql = sql_template.format(
                days=_RETENTION_DAYS,
                keep_rows=_ASSET_EVAL_KEEP_ROWS,
            )
            cur.execute(sql)
            deleted[table] = cur.rowcount
            context.log.info(f"Deleted {cur.rowcount} rows from {table}")

        conn.commit()

        # VACUUM outside transaction to reclaim disk space
        conn.autocommit = True
        for table, _ in _CLEANUP_STATEMENTS:
            context.log.info(f"VACUUMing {table}...")
            cur.execute(f"VACUUM {table}")

    finally:
        conn.close()

    total = sum(deleted.values())
    return MaterializeResult(
        metadata={
            "total_deleted": MetadataValue.int(total),
            **{f"deleted_{t}": MetadataValue.int(n) for t, n in deleted.items()},
        }
    )


__all__ = ["postgres_cleanup_asset"]
