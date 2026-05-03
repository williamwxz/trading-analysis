"""
ClickHouse connectivity check asset.

Runs every 5 minutes. Connects via clickhouse-connect (HTTPS/8443),
queries system.one to confirm reachability, and logs the server hostname
so we can verify which IP/NAT the connection egresses from.
"""

import socket
from datetime import datetime

from dagster import AssetExecutionContext, AutomationCondition, asset

from ..utils.clickhouse_client import get_client


@asset(
    name="clickhouse_connectivity_check",
    group_name="infra",
    description="Periodic connectivity probe to ClickHouse Cloud via HTTPS/8443. Confirms NAT egress IP is allowlisted.",
    automation_condition=AutomationCondition.on_cron("*/5 * * * *"),
    op_tags={"dagster/timeout": 60},
)
def clickhouse_connectivity_check_asset(context: AssetExecutionContext):
    client = get_client()

    # Confirm basic connectivity
    result = client.query("SELECT 1, hostName(), version()")
    row = result.result_rows[0]
    ch_hostname = row[1]
    ch_version = row[2]

    # Report our own IP as seen from ECS (best-effort)
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "unknown"

    context.log.info(
        f"ClickHouse connection OK | "
        f"server_hostname={ch_hostname} | "
        f"clickhouse_version={ch_version} | "
        f"ecs_container_ip={local_ip} | "
        f"checked_at={datetime.utcnow().isoformat()}Z"
    )

    return {
        "server_hostname": ch_hostname,
        "clickhouse_version": ch_version,
        "ecs_container_ip": local_ip,
    }
