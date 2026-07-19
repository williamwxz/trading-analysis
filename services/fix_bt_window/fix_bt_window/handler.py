"""AWS Lambda entry point for scheduled trailing-window PnL repairs.

One image serves three scheduled functions (bt daily, prod and real_trade
6-hourly) — the mode comes from env, never the event payload, so a mis-sent
payload can't repair one mode while pausing another mode's consumer:

    AUDIT_TYPE        bt | prod | real_trade   (default bt)
    CONSUMER_SERVICE  ECS service to pause/resume around the rewrite
    LOOKBACK_HOURS    default trailing-window size

Why this exists: all live-written PnL rows go stale when their position
source arrives late. For bt, strategy_cum_pnl_bt_v2 is published in batches
hours-to-days after bar time; for prod/real_trade, first revisions land
p50 ~16 min / p99 ~96 min past bar close (measured 2026-07-19). Once the
bars land, history is fully reconstructible — this Lambda re-runs the
standard window repair on a schedule:

    pause consumer -> audit_pnl --type <mode> --fix-window [now-Lh, now)
                   -> resume consumer

The pause/resume matters: the repair rewrites the table tail, and the
consumer's post-resume bootstrap re-seeds its in-memory chain from that
repaired tail (hardened in PR #54), flushing the accumulated lag error.
Kafka replays the paused span on resume, so no minutes are lost.

Event schema (all optional; empty event = rolling window):
    {
        "window_start": "2026-07-07 14:00:00",   # or "2026-07-07"
        "window_end":   "2026-07-09 15:00:00",
        "lookback_hours": 49,
        "dry_run": true          # compute + log only; skips ECS entirely
    }

Ad-hoc invoke:
    aws lambda invoke --function-name trading-analysis-fix-bt-window \\
        --payload '{"dry_run": true}' \\
        --cli-binary-format raw-in-base64-out /dev/stdout
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

_DEFAULT_LOOKBACK_HOURS = 49  # 48h coverage + 1h so runs overlap; repair is idempotent
_STATE_FILE = "/tmp/audit_pnl_state.json"  # Lambda FS is read-only outside /tmp
_REPORT_FILE = "/tmp/audit_pnl_report.md"


def _fetch_secrets_into_env() -> None:
    """Pull the ClickHouse secret bundle and stuff host/password into env.

    Cached per cold-start container; warm invocations skip the round-trip.
    """
    if os.environ.get("CLICKHOUSE_HOST") and os.environ.get("CLICKHOUSE_PASSWORD"):
        return

    arn = os.environ.get("CLICKHOUSE_SECRET_ARN")
    if not arn:
        log.warning(
            "CLICKHOUSE_SECRET_ARN not set — assuming HOST/PASSWORD already in env"
        )
        return

    bundle = json.loads(
        boto3.client("secretsmanager").get_secret_value(SecretId=arn)["SecretString"]
    )
    os.environ["CLICKHOUSE_HOST"] = str(bundle["host"])
    os.environ["CLICKHOUSE_PASSWORD"] = str(bundle["password"])


def _ecs():
    return boto3.client("ecs")


def _cluster() -> str:
    return os.environ.get("ECS_CLUSTER", "trading-analysis")


_VALID_AUDIT_TYPES = ("bt", "prod", "real_trade")

# audit_pnl --type value -> ECS service suffix (real_trade uses a hyphen there).
_DEFAULT_SERVICES = {
    "bt": "trading-analysis-pnl-consumer-bt",
    "prod": "trading-analysis-pnl-consumer-prod",
    "real_trade": "trading-analysis-pnl-consumer-real-trade",
}


def _audit_type() -> str:
    mode = os.environ.get("AUDIT_TYPE", "bt")
    if mode not in _VALID_AUDIT_TYPES:
        raise ValueError(
            f"AUDIT_TYPE must be one of {_VALID_AUDIT_TYPES}, got {mode!r}"
        )
    return mode


def _consumer_service(audit_type: str) -> str:
    svc = os.environ.get("CONSUMER_SERVICE")
    if svc:
        return svc
    if audit_type == "bt":
        # Pre-parameterization env var — keeps an old-config deploy working.
        legacy = os.environ.get("BT_CONSUMER_SERVICE")
        if legacy:
            return legacy
    return _DEFAULT_SERVICES[audit_type]


def compute_window(now: datetime, event: dict[str, Any]) -> tuple[str, str]:
    """[window_start, window_end) as 'YYYY-MM-DD HH:MM:SS', minute-floored.

    Default: the trailing `lookback_hours` (49h) ending at now. Explicit
    event overrides win; a bare date means midnight.
    """

    def _norm(s: str) -> str:
        s = str(s)
        return f"{s} 00:00:00" if len(s) == 10 else s[:19]

    we = event.get("window_end")
    ws = event.get("window_start")
    end_dt = (
        datetime.strptime(_norm(we), "%Y-%m-%d %H:%M:%S")
        if we
        else now.replace(second=0, microsecond=0)
    )
    if ws:
        start_dt = datetime.strptime(_norm(ws), "%Y-%m-%d %H:%M:%S")
    else:
        lookback = int(
            event.get(
                "lookback_hours",
                os.environ.get("LOOKBACK_HOURS", _DEFAULT_LOOKBACK_HOURS),
            )
        )
        start_dt = end_dt - timedelta(hours=lookback)
    return (
        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    )


def build_argv(
    window_start: str,
    window_end: str,
    *,
    dry_run: bool,
    audit_type: str = "bt",
) -> list[str]:
    """argv for audit_pnl.main() — the same CLI contract as a manual run.

    --no-pause is required: audit_pnl's own ECS helper resolves a local AWS
    profile (breaks in Lambda), and this handler manages the pause itself.
    """
    argv = [
        "audit_pnl",
        "--type",
        audit_type,
        "--fix-window",
        window_start,
        window_end,
        "--no-pause",
        "--state-file",
        _STATE_FILE,
        "--report",
        _REPORT_FILE,
    ]
    if dry_run:
        argv.append("--dry-run")
    return argv


def run_audit(argv: list[str]) -> int:
    """Invoke scripts/audit_pnl.py's main() in-process with a patched argv.

    Lazy import: audit_pnl pulls in clickhouse_connect and libs.computation;
    importing at call time keeps module import cheap for unit tests.
    """
    import audit_pnl  # copied into the image alongside libs/

    old_argv = sys.argv
    sys.argv = argv
    try:
        return int(audit_pnl.main())
    finally:
        sys.argv = old_argv


def handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    logging.getLogger().setLevel(logging.INFO)
    event = event or {}
    dry_run = bool(event.get("dry_run", False))

    _fetch_secrets_into_env()

    audit_type = _audit_type()
    now = datetime.now(UTC).replace(tzinfo=None)
    ws, we = compute_window(now, event)
    argv = build_argv(ws, we, dry_run=dry_run, audit_type=audit_type)
    log.info("fix-window [%s]: [%s, %s) dry_run=%s", audit_type, ws, we, dry_run)

    if dry_run:
        # No writes, no ECS churn — just compute and log what would happen.
        rc = run_audit(argv)
        if rc != 0:
            raise RuntimeError(f"audit_pnl dry-run failed rc={rc}")
        return {"status": "ok", "type": audit_type, "window": [ws, we], "dry_run": True}

    ecs = _ecs()
    cluster, service = _cluster(), _consumer_service(audit_type)
    log.info("Pausing %s", service)
    ecs.update_service(cluster=cluster, service=service, desiredCount=0)
    ecs.get_waiter("services_stable").wait(cluster=cluster, services=[service])
    try:
        rc = run_audit(argv)
        if rc != 0:
            raise RuntimeError(f"audit_pnl failed rc={rc}")
    finally:
        # Resume even on failure — a paused consumer is the worse outcome.
        # Its bootstrap re-seeds from the (repaired) table tail; Kafka replays
        # the paused span, so the pause loses nothing.
        log.info("Resuming %s", service)
        ecs.update_service(cluster=cluster, service=service, desiredCount=1)

    return {"status": "ok", "type": audit_type, "window": [ws, we], "dry_run": False}
