"""AWS Lambda entry point for the scheduled PnL window repair.

One image, three functions, selected by the AUDIT_TYPE env var:

    fix-bt-window          AUDIT_TYPE=bt          daily 15:00 UTC
    fix-prod-window        AUDIT_TYPE=prod        6-hourly from 03:10 UTC
    fix-real-trade-window  AUDIT_TYPE=real_trade  6-hourly from 03:40 UTC

Each one does:

    pause <mode> consumer -> audit_pnl --type <mode> --fix-window [start, now)
                          -> resume <mode> consumer

The pause/resume matters: the repair rewrites the table tail, and the
consumer's post-resume bootstrap re-seeds its in-memory chain from that
repaired tail (hardened in PR #54), flushing the accumulated lag error.
Kafka replays the paused span on resume, so no minutes are lost.

Why this exists at all — `cumulative_pnl` is a chained anchor quantity, so a
minute computed on a stale position is permanent and every later minute
inherits it. Without a repair pass the error only ever grows. Measured
2026-07-31: bt (which had this Lambda) rewrote 100% of rows daily, while prod
and real_trade had *zero* rows with a write lag over 1h across 8 days.

How `start` is chosen differs by mode, because the two position sources fail
differently:

  bt          `strategy_cum_pnl_bt_v2` is re-pushed in bulk hours-to-days after
              bar time. A fixed trailing 49h window is the right shape.
  prod / rt   `strategy_output_history_v2` revisions land p50 ~16m / p90 ~72m
              past bar close — but on 2026-07-28 a publisher stall was followed
              by ~168k revisions carrying bars up to 144h old. A fixed 49h
              window repairs none of those, so the start is pulled back to the
              oldest bar revised since the last run, floored at the fixed
              lookback and capped at MAX_LOOKBACK_HOURS.

Event schema (all optional; empty event = the mode's rolling window):
    {
        "window_start": "2026-07-07 14:00:00",   # or "2026-07-07"
        "window_end":   "2026-07-09 15:00:00",
        "lookback_hours": 49,
        "arrival_window_hours": 7,   # prod/real_trade only
        "dry_run": true              # compute + log only; skips ECS entirely
    }
An explicit window_start is authoritative and skips the arrival probe.

Ad-hoc invoke:
    aws lambda invoke --function-name trading-analysis-fix-prod-window \\
        --payload '{"dry_run": true}' \\
        --cli-binary-format raw-in-base64-out /dev/stdout
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import boto3

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

Mode = Literal["bt", "prod", "real_trade"]
_MODES: frozenset[str] = frozenset({"bt", "prod", "real_trade"})

# prod/real_trade source positions from strategy_output_history_v2, whose
# revisions land hours-to-days after bar time — a fixed trailing window misses
# them. bt sources from the cum table and is already covered by its 49h window.
_ARRIVAL_DRIVEN_MODES: frozenset[str] = frozenset({"prod", "real_trade"})

_DEFAULT_LOOKBACK_HOURS = 49  # 48h coverage + 1h so runs overlap; repair is idempotent
_DEFAULT_MAX_LOOKBACK_HOURS = 336  # 14d ceiling; a full-table re-push must not
#                                    turn one scheduled run into a huge recompute
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


def audit_type() -> Mode:
    """Which mode this function repairs — one image backs all three schedules."""
    mode = os.environ.get("AUDIT_TYPE", "bt")
    if mode not in _MODES:
        raise ValueError(
            f"AUDIT_TYPE={mode!r} is not one of {sorted(_MODES)} — refusing to run"
        )
    return mode  # type: ignore[return-value]


def consumer_service() -> str:
    """ECS service to pause. Explicit env wins; otherwise derive from the mode."""
    explicit = os.environ.get("CONSUMER_SERVICE")
    if explicit:
        return explicit
    mode = audit_type()
    if mode == "bt":
        # Predates AUDIT_TYPE; still honoured so existing bt config keeps working.
        legacy = os.environ.get("BT_CONSUMER_SERVICE")
        if legacy:
            return legacy
    return f"trading-analysis-pnl-consumer-{mode.replace('_', '-')}"


def _probe_oldest_revised_bar(end_dt: datetime, arrival_hours: int) -> datetime | None:
    """Oldest bar `ts` whose revision landed within the last `arrival_hours`.

    This is the arrival-driven start bound. A fixed trailing lookback assumes
    bars arrive promptly; they don't. On 2026-07-28 the publisher stalled for
    4h and then backfilled ~168k revisions carrying bars up to 144h old — a 49h
    window would have repaired none of them, leaving the stale minutes baked
    into the chain permanently.

    Bounded by MAX_LOOKBACK_HOURS on both sides so a full-table re-push can't
    turn one run into a six-month recompute.
    """
    from libs.clickhouse_client import query_scalar

    max_lookback = _max_lookback_hours()
    got = query_scalar(
        "SELECT min(ts) FROM analytics.strategy_output_history_v2 "
        f"WHERE revision_ts >= toDateTime('{end_dt:%Y-%m-%d %H:%M:%S}') "
        f"                    - INTERVAL {arrival_hours} HOUR "
        f"  AND ts          >= toDateTime('{end_dt:%Y-%m-%d %H:%M:%S}') "
        f"                    - INTERVAL {max_lookback} HOUR"
    )
    # ClickHouse returns the DateTime zero-value when no rows match.
    if not isinstance(got, datetime) or got.year <= 1970:
        return None
    return got


def _max_lookback_hours() -> int:
    return int(os.environ.get("MAX_LOOKBACK_HOURS", _DEFAULT_MAX_LOOKBACK_HOURS))


def compute_window(
    now: datetime,
    event: dict[str, Any],
    *,
    arrival_probe: Callable[[datetime, int], datetime | None] | None = None,
) -> tuple[str, str]:
    """[window_start, window_end) as 'YYYY-MM-DD HH:MM:SS', minute-floored.

    Baseline: the trailing `lookback_hours` (49h) ending at now. For prod and
    real_trade the start is additionally pulled back to the oldest bar revised
    since the last run (see `_probe_oldest_revised_bar`), because those modes
    source positions from `strategy_output_history_v2`, whose revisions arrive
    hours-to-days late. bt sources from the cum table and keeps the fixed
    window. Explicit event overrides always win; a bare date means midnight.
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

        if audit_type() in _ARRIVAL_DRIVEN_MODES:
            probe = arrival_probe or _probe_oldest_revised_bar
            arrival_hours = int(
                event.get(
                    "arrival_window_hours",
                    os.environ.get("ARRIVAL_WINDOW_HOURS", lookback),
                )
            )
            floor_dt = end_dt - timedelta(hours=_max_lookback_hours())
            try:
                oldest = probe(end_dt, arrival_hours)
            except Exception:
                # Degrade to the fixed window rather than skip the run entirely.
                log.exception("arrival probe failed — falling back to %dh", lookback)
                oldest = None
            if oldest and oldest < start_dt:
                start_dt = max(oldest, floor_dt)
                if start_dt == floor_dt:
                    log.warning(
                        "arrival probe returned %s, clamped to MAX_LOOKBACK_HOURS=%d "
                        "(%s) — older stale minutes are NOT repaired by this run",
                        oldest,
                        _max_lookback_hours(),
                        floor_dt,
                    )
                else:
                    log.info(
                        "arrival-driven start: %s (fixed lookback would have been %s)",
                        start_dt,
                        end_dt - timedelta(hours=lookback),
                    )
    return (
        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    )


def build_argv(window_start: str, window_end: str, *, dry_run: bool) -> list[str]:
    """argv for audit_pnl.main() — the same CLI contract as a manual run.

    --no-pause is required: audit_pnl's own ECS helper resolves a local AWS
    profile (breaks in Lambda), and this handler manages the pause itself.
    """
    argv = [
        "audit_pnl",
        "--type",
        audit_type(),
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

    now = datetime.now(UTC).replace(tzinfo=None)
    ws, we = compute_window(now, event)
    argv = build_argv(ws, we, dry_run=dry_run)
    log.info("fix-window[%s]: [%s, %s) dry_run=%s", audit_type(), ws, we, dry_run)

    if dry_run:
        # No writes, no ECS churn — just compute and log what would happen.
        rc = run_audit(argv)
        if rc != 0:
            raise RuntimeError(f"audit_pnl dry-run failed rc={rc}")
        return {
            "status": "ok",
            "mode": audit_type(),
            "window": [ws, we],
            "dry_run": True,
        }

    ecs = _ecs()
    cluster, service = _cluster(), consumer_service()
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

    return {"status": "ok", "mode": audit_type(), "window": [ws, we], "dry_run": False}
