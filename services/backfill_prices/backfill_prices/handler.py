"""AWS Lambda entry point for the daily futures-price backfill.

Cold start:
    1. Fetch ClickHouse host/password from Secrets Manager into env vars
       (Lambda has no native secret-injection like ECS task defs).
    2. Apply optional event-driven overrides (window_start, window_end, etc.)
    3. Lazy-import the script and invoke main().

Event schema (all optional):
    {
        "window_start": "2026-05-30",            # or "2026-05-30 00:00:00"
        "window_end":   "2026-05-31",
        "instruments":  "BTCUSDT,ETHUSDT",
        "lookback_hours": 72
    }

Empty event = daily rolling lookback (what the EventBridge Rule sends).

Ad-hoc invoke for a historical window:
    aws lambda invoke --function-name trading-analysis-backfill-prices \\
        --payload '{"window_start":"2026-06-04","window_end":"2026-06-06"}' \\
        --cli-binary-format raw-in-base64-out /dev/stdout
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

_SECRET_ARN_ENV = "CLICKHOUSE_SECRET_ARN"
# Map Secrets Manager JSON keys → env var names the script expects.
_SECRET_KEY_TO_ENV = {
    "host": "CLICKHOUSE_HOST",
    "password": "CLICKHOUSE_PASSWORD",
}


def _fetch_secrets_into_env() -> None:
    """Pull the ClickHouse secret bundle and stuff host/password into env.

    Cached per cold-start container; warm invocations skip the network round-trip.
    """
    if all(os.environ.get(v) for v in _SECRET_KEY_TO_ENV.values()):
        return  # already populated this container

    arn = os.environ.get(_SECRET_ARN_ENV)
    if not arn:
        log.warning(
            "%s not set — assuming CLICKHOUSE_HOST/PASSWORD already in env "
            "(local dev?)",
            _SECRET_ARN_ENV,
        )
        return

    sm = boto3.client("secretsmanager")
    raw = sm.get_secret_value(SecretId=arn)["SecretString"]
    bundle = json.loads(raw)
    for key, env_name in _SECRET_KEY_TO_ENV.items():
        if key in bundle:
            os.environ[env_name] = str(bundle[key])
        else:
            log.warning("secret key %r missing from %s", key, arn)


_EVENT_TO_ENV = {
    "window_start": "WINDOW_START",
    "window_end": "WINDOW_END",
    "instruments": "INSTRUMENTS",
    "lookback_hours": "LOOKBACK_HOURS",
}


def _apply_event_overrides(event: dict[str, Any] | None) -> None:
    if not event:
        return
    for key, env_name in _EVENT_TO_ENV.items():
        if key in event and event[key] is not None:
            os.environ[env_name] = str(event[key])


def handler(event: dict[str, Any] | None, _context: Any) -> dict[str, Any]:
    _fetch_secrets_into_env()
    _apply_event_overrides(event)

    # Lazy import so module-level constants in __main__ pick up any env vars
    # that we just set above.
    from backfill_prices.__main__ import main  # noqa: PLC0415

    exit_code = main()
    return {"exit_code": exit_code, "status": "ok" if exit_code == 0 else "error"}
