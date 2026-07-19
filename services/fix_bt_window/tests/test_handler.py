"""Unit tests for the fix-bt-window Lambda handler.

Orchestration only — audit_pnl invocation and ECS are mocked. The handler's
contract: pause the bt consumer, run audit_pnl --type bt --fix-window over the
trailing window, resume the consumer even on failure.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fix_bt_window import handler as h

_NOW = datetime(2026, 7, 9, 15, 0, 37)


@pytest.mark.unit
def test_compute_window_default_lookback():
    """Default window: [now - 49h, now), both floored to the minute."""
    ws, we = h.compute_window(_NOW, {})
    assert we == "2026-07-09 15:00:00"
    assert ws == "2026-07-07 14:00:00"  # 49h earlier


@pytest.mark.unit
def test_compute_window_event_overrides():
    ws, we = h.compute_window(
        _NOW, {"window_start": "2026-07-01 00:00:00", "window_end": "2026-07-02"}
    )
    assert ws == "2026-07-01 00:00:00"
    assert we == "2026-07-02 00:00:00"


@pytest.mark.unit
def test_build_argv_shape():
    argv = h.build_argv("2026-07-07 14:00:00", "2026-07-09 15:00:00", dry_run=False)
    assert argv[0] == "audit_pnl"
    assert ["--type", "bt"] == argv[1:3]  # bt is the default audit type
    assert ["--fix-window", "2026-07-07 14:00:00", "2026-07-09 15:00:00"] == argv[3:6]
    assert "--no-pause" in argv  # the Lambda manages ECS itself
    # Lambda FS is read-only outside /tmp
    assert argv[argv.index("--state-file") + 1].startswith("/tmp/")
    assert argv[argv.index("--report") + 1].startswith("/tmp/")
    assert "--dry-run" not in argv


@pytest.mark.unit
def test_build_argv_dry_run():
    argv = h.build_argv("a", "b", dry_run=True)
    assert "--dry-run" in argv


# ─────────────────────────────────────────────────────────────────────────────
# Mode parameterization — one image serves the bt/prod/real_trade schedules;
# AUDIT_TYPE + CONSUMER_SERVICE env select the mode (never the event payload,
# so a mis-sent payload can't repair one mode while pausing another's consumer).
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_build_argv_prod_type():
    argv = h.build_argv("a", "b", dry_run=False, audit_type="prod")
    assert ["--type", "prod"] == argv[1:3]


@pytest.mark.unit
def test_audit_type_from_env(monkeypatch):
    monkeypatch.setenv("AUDIT_TYPE", "real_trade")
    assert h._audit_type() == "real_trade"


@pytest.mark.unit
def test_audit_type_defaults_to_bt(monkeypatch):
    monkeypatch.delenv("AUDIT_TYPE", raising=False)
    assert h._audit_type() == "bt"


@pytest.mark.unit
def test_audit_type_rejects_unknown(monkeypatch):
    monkeypatch.setenv("AUDIT_TYPE", "everything")
    with pytest.raises(ValueError, match="AUDIT_TYPE"):
        h._audit_type()


@pytest.mark.unit
def test_consumer_service_from_env(monkeypatch):
    monkeypatch.setenv("CONSUMER_SERVICE", "svc-x")
    assert h._consumer_service("prod") == "svc-x"


@pytest.mark.unit
def test_consumer_service_legacy_bt_env_fallback(monkeypatch):
    """BT_CONSUMER_SERVICE (the pre-parameterization env var) still wins for
    bt when CONSUMER_SERVICE is unset — an old-config deploy keeps working."""
    monkeypatch.delenv("CONSUMER_SERVICE", raising=False)
    monkeypatch.setenv("BT_CONSUMER_SERVICE", "legacy-bt-svc")
    assert h._consumer_service("bt") == "legacy-bt-svc"


@pytest.mark.unit
def test_consumer_service_mode_defaults(monkeypatch):
    monkeypatch.delenv("CONSUMER_SERVICE", raising=False)
    monkeypatch.delenv("BT_CONSUMER_SERVICE", raising=False)
    assert h._consumer_service("prod") == "trading-analysis-pnl-consumer-prod"
    assert h._consumer_service("real_trade") == (
        "trading-analysis-pnl-consumer-real-trade"
    )
    assert h._consumer_service("bt") == "trading-analysis-pnl-consumer-bt"


@pytest.mark.unit
def test_handler_prod_mode_pauses_prod_service_and_passes_type(monkeypatch):
    monkeypatch.setenv("AUDIT_TYPE", "prod")
    monkeypatch.setenv("CONSUMER_SERVICE", "trading-analysis-pnl-consumer-prod")
    ecs = MagicMock()
    with (
        patch.object(h, "_ecs", return_value=ecs),
        patch.object(h, "run_audit", return_value=0) as ra,
        patch.object(h, "_fetch_secrets_into_env"),
    ):
        out = h.handler({}, None)

    argv = ra.call_args.args[0]
    assert ["--type", "prod"] == argv[1:3]
    paused = ecs.update_service.call_args_list[0]
    assert paused.kwargs["service"] == "trading-analysis-pnl-consumer-prod"
    assert out["status"] == "ok"


@pytest.mark.unit
def test_handler_pauses_fixes_resumes_in_order():
    """pause (desired 0 + wait stable) -> audit -> resume (desired 1)."""
    ecs = MagicMock()
    order: list[str] = []
    ecs.update_service.side_effect = lambda **kw: order.append(
        f"desired={kw['desiredCount']}"
    )

    def _audit(argv):
        order.append("audit")
        return 0

    with (
        patch.object(h, "_ecs", return_value=ecs),
        patch.object(h, "run_audit", side_effect=_audit) as ra,
        patch.object(h, "_fetch_secrets_into_env"),
    ):
        out = h.handler({}, None)

    assert order == ["desired=0", "audit", "desired=1"]
    ecs.get_waiter.assert_called_once_with("services_stable")
    assert out["status"] == "ok"
    # window bounds passed through to audit argv
    argv = ra.call_args.args[0]
    assert "--fix-window" in argv


@pytest.mark.unit
def test_handler_resumes_even_when_audit_fails():
    ecs = MagicMock()
    with (
        patch.object(h, "_ecs", return_value=ecs),
        patch.object(h, "run_audit", side_effect=RuntimeError("boom")),
        patch.object(h, "_fetch_secrets_into_env"),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            h.handler({}, None)

    # resumed despite the failure
    resume_calls = [
        c for c in ecs.update_service.call_args_list if c.kwargs["desiredCount"] == 1
    ]
    assert len(resume_calls) == 1


@pytest.mark.unit
def test_handler_dry_run_skips_ecs_entirely():
    ecs = MagicMock()
    with (
        patch.object(h, "_ecs", return_value=ecs),
        patch.object(h, "run_audit", return_value=0) as ra,
        patch.object(h, "_fetch_secrets_into_env"),
    ):
        out = h.handler({"dry_run": True}, None)

    ecs.update_service.assert_not_called()
    assert "--dry-run" in ra.call_args.args[0]
    assert out["status"] == "ok"


@pytest.mark.unit
def test_handler_nonzero_audit_rc_raises_and_resumes():
    ecs = MagicMock()
    with (
        patch.object(h, "_ecs", return_value=ecs),
        patch.object(h, "run_audit", return_value=2),
        patch.object(h, "_fetch_secrets_into_env"),
    ):
        with pytest.raises(RuntimeError, match="rc=2"):
            h.handler({}, None)
    resume_calls = [
        c for c in ecs.update_service.call_args_list if c.kwargs["desiredCount"] == 1
    ]
    assert len(resume_calls) == 1
