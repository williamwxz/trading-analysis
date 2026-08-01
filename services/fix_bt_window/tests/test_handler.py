"""Unit tests for the fix-window Lambda handler.

Orchestration only — audit_pnl invocation and ECS are mocked. The handler's
contract: pause the mode's consumer, run audit_pnl --type <mode> --fix-window
over the trailing window, resume the consumer even on failure.

The handler is parameterized by AUDIT_TYPE so one image backs three scheduled
functions (bt / prod / real_trade). prod and real_trade additionally use an
arrival-driven start bound — see the `arrival` tests below for why.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fix_bt_window import handler as h

_NOW = datetime(2026, 7, 9, 15, 0, 37)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Each test starts from the bt default; opt in to other modes explicitly."""
    for k in (
        "AUDIT_TYPE",
        "CONSUMER_SERVICE",
        "BT_CONSUMER_SERVICE",
        "LOOKBACK_HOURS",
        "ARRIVAL_WINDOW_HOURS",
        "MAX_LOOKBACK_HOURS",
    ):
        monkeypatch.delenv(k, raising=False)


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
    assert ["--type", "bt"] == argv[1:3]
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


# ── mode parameterization (AUDIT_TYPE) ──────────────────────────────────────


@pytest.mark.unit
def test_audit_type_defaults_to_bt():
    assert h.audit_type() == "bt"


@pytest.mark.unit
@pytest.mark.parametrize("mode", ["bt", "prod", "real_trade"])
def test_audit_type_from_env(monkeypatch, mode):
    monkeypatch.setenv("AUDIT_TYPE", mode)
    assert h.audit_type() == mode


@pytest.mark.unit
def test_audit_type_rejects_unknown_mode(monkeypatch):
    """A typo'd AUDIT_TYPE must fail loudly, not silently repair the wrong table."""
    monkeypatch.setenv("AUDIT_TYPE", "prd")
    with pytest.raises(ValueError, match="prd"):
        h.audit_type()


@pytest.mark.unit
@pytest.mark.parametrize(
    "mode,expected",
    [
        ("bt", "trading-analysis-pnl-consumer-bt"),
        ("prod", "trading-analysis-pnl-consumer-prod"),
        ("real_trade", "trading-analysis-pnl-consumer-real-trade"),
    ],
)
def test_consumer_service_derived_from_mode(monkeypatch, mode, expected):
    """real_trade underscores must map to the hyphenated ECS service name."""
    monkeypatch.setenv("AUDIT_TYPE", mode)
    assert h.consumer_service() == expected


@pytest.mark.unit
def test_consumer_service_explicit_override_wins(monkeypatch):
    monkeypatch.setenv("AUDIT_TYPE", "prod")
    monkeypatch.setenv("CONSUMER_SERVICE", "some-other-service")
    assert h.consumer_service() == "some-other-service"


@pytest.mark.unit
def test_consumer_service_honours_legacy_bt_env(monkeypatch):
    """BT_CONSUMER_SERVICE predates this change — keep it working for bt."""
    monkeypatch.setenv("BT_CONSUMER_SERVICE", "legacy-bt-svc")
    assert h.consumer_service() == "legacy-bt-svc"


@pytest.mark.unit
def test_build_argv_uses_audit_type(monkeypatch):
    monkeypatch.setenv("AUDIT_TYPE", "real_trade")
    argv = h.build_argv("2026-07-07 14:00:00", "2026-07-09 15:00:00", dry_run=False)
    assert ["--type", "real_trade"] == argv[1:3]


# ── arrival-driven start bound (prod / real_trade) ──────────────────────────
#
# 2026-07-28: the upstream publisher stalled 4h, then backfilled ~168k revisions
# carrying bars up to 144h old. A fixed 49h lookback silently misses those bars,
# so the stale minutes never get repaired. The start bound must reach back to the
# oldest bar whose revision actually landed since the last run.


@pytest.mark.unit
def test_arrival_start_extends_window_past_fixed_lookback(monkeypatch):
    """A 144h-old bar revised in the last run interval pulls the start back."""
    monkeypatch.setenv("AUDIT_TYPE", "prod")
    oldest = datetime(2026, 7, 3, 15, 0, 0)  # ~144h before _NOW
    ws, we = h.compute_window(_NOW, {}, arrival_probe=lambda *_: oldest)
    assert ws == "2026-07-03 15:00:00"
    assert we == "2026-07-09 15:00:00"


@pytest.mark.unit
def test_arrival_start_never_shrinks_below_default_lookback(monkeypatch):
    """Quiet interval (only fresh bars revised) still repairs the 49h tail."""
    monkeypatch.setenv("AUDIT_TYPE", "prod")
    recent = datetime(2026, 7, 9, 14, 0, 0)  # 1h old — newer than now-49h
    ws, _ = h.compute_window(_NOW, {}, arrival_probe=lambda *_: recent)
    assert ws == "2026-07-07 14:00:00"  # the 49h floor, not the 1h arrival


@pytest.mark.unit
def test_arrival_start_capped_by_max_lookback(monkeypatch):
    """A full-table re-push must not trigger a six-month recompute."""
    monkeypatch.setenv("AUDIT_TYPE", "prod")
    monkeypatch.setenv("MAX_LOOKBACK_HOURS", "336")  # 14 days
    ancient = datetime(2026, 1, 1, 0, 0, 0)
    ws, _ = h.compute_window(_NOW, {}, arrival_probe=lambda *_: ancient)
    assert ws == "2026-06-25 15:00:00"  # exactly 336h before _NOW


@pytest.mark.unit
def test_arrival_probe_not_consulted_for_bt(monkeypatch):
    """bt sources positions from the cum table, not revisions — fixed 49h stands."""
    monkeypatch.setenv("AUDIT_TYPE", "bt")
    probe = MagicMock()
    ws, _ = h.compute_window(_NOW, {}, arrival_probe=probe)
    probe.assert_not_called()
    assert ws == "2026-07-07 14:00:00"


@pytest.mark.unit
def test_explicit_window_start_skips_arrival_probe(monkeypatch):
    """An operator-supplied window is authoritative."""
    monkeypatch.setenv("AUDIT_TYPE", "prod")
    probe = MagicMock()
    ws, _ = h.compute_window(
        _NOW, {"window_start": "2026-07-01 00:00:00"}, arrival_probe=probe
    )
    probe.assert_not_called()
    assert ws == "2026-07-01 00:00:00"


@pytest.mark.unit
def test_arrival_probe_failure_falls_back_to_fixed_lookback(monkeypatch):
    """A ClickHouse hiccup must degrade to the old behaviour, not abort the run."""
    monkeypatch.setenv("AUDIT_TYPE", "prod")

    def _boom(*_):
        raise RuntimeError("clickhouse unreachable")

    ws, _ = h.compute_window(_NOW, {}, arrival_probe=_boom)
    assert ws == "2026-07-07 14:00:00"


@pytest.mark.unit
def test_arrival_probe_none_falls_back_to_fixed_lookback(monkeypatch):
    """No revisions since the last run → nothing extra to repair."""
    monkeypatch.setenv("AUDIT_TYPE", "prod")
    ws, _ = h.compute_window(_NOW, {}, arrival_probe=lambda *_: None)
    assert ws == "2026-07-07 14:00:00"


@pytest.mark.unit
def test_handler_pauses_the_mode_specific_consumer(monkeypatch):
    monkeypatch.setenv("AUDIT_TYPE", "real_trade")
    ecs = MagicMock()
    with (
        patch.object(h, "_ecs", return_value=ecs),
        patch.object(h, "run_audit", return_value=0),
        patch.object(h, "_fetch_secrets_into_env"),
        patch.object(h, "_probe_oldest_revised_bar", return_value=None),
    ):
        h.handler({}, None)
    services = {c.kwargs["service"] for c in ecs.update_service.call_args_list}
    assert services == {"trading-analysis-pnl-consumer-real-trade"}
