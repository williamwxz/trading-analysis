"""Tests for the ordering guarantees in scripts/provision_grafana_alerts.sh."""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/provision_grafana_alerts.sh"


def test_script_is_valid_bash():
    subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


def test_retired_rules_are_deleted_after_the_upsert_not_before():
    """Deleting first means a transient upsert failure leaves no position
    divergence coverage — and the CI step is continue-on-error, so the outage
    would persist silently until a later successful run."""
    body = SCRIPT.read_text()
    upsert = body.index("==> Upserting alert rules")
    delete = body.index("==> Deleting retired alert rules")
    assert upsert < delete, "the delete pass must run after the upsert pass"


def test_delete_is_guarded_on_the_replacement_being_installed():
    body = SCRIPT.read_text()
    guard = re.search(
        r'if \[\[ "\$replacement_ok" == 1 \]\]; then\s*\n\s*for uid in ([^\n;]+); do',
        body,
    )
    assert guard, "delete loop must be guarded by replacement_ok"
    assert "divergence-bt-prod" in guard.group(1)
    assert "divergence-prod-rt" in guard.group(1)


def test_replacement_flag_is_only_set_on_a_successful_upsert():
    body = SCRIPT.read_text()
    assert 'REPLACEMENT_UID="divergence-bt-prod-per-underlying"' in body
    assert "replacement_ok=0" in body
    # the flag is set inside the `if ok "$code"` branch, not unconditionally
    ok_branch = body.index('if ok "$code"; then\n    echo "   OK  $title')
    flag_set = body.index('[[ "$uid" == "$REPLACEMENT_UID" ]] && replacement_ok=1')
    err_branch = body.index('echo "   ERROR $title HTTP $code')
    assert ok_branch < flag_set < err_branch
