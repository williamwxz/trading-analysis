"""Cold-start walk-verify must tolerate late-revision drift, not crash on it.

The bootstrap walk recomputes recent cumulative_pnl and compares it to the stored
value. Before, any deviation > 0.2% aborted the bootstrap. But late-arriving
revisions routinely shift recent cpnl by a fraction of a percent now that the
Dagster batch recompute (which reconciled them) is gone — so on every restart the
walk found ~0.3-1% drift on data the consumer itself had just written, and
crash-looped. The consumer already resumes from the *stored* values regardless of
this check, so aborting never fixed anything; it only prevented recovery.

These tests pin the new behavior: tolerate routine drift (warn), abort only on a
gross deviation that signals real corruption. bt skips the check entirely; prod/rt
now warn on drift and abort only when clearly broken.
"""

import pytest

from pnl_consumer.pnl_consumer import (
    _PNL_ABORT_TOLERANCE,
    _PNL_WARN_TOLERANCE,
    _walk_deviation_action,
)


@pytest.mark.unit
def test_below_warn_is_ok():
    assert _walk_deviation_action(0.0) == "ok"
    assert _walk_deviation_action(_PNL_WARN_TOLERANCE / 2) == "ok"


@pytest.mark.unit
def test_routine_late_revision_drift_warns_not_aborts():
    """Deviations of the magnitude actually observed in production (0.3%-1.1%)
    must WARN, never ABORT — otherwise the consumer crash-loops on restart."""
    for drift in (0.003, 0.0108, 0.05):
        assert _walk_deviation_action(drift) == "warn", drift


@pytest.mark.unit
def test_gross_deviation_aborts():
    """A gross deviation (corruption-scale, far above routine drift) still aborts."""
    assert _walk_deviation_action(_PNL_ABORT_TOLERANCE * 2) == "abort"
    assert _walk_deviation_action(5.0) == "abort"


@pytest.mark.unit
def test_abort_threshold_well_above_observed_drift():
    """The abort threshold must sit comfortably above the worst routine drift
    seen in production (~1.1%) so normal operation never trips it."""
    assert _PNL_ABORT_TOLERANCE >= 0.1
