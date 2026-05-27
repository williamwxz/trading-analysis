"""Real-trade revision-guard correctness after restore.

After restoring an AnchorState from checkpoint, the revision guard
(bar_ts, revision_ts) tuple comparison must continue to work exactly as
in-memory. These tests construct an AnchorState as if just restored and feed
new revisions into should_apply_revision, asserting accept/reject decisions.
"""

from datetime import UTC, datetime

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState

pytestmark = pytest.mark.unit


_BAR_10 = datetime(2024, 6, 1, 10, 0, tzinfo=UTC)
_BAR_11 = datetime(2024, 6, 1, 11, 0, tzinfo=UTC)
_BAR_9 = datetime(2024, 6, 1, 9, 0, tzinfo=UTC)

_REV_T0 = datetime(2024, 6, 1, 10, 0, 0, tzinfo=UTC)
_REV_T1 = datetime(2024, 6, 1, 10, 0, 30, tzinfo=UTC)
_REV_TM1 = datetime(2024, 6, 1, 9, 59, 30, tzinfo=UTC)


def _restored_state_at(bar_ts, revision_ts) -> AnchorState:
    state = AnchorState()
    state.set(
        "strat_a",
        AnchorRecord(
            pnl=1.0,
            price=100.0,
            position=1.0,
            bar_ts=bar_ts,
            revision_ts=revision_ts,
            strategy_id=1,
            strategy_name="s",
            underlying="BTC",
            config_timeframe="1m",
            weighting=1.0,
            strategy_instance_id="i",
            final_signal=0.0,
            benchmark=0.0,
        ),
    )
    return state


def test_case_1_older_revision_same_bar_rejected():
    state = _restored_state_at(_BAR_10, _REV_T0)
    # Old revision_ts (T-1) for the same bar
    assert state.should_apply_revision("strat_a", _BAR_10, _REV_TM1) is False


def test_case_2_same_revision_rejected():
    state = _restored_state_at(_BAR_10, _REV_T0)
    assert state.should_apply_revision("strat_a", _BAR_10, _REV_T0) is False


def test_case_3_newer_revision_same_bar_accepted():
    state = _restored_state_at(_BAR_10, _REV_T0)
    assert state.should_apply_revision("strat_a", _BAR_10, _REV_T1) is True


def test_case_4_newer_bar_older_revision_ts_accepted():
    """Tuple comparison: (bar=11, rev=09:59:30) > (bar=10, rev=10:00:00). Accept."""
    state = _restored_state_at(_BAR_10, _REV_T0)
    assert state.should_apply_revision("strat_a", _BAR_11, _REV_TM1) is True


def test_case_5_older_bar_newer_revision_ts_rejected():
    """(bar=9, rev=10:00:30) < (bar=10, rev=10:00:00). Reject."""
    state = _restored_state_at(_BAR_10, _REV_T0)
    assert state.should_apply_revision("strat_a", _BAR_9, _REV_T1) is False


def test_case_6_uninitialized_guard_accepts_first_revision():
    """Sanity-check would reject a real-trade checkpoint with min sentinels,
    but in-memory the tuple comparison correctly accepts the first revision."""
    state = AnchorState()
    # AnchorRecord defaults: bar_ts = revision_ts = datetime.min (naive).
    state.set("strat_a", AnchorRecord(strategy_instance_id="i"))
    assert state.should_apply_revision("strat_a", _BAR_10, _REV_T0) is True


def test_case_7_restore_then_serialize_then_compare_tuple():
    """Restore a state, serialize via compute_state_hash, restore another with same
    values — the tuple comparison invariant still holds for both."""
    from libs.computation.checkpoint_store import compute_state_hash

    s1 = _restored_state_at(_BAR_10, _REV_T0)
    s2 = _restored_state_at(_BAR_10, _REV_T0)
    assert compute_state_hash(s1) == compute_state_hash(s2)
    # Both reject the same older revision.
    assert s1.should_apply_revision("strat_a", _BAR_10, _REV_TM1) is False
    assert s2.should_apply_revision("strat_a", _BAR_10, _REV_TM1) is False
