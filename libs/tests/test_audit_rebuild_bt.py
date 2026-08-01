"""Unit tests for the bt full-rebuild chunking helper in scripts/audit_pnl.py.

Pure-Python (no ClickHouse): only _month_chunks is exercised here. The DB-backed
pieces (_bt_rebuild_fixes, rebuild_bt) are covered by the live dry-run smoke.
"""

from datetime import datetime

import pytest

from scripts.audit_pnl import _month_chunks


@pytest.mark.unit
def test_month_chunks_spans_calendar_months_half_open():
    chunks = _month_chunks(datetime(2026, 2, 27), datetime(2026, 4, 3, 5, 0))
    assert chunks == [
        (datetime(2026, 2, 27), datetime(2026, 3, 1)),
        (datetime(2026, 3, 1), datetime(2026, 4, 1)),
        (datetime(2026, 4, 1), datetime(2026, 4, 3, 5, 0)),
    ]


@pytest.mark.unit
def test_month_chunks_single_partial_month():
    chunks = _month_chunks(datetime(2026, 6, 10), datetime(2026, 6, 25))
    assert chunks == [(datetime(2026, 6, 10), datetime(2026, 6, 25))]


@pytest.mark.unit
def test_month_chunks_crosses_year_boundary():
    chunks = _month_chunks(datetime(2026, 12, 15), datetime(2027, 1, 10))
    assert chunks == [
        (datetime(2026, 12, 15), datetime(2027, 1, 1)),
        (datetime(2027, 1, 1), datetime(2027, 1, 10)),
    ]


@pytest.mark.unit
def test_month_chunks_empty_when_start_ge_end():
    assert _month_chunks(datetime(2026, 6, 10), datetime(2026, 6, 10)) == []


# ── fetch_seed_anchor: unbounded fallback ───────────────────────────────────
#
# The seed anchor is what makes a repaired window continue the existing PnL
# chain instead of restarting it. It used a hard 7-day lower bound and returned
# a ZERO anchor when nothing was found — so a strategy with no rows for longer
# than that would silently restart cumulative_pnl from 0 on the next repair.
# In practice carry-forward writes a row every minute so the bound is never
# reached, but carry-forward has stopped before (metadata-less anchor records,
# incident 2026-07-13), and that is exactly when a reset would be worst.


class _FakeClient:
    """Returns a queued result per query, recording the SQL it saw."""

    def __init__(self, results):
        self._results, self.queries = list(results), []

    def query(self, sql):
        self.queries.append(sql)

        class _R:
            result_rows = self._results.pop(0) if self._results else []

        return _R()


@pytest.mark.unit
def test_fetch_seed_anchor_uses_bounded_query_when_it_hits():
    """The fast bounded query answers; no second round-trip."""
    from scripts.audit_pnl import fetch_seed_anchor

    c = _FakeClient([[(1.5, 100.0, 1.0)]])
    got = fetch_seed_anchor("prod", "sid=1|x", datetime(2026, 7, 25), c)
    assert got == {"pnl": 1.5, "price": 100.0, "position": 1.0}
    assert len(c.queries) == 1
    assert "INTERVAL 7 DAY" in c.queries[0]


@pytest.mark.unit
def test_fetch_seed_anchor_falls_back_to_unbounded_lookback():
    """Nothing within 7 days -> look as far back as the table goes, and use it."""
    from scripts.audit_pnl import fetch_seed_anchor

    c = _FakeClient([[], [(0.25, 50.0, -1.0)]])
    got = fetch_seed_anchor("prod", "sid=1|x", datetime(2026, 7, 25), c)
    assert got == {"pnl": 0.25, "price": 50.0, "position": -1.0}
    assert len(c.queries) == 2
    # the retry must drop the lower bound entirely
    assert "INTERVAL 7 DAY" not in c.queries[1]


@pytest.mark.unit
def test_fetch_seed_anchor_zero_only_when_strategy_has_no_history_at_all():
    from scripts.audit_pnl import fetch_seed_anchor

    c = _FakeClient([[], []])
    got = fetch_seed_anchor("prod", "brand-new", datetime(2026, 7, 25), c)
    assert got == {"pnl": 0.0, "price": 0.0, "position": 0.0}
    assert len(c.queries) == 2


@pytest.mark.unit
def test_fetch_seed_anchor_null_row_triggers_fallback():
    """ClickHouse returns a one-row all-NULL result for an empty aggregate."""
    from scripts.audit_pnl import fetch_seed_anchor

    c = _FakeClient([[(None, None, None)], [(2.0, 10.0, 1.0)]])
    got = fetch_seed_anchor("prod", "sid=1|x", datetime(2026, 7, 25), c)
    assert got == {"pnl": 2.0, "price": 10.0, "position": 1.0}
    assert len(c.queries) == 2
