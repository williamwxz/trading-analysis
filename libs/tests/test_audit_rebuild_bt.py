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
