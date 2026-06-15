"""Unit tests for the position-divergence detector (pure logic, no ClickHouse)."""

from datetime import datetime, timedelta

import pytest

from scripts.divergence_alert import Breach, detect, format_alert


def _series(prod, rt, bt, n=15):
    """Build {mode: {minute: wpos}} from constant per-mode values over n minutes."""
    base = datetime(2026, 6, 15, 2, 0, 0)
    mins = [base + timedelta(minutes=i) for i in range(n)]
    return {
        "prod": {m: prod for m in mins},
        "real_trade": {m: rt for m in mins},
        "bt": {m: bt for m in mins},
    }


def test_no_breach_when_within_threshold():
    # bt-prod = 0.02, prod-rt = 0.0026 — both under 0.05
    s = _series(prod=0.011, rt=0.0084, bt=0.031)
    assert detect(s, threshold=0.05, window_min=15, sustained_min=10) == []


def test_breach_fires_on_sustained_bt_prod():
    s = _series(prod=0.0, rt=0.0, bt=0.10)  # bt-prod = 0.10 every minute
    breaches = detect(s, threshold=0.05, window_min=15, sustained_min=10)
    assert len(breaches) == 1
    b = breaches[0]
    assert b.pair == ("bt", "prod")
    assert b.sustained == 15 and b.window == 15
    assert b.latest == pytest.approx(0.10)


def test_no_breach_when_not_sustained():
    # only 5 of 15 minutes exceed threshold -> below sustained_min=10
    s = _series(prod=0.0, rt=0.0, bt=0.0)
    base = datetime(2026, 6, 15, 2, 0, 0)
    for i in range(5):  # last 5 minutes spike
        s["bt"][base + timedelta(minutes=10 + i)] = 0.2
    assert detect(s, threshold=0.05, window_min=15, sustained_min=10) == []


def test_negative_divergence_uses_absolute_value():
    s = _series(prod=0.0, rt=0.20, bt=0.0)  # prod-rt = -0.20
    breaches = detect(s, threshold=0.05, window_min=15, sustained_min=10)
    assert len(breaches) == 1
    assert breaches[0].pair == ("prod", "real_trade")
    assert breaches[0].peak == pytest.approx(-0.20)


def test_window_min_caps_lookback():
    # 30 minutes of data, window_min=10 -> only last 10 considered
    s = _series(prod=0.0, rt=0.0, bt=0.10, n=30)
    b = detect(s, threshold=0.05, window_min=10, sustained_min=10)[0]
    assert b.window == 10


def test_format_alert_renders_pair_and_values():
    b = Breach(pair=("bt", "prod"), sustained=12, window=15, latest=0.073, peak=0.091)
    msg = format_alert(b, threshold=0.05)
    assert "Backtest" in msg and "Production" in msg
    assert "12/15" in msg
    assert "+0.0730" in msg and "+0.0910" in msg
