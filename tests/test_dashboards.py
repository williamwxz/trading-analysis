"""Structural tests for the Grafana dashboard JSON files."""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DASH = ROOT / "infra/grafana/dashboards"
sys.path.insert(0, str(ROOT / "infra/grafana/alerting"))

import gen_rules  # noqa: E402


def _dash(name):
    return json.loads((DASH / name).read_text())


@pytest.mark.parametrize("path", sorted(DASH.glob("*.json")), ids=lambda p: p.name)
def test_dashboard_json_is_valid_and_panel_ids_unique(path):
    d = json.loads(path.read_text())
    d = d.get("dashboard", d)
    assert d.get("uid"), f"{path.name} has no uid"
    assert d.get("title"), f"{path.name} has no title"
    ids = [p["id"] for p in d.get("panels", [])]
    assert len(ids) == len(set(ids)), f"{path.name} has duplicate panel ids: {ids}"


def _divergence_panel():
    d = _dash("strategy-pnl-l4-underlying.json")
    return next(p for p in d["panels"] if p["id"] == 7)


def test_l4_has_a_position_divergence_panel():
    p = _divergence_panel()
    assert p["title"] == "Position Divergence — ${underlying}"
    assert p["type"] == "timeseries"
    assert p["gridPos"] == {"x": 0, "y": 34, "w": 24, "h": 7}


def test_l4_divergence_panel_does_not_plot_prod_rt():
    """prod-rt position comparison was retired; it must not reappear here."""
    sqls = " ".join(t["rawSql"] for t in _divergence_panel()["targets"])
    assert "real_trade" not in sqls


def test_l4_divergence_panel_compares_bt_against_prod():
    sqls = " ".join(t["rawSql"] for t in _divergence_panel()["targets"])
    for tbl in (
        "strategy_pnl_1min_bt_v2",
        "strategy_pnl_1hour_bt_v2",
        "strategy_pnl_1day_bt_v2",
        "strategy_pnl_1min_prod_v2",
        "strategy_pnl_1hour_prod_v2",
        "strategy_pnl_1day_prod_v2",
    ):
        assert tbl in sqls, f"missing {tbl}"
    assert "Backtest − Production" in sqls


def test_l4_divergence_panel_is_filtered_to_the_selected_underlying():
    sqls = " ".join(t["rawSql"] for t in _divergence_panel()["targets"])
    assert "${underlying}" in sqls


def test_l4_threshold_lines_match_the_alert_thresholds():
    """The reference lines must not drift from gen_rules."""
    sqls = " ".join(t["rawSql"] for t in _divergence_panel()["targets"])
    for coin, thr in gen_rules.THRESHOLDS.items():
        assert f"WHEN '{coin}' THEN {thr:g}" in sqls, f"{coin} threshold missing/stale"
    assert f"ELSE {gen_rules.DEFAULT_THRESHOLD:g} END" in sqls


def test_l4_threshold_series_are_symmetric():
    """The rule fires on |Δ|, so the panel needs both +T and −T reference lines."""
    sqls = " ".join(t["rawSql"] for t in _divergence_panel()["targets"])
    assert "Threshold +" in sqls
    assert "Threshold −" in sqls
