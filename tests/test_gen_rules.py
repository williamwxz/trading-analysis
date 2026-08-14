"""Unit tests for the divergence alert-rule generator (no ClickHouse, no Grafana)."""

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALERTING = ROOT / "infra/grafana/alerting"
sys.path.insert(0, str(ALERTING))

import gen_rules  # noqa: E402

RULES_JSON = ALERTING / "rules-divergence.json"


def _rules():
    return json.loads(RULES_JSON.read_text())


def test_thresholds_cover_all_eight_underlyings():
    assert gen_rules.THRESHOLDS == {
        "BTC": 0.15,
        "ETH": 0.15,
        "SOL": 0.20,
        "ADA": 0.25,
        "DOGE": 0.25,
        "AVAX": 0.30,
        "XRP": 0.30,
        "FET": 0.40,
    }
    assert gen_rules.DEFAULT_THRESHOLD == 0.25


def test_threshold_case_emits_every_coin_and_a_default():
    case = gen_rules.threshold_case("u")
    for coin, thr in gen_rules.THRESHOLDS.items():
        assert f"WHEN '{coin}' THEN {thr:g}" in case
    assert f"ELSE {gen_rules.DEFAULT_THRESHOLD:g} END" in case


def test_ranking_has_a_deterministic_tie_break():
    """Many strategies sit at exactly zero contribution. Without a second sort key
    row_number() is arbitrary among ties, top-5 membership churns every evaluation,
    and the `for` timer never matures — the alert would never fire."""
    sql = gen_rules.per_underlying_sql()
    assert "ORDER BY contrib DESC, s ASC" in sql


def test_ranking_window_is_longer_than_value_window():
    """Membership must hold still for the 10 minutes the `for` timer needs."""
    assert gen_rules.RANK_WINDOW_MIN > gen_rules.WINDOW_MIN
    assert gen_rules.RANK_WINDOW_MIN == 60
    assert gen_rules.WINDOW_MIN == 17


def test_sql_selects_the_instance_dimensions():
    sql = gen_rules.per_underlying_sql()
    assert "AS underlying" in sql
    assert "AS sid" in sql
    assert "AS sno" in sql
    assert "extract(top.s, 'sid=([0-9]+)')" in sql
    assert "extract(top.s, 'sno=([0-9]+)')" in sql


def test_sql_keeps_the_completeness_gate():
    assert "countDistinct(underlying) = 8" in gen_rules.per_underlying_sql()


def test_sql_limits_to_top_n_per_underlying():
    assert f"WHERE rn <= {gen_rules.TOP_N}" in gen_rules.per_underlying_sql()


def test_rule_condition_is_a_flat_ratio_threshold():
    """Per-coin thresholds live in the SQL as a ratio, so the Grafana threshold is 1."""
    rule = gen_rules.per_underlying_rule()
    threshold = next(d for d in rule["data"] if d["refId"] == "C")
    evaluator = threshold["model"]["conditions"][0]["evaluator"]
    assert evaluator["type"] == "gt"
    assert evaluator["params"] == [1.0]
    assert rule["condition"] == "C"


def test_rule_is_sustained_and_routed_to_telegram():
    rule = gen_rules.per_underlying_rule()
    assert rule["for"] == "10m"
    assert rule["noDataState"] == "OK"
    assert rule["execErrState"] == "Alerting"
    assert rule["notification_settings"] == {"receiver": "telegram-divergence"}
    assert rule["ruleGroup"] == "divergence"


def test_generated_file_has_exactly_the_two_expected_rules():
    uids = [r["uid"] for r in _rules()]
    assert uids == ["divergence-bt-prod-per-underlying", "divergence-pnl-prod-rt"]


def test_retired_rules_are_gone_from_the_generated_file():
    uids = {r["uid"] for r in _rules()}
    for uid in gen_rules.RETIRED_UIDS:
        assert uid not in uids
    assert gen_rules.RETIRED_UIDS == ["divergence-bt-prod", "divergence-prod-rt"]


def test_pnl_rule_is_untouched():
    pnl = next(r for r in _rules() if r["uid"] == "divergence-pnl-prod-rt")
    assert pnl["title"] == "PnL divergence: Production − Real-Trade"
    assert pnl["labels"]["metric"] == "cumulative_pnl"
    threshold = next(d for d in pnl["data"] if d["refId"] == "C")
    evaluator = threshold["model"]["conditions"][0]["evaluator"]
    assert evaluator["type"] == "outside_range"
    assert evaluator["params"] == [-0.015, 0.015]


def test_generated_file_is_in_sync_with_the_generator():
    """rules-divergence.json is generated; re-running must be a no-op."""
    before = RULES_JSON.read_text()
    subprocess.run(
        [sys.executable, str(ALERTING / "gen_rules.py")],
        check=True,
        capture_output=True,
    )
    assert RULES_JSON.read_text() == before, "run gen_rules.py and commit the result"
