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
    assert "countDistinct(underlying) = (" in gen_rules.per_underlying_sql()


def test_completeness_gate_is_not_hardcoded_to_a_roster_size():
    """An exact `= 8` rejects every minute once a ninth instrument is onboarded.
    With noDataState=OK that silently disables alerting instead of failing loudly,
    and makes DEFAULT_THRESHOLD unreachable. Both rules must derive the count."""
    for sql in (
        gen_rules.per_underlying_sql(),
        next(d for d in gen_rules.pnl_rule()["data"] if d["refId"] == "A")["model"][
            "rawSql"
        ],
    ):
        assert "countDistinct(underlying) = 8" not in sql
        assert "countDistinct(underlying) = (SELECT countDistinct(underlying)" in sql


def test_roster_size_is_scoped_to_the_same_window_as_the_gate():
    sql = gen_rules.roster_size("strategy_pnl_1min_prod_v2")
    assert f"INTERVAL {gen_rules.WINDOW_MIN} MINUTE" in sql
    assert "FINAL" not in sql
    assert "FINAL" in gen_rules.roster_size("strategy_pnl_1min_prod_v2", final=True)


def test_sql_limits_to_top_n_per_underlying():
    assert f"WHERE rn <= {gen_rules.TOP_N}" in gen_rules.per_underlying_sql()


def test_per_underlying_query_uses_timeseries_format_not_table():
    """The result carries string DIMENSION columns, i.e. a `long` frame. Grafana
    server-side expressions reject those outright:

        [sse.readDataError] [A] got error: input data must be a wide series
        but got type long

    format 0 (FormatOptionTimeSeries) runs LongToWide and attaches the string
    columns as field Labels — that is what produces the underlying/sid/sno alert
    labels. format 1 (FormatOptionTable) passes the long frame through and every
    evaluation errors with all annotations rendering `[no value]`.
    """
    q = next(d for d in gen_rules.per_underlying_rule()["data"] if d["refId"] == "A")
    assert q["model"]["format"] == 0
    assert gen_rules.FORMAT_TIME_SERIES == 0
    assert gen_rules.FORMAT_TABLE == 1


def test_per_underlying_query_range_covers_the_ranking_window():
    """Ranking reads 60 minutes back; the declared range must not understate it."""
    q = next(d for d in gen_rules.per_underlying_rule()["data"] if d["refId"] == "A")
    assert q["relativeTimeRange"]["from"] >= gen_rules.RANK_WINDOW_MIN * 60


def test_pnl_query_stays_table_format():
    """It returns a single `time, value` series, which is already wide."""
    q = next(d for d in gen_rules.pnl_rule()["data"] if d["refId"] == "A")
    assert q["model"]["format"] == 1


def test_summary_names_the_offending_strategy():
    """The whole point of the rule: the notification must identify who diverged."""
    summary = gen_rules.per_underlying_rule()["annotations"]["summary"]
    for label in ("underlying", "sid", "sno"):
        assert f"{{{{ $labels.{label} }}}}" in summary
    assert "{{ $values.B }}" in summary


def test_no_static_annotation_repeated_per_instance():
    """All TOP_N instances of a breaching coin fire together and Grafana groups
    them into one notification, so a static annotation is repeated TOP_N times on
    top of a full label set per block. Keep the threshold table in the README."""
    annotations = gen_rules.per_underlying_rule()["annotations"]
    assert set(annotations) == {"summary"}


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
