"""Tests for the Telegram contact point's message template.

Grafana's DEFAULT Telegram template renders, per alert instance: the value, the
full label set, every annotation, a Source link and a Silence link (a long query
string). A single breaching coin fires TOP_N instances at once and they group
into one notification, so the default produced ~90 lines for what is really five
short facts. The custom template collapses that to one line each.

The template was verified by executing it against Go's text/template with
Alertmanager-shaped data for four cases: normal multi-instance firing, an
evaluation error, a synthetic DatasourceNoData alert carrying no annotations,
and a resolve. These tests pin the structural properties that verification
established, since CI runs pytest only and cannot re-execute a Go template.
"""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALERTING = ROOT / "infra/grafana/alerting"
CONTACT_POINT = ALERTING / "contact-point-telegram.json"


def _settings():
    return json.loads(CONTACT_POINT.read_text())["settings"]


def test_contact_point_defines_a_custom_message_template():
    assert _settings().get("message"), (
        "without an explicit message the Grafana default renders a Source link, "
        "a long Silence URL and the whole label set per instance"
    )


def test_template_renders_one_line_per_alert_from_summary():
    msg = _settings()["message"]
    assert "{{ range .Alerts.Firing }}" in msg
    assert "{{ range .Alerts.Resolved }}" in msg


def test_template_surfaces_evaluation_errors():
    """When a rule hits execErrState/noDataState Alerting, Grafana attaches an
    `Error` annotation and renders the rule's own summary as `[no value]`. That
    Error text is the only diagnostic in the notification — dropping it is how a
    broken rule becomes undebuggable from Telegram alone."""
    msg = _settings()["message"]
    assert '{{ with index .Annotations "Error" }}' in msg
    assert "{{ else }}" in msg, "the summary path must be the `with` else-branch"


def test_template_falls_back_to_labels_when_summary_is_absent():
    """A synthetic DatasourceNoData alert carries no rule annotations at all, so
    a summary-only template would notify a blank line."""
    msg = _settings()["message"]
    assert msg.count("{{ if .Annotations.summary }}") == 2  # firing + resolved
    assert msg.count("{{ .Labels }}") == 2


def test_template_does_not_render_the_noisy_default_fields():
    msg = _settings()["message"]
    for noisy in (".SilenceURL", ".GeneratorURL", ".DashboardURL", ".PanelURL"):
        assert noisy not in msg


def test_template_only_uses_fields_every_routed_rule_provides():
    """This contact point serves the divergence AND source-freshness rules, so the
    template must not depend on anything only one family defines. All of them set
    `summary`; anything without one degrades to the label fallback, not a blank."""
    for rules_file in ("rules-divergence.json", "rules-source-freshness.json"):
        for rule in json.loads((ALERTING / rules_file).read_text()):
            assert rule["annotations"].get("summary"), (
                f"{rule['uid']} has no summary; give it a one-line summary or it "
                f"will notify as a raw label dump"
            )
    assert ".CommonLabels.alertname" in _settings()["message"]


def test_plain_text_because_error_text_is_untrusted_markup():
    """Telegram's HTML mode rejects the whole message on an unknown tag, so a
    datasource error containing `<` (this repo's SQL has `rn <= 5`) would mean
    NO notification delivered at exactly the moment something is broken."""
    settings = _settings()
    assert settings["parse_mode"] == "None"
    assert not re.search(r"</?[a-z]+>", settings["message"])
