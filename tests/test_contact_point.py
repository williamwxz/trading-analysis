"""Tests for the Telegram contact point's message template.

Grafana's DEFAULT Telegram template renders, per alert instance: the value, the
full label set, every annotation, a Source link and a Silence link (which is a
long query string). A single breaching coin fires TOP_N instances at once and
they group into one notification, so the default produced ~90 lines for what is
really five short facts. The custom template collapses that to one line each.
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
    assert msg.count("{{ .Annotations.summary }}") == 2  # firing + resolved


def test_template_does_not_render_labels_or_silence_urls():
    """Those are the bulk of the default template."""
    msg = _settings()["message"]
    for noisy in (".Labels", ".SilenceURL", ".GeneratorURL", ".DashboardURL"):
        assert noisy not in msg


def test_template_only_uses_fields_every_routed_rule_provides():
    """This contact point serves the divergence AND source-freshness rules, so the
    template must not depend on anything only one family defines. All of them set
    `summary`; none of the referenced fields are rule-specific."""
    msg = _settings()["message"]
    for rules_file in ("rules-divergence.json", "rules-source-freshness.json"):
        for rule in json.loads((ALERTING / rules_file).read_text()):
            assert rule["annotations"].get("summary"), (
                f"{rule['uid']} has no summary; the Telegram template renders "
                f"only .Annotations.summary, so it would notify an empty line"
            )
    assert ".CommonLabels.alertname" in msg


def test_parse_mode_html_is_matched_by_the_markup_used():
    settings = _settings()
    assert settings["parse_mode"] == "HTML"
    msg = settings["message"]
    # Only <b> is used; Telegram's HTML mode rejects unknown tags outright.
    tags = {t.strip("</>") for t in re.findall(r"</?[a-z]+>", msg)}
    assert tags <= {"b", "i", "code", "pre", "a", "u", "s"}, tags
