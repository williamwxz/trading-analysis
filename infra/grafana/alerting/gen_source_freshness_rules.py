"""Generate Grafana alert rules that watch the *source* table, not the PnL output.

Writes infra/grafana/alerting/rules-source-freshness.json (same provisioning
contract as gen_rules.py: an array for /api/v1/provisioning/alert-rules, with
folderUID left as __FOLDER_UID__ for the script to substitute).

Why these exist
---------------
The divergence rules in gen_rules.py compare PnL modes against each other. They
cannot see the failure that actually cost us money in the week of 2026-07-24,
because when a source bar is missing the consumer carries the last position
forward and still writes a complete row for every strategy every minute. Row
counts, strategy counts, and underlying-completeness all stay perfect — the
position is just silently stale, and since cumulative_pnl is a chained anchor
quantity the error is permanent.

Three incidents that week, none of which raised anything:

  1. 07-25 05:00-21:00 (17h)  — 12 strategies stopped publishing bars
  2. 07-26 08:00-22:00 (15h)  — 87 strategies stopped publishing bars
                                (portfolio-weighted prod PnL error +0.1276%)
  3. 07-28 03:00-07:00  (4h)  — near-total publisher stall: only 16-18
                                strategies emitting revisions, then a burst of
                                ~168k revisions carrying bars up to 144h old

Incidents 1 and 2 are bar-coverage failures; incident 3 is an arrival failure
with eventually-complete coverage. They need two different detectors.

Both rules are ratio-based against a trailing baseline rather than an absolute
count, so they survive the strategy roster changing (695 total: 614 1h + 2 10m
+ 79 1d) without a threshold edit.
"""

import json
import os

DS_UID = "dfjc5vjyfcc8wf"  # grafana-clickhouse-datasource (same as L5 panels)

# Baseline lookback. 24h, not 7d: this query scans strategy_output_history_v2 on
# every evaluation and the table is large. 24h is plenty to establish "normal"
# and keeps the scan cheap.
BASELINE_HOURS = 24

# How long to let a bar-hour settle before judging its coverage. First revisions
# land p50 ~16m / p90 ~72m / p99 ~96m past bar close, so anything under ~2h is
# still legitimately filling in. 3h is comfortably past p99.
COVERAGE_SETTLE_HOURS = 3

# Arrival is judged on the previous completed hour — no settle needed, we are
# asking "did anything publish", not "did everything publish".
ARRIVAL_SETTLE_HOURS = 1

RULE_GROUP = "source-freshness"


def coverage_sql() -> str:
    """Settled bar-hour's 1h-strategy coverage / best recent hour.

    1.0 when every strategy that normally publishes did. Gap B reads 529/614 =
    0.861; the smaller Gap A reads 604/614 = 0.984.
    """
    return (
        "WITH per_hour AS ("
        "  SELECT toStartOfHour(ts) AS h,"
        "         countDistinct(strategy_table_name) AS n"
        "  FROM analytics.strategy_output_history_v2"
        f" WHERE ts >= now() - INTERVAL {BASELINE_HOURS} HOUR"
        f"   AND ts <  now() - INTERVAL {COVERAGE_SETTLE_HOURS} HOUR"
        "    AND config_timeframe = '1h'"
        "  GROUP BY h)"
        " SELECT (SELECT max(h) FROM per_hour) AS time,"
        "        (SELECT argMax(n, h) FROM per_hour)"
        "        / nullIf((SELECT max(n) FROM per_hour), 0) AS value"
    )


def arrival_sql() -> str:
    """Last completed hour's publishing breadth / median recent hour.

    Uses distinct strategies rather than raw revision count: revision volume is
    genuinely lumpy (628 to 72,097 per hour in normal operation) but breadth is
    rock-steady at 616/hour. During the 07-28 stall it collapsed to 16-18, i.e.
    a ratio of ~0.03.
    """
    return (
        "WITH per_hour AS ("
        "  SELECT toStartOfHour(revision_ts) AS h,"
        "         countDistinct(strategy_table_name) AS n"
        "  FROM analytics.strategy_output_history_v2"
        f" WHERE revision_ts >= now() - INTERVAL {BASELINE_HOURS} HOUR"
        f"   AND revision_ts <  now() - INTERVAL {ARRIVAL_SETTLE_HOURS} HOUR"
        "  GROUP BY h)"
        " SELECT (SELECT max(h) FROM per_hour) AS time,"
        "        (SELECT argMax(n, h) FROM per_hour)"
        "        / nullIf((SELECT quantile(0.5)(n) FROM per_hour), 0) AS value"
    )


# (uid, title, sql, below_threshold, for_duration, summary)
RULES = [
    (
        "source-bar-coverage",
        "Source bar coverage — strategy_output_history_v2",
        coverage_sql(),
        # 0.985 => fires at >= ~10 of 614 strategies missing. Gap A (12 missing,
        # 0.9837) fires; ordinary single-strategy jitter (0.9984) does not.
        0.985,
        "30m",
        (
            "Strategies stopped publishing 1h bars to strategy_output_history_v2. "
            "prod and real_trade are carrying stale positions forward RIGHT NOW and "
            "the PnL error is permanent once written (cumulative_pnl is chained). "
            "Check which strategies: SELECT strategy_table_name, count() FROM "
            "analytics.strategy_output_history_v2 WHERE ts >= now() - INTERVAL 6 HOUR "
            "AND config_timeframe='1h' GROUP BY 1. Escalate upstream — a recompute "
            "cannot invent bars that were never published."
        ),
    ),
    (
        "source-revision-arrival",
        "Source revision arrival — publisher stall",
        arrival_sql(),
        # 0.5 => fires only on a real collapse. The 07-28 stall read ~0.03.
        0.5,
        "1h",
        (
            "The strategy publisher has largely stopped emitting revisions. "
            "Bars usually arrive late in a burst afterwards (up to 144h old on "
            "2026-07-28), so prod/real_trade positions are frozen until it recovers. "
            "The arrival-driven fix-prod-window / fix-real-trade-window Lambdas will "
            "repair the span once the bars land, but only back to MAX_LOOKBACK_HOURS "
            "(120h) — watch for the companion 'window-clamped' CloudWatch alarm."
        ),
    ),
]


def rule(uid, title, sql, threshold, for_duration, summary):
    return {
        "uid": uid,
        "title": title,
        "folderUID": "__FOLDER_UID__",
        "ruleGroup": RULE_GROUP,
        "condition": "C",
        "for": for_duration,
        # NoData here means the source table returned nothing at all for a whole
        # day — that is itself an outage, so alert rather than silently pass.
        "noDataState": "Alerting",
        "execErrState": "Alerting",
        "orgID": 1,
        "labels": {"alertgroup": RULE_GROUP, "metric": uid},
        "annotations": {"summary": summary},
        "notification_settings": {"receiver": "telegram-divergence"},
        "data": [
            {
                "refId": "A",
                "relativeTimeRange": {"from": BASELINE_HOURS * 3600, "to": 0},
                "datasourceUid": DS_UID,
                "model": {
                    "refId": "A",
                    "editorType": "sql",
                    "rawSql": sql,
                    "format": 1,
                    "queryType": "timeseries",
                    "datasource": {
                        "type": "grafana-clickhouse-datasource",
                        "uid": DS_UID,
                    },
                    "intervalMs": 300000,
                    "maxDataPoints": 100,
                },
            },
            {
                "refId": "B",
                "datasourceUid": "__expr__",
                "model": {
                    "refId": "B",
                    "type": "reduce",
                    "datasource": {"type": "__expr__", "uid": "__expr__"},
                    "expression": "A",
                    "reducer": "last",
                    "settings": {"mode": "dropNN"},
                },
            },
            {
                "refId": "C",
                "datasourceUid": "__expr__",
                "model": {
                    "refId": "C",
                    "type": "threshold",
                    "datasource": {"type": "__expr__", "uid": "__expr__"},
                    "expression": "B",
                    "conditions": [
                        {
                            "type": "query",
                            "evaluator": {"type": "lt", "params": [threshold]},
                        }
                    ],
                },
            },
        ],
    }


rules = [rule(*r) for r in RULES]
out = os.path.join(os.path.dirname(__file__), "rules-source-freshness.json")
with open(out, "w") as f:
    json.dump(rules, f, indent=2, ensure_ascii=False)
    f.write("\n")
print(f"wrote {out} ({len(rules)} rules)")
