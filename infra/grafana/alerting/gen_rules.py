"""Generate Grafana provisioning alert-rule JSON for the divergence rules (two
position pairs + one prod-rt PnL spread). Writes
infra/grafana/alerting/rules-divergence.json (an array of rule objects for
POST/PUT /api/v1/provisioning/alert-rules).

folderUID is left as the literal __FOLDER_UID__ so the provisioning script can
substitute the real folder uid after ensuring the folder exists. Threshold,
window, and sustained duration are encoded here so the as-code source matches
the dashboard panels (PR #33) and scripts/divergence_alert.py.
"""

import json
import os

DS_UID = "dfjc5vjyfcc8wf"  # grafana-clickhouse-datasource (same as L5 panels)
WINDOW_MIN = 17  # query lookback; 15 complete minutes + slack
FOR = "10m"  # sustained breach before firing

# Each rule: (metric, threshold, kind, a, b, tbl_a, tbl_b, title, uid)
# - position threshold 0.05: prod-rt sits ~0, bt-prod has a structural ~0.02 baseline.
# - pnl threshold 0.015: prod-rt cpnl carries a legitimate structural offset (rt fills
#   at revision_ts, prod at bar close) that has ranged within ~[-0.009, +0.003]; 0.015
#   clears that envelope so the alert fires only on a genuine break (e.g. a big
#   re-anchor seam), not the normal offset. bt is excluded for pnl — it stores raw
#   row_json cpnl on a ~+4 scale, not comparable.
RULES = [
    (
        "position",
        0.05,
        "Position",
        "bt",
        "prod",
        "strategy_pnl_1min_bt_v2",
        "strategy_pnl_1min_prod_v2",
        "Backtest − Production",
        "divergence-bt-prod",
    ),
    (
        "position",
        0.05,
        "Position",
        "prod",
        "rt",
        "strategy_pnl_1min_prod_v2",
        "strategy_pnl_1min_real_trade_v2",
        "Production − Real-Trade",
        "divergence-prod-rt",
    ),
    (
        "cumulative_pnl",
        0.015,
        "PnL",
        "prod",
        "rt",
        "strategy_pnl_1min_prod_v2",
        "strategy_pnl_1min_real_trade_v2",
        "Production − Real-Trade",
        "divergence-pnl-prod-rt",
    ),
]


def wagg(tbl: str, metric: str) -> str:
    return (
        f"SELECT toStartOfMinute(ts) t, "
        f"sum({metric} * weighting) / nullIf(sum(weighting), 0) wp "
        f"FROM analytics.{tbl} FINAL "
        f"WHERE ts >= now() - INTERVAL {WINDOW_MIN} MINUTE "
        f"AND toStartOfMinute(ts) IN ("
        f"SELECT toStartOfMinute(ts) FROM analytics.{tbl} FINAL "
        f"WHERE ts >= now() - INTERVAL {WINDOW_MIN} MINUTE "
        f"GROUP BY toStartOfMinute(ts) HAVING countDistinct(underlying) = 8) "
        f"GROUP BY t"
    )


def rule(metric, threshold, kind, a, b, tbl_a, tbl_b, title, uid):
    sql = (
        f"SELECT a.t AS time, a.wp - b.wp AS value "
        f"FROM ({wagg(tbl_a, metric)}) a "
        f"INNER JOIN ({wagg(tbl_b, metric)}) b USING (t) ORDER BY time"
    )
    noun = "position" if metric == "position" else "PnL"
    return {
        "uid": uid,
        "title": f"{kind} divergence: {title}",
        "folderUID": "__FOLDER_UID__",
        "ruleGroup": "divergence",
        "condition": "C",
        "for": FOR,
        "noDataState": "OK",
        "execErrState": "Alerting",
        "orgID": 1,
        "labels": {"alertgroup": "divergence", "metric": metric, "pair": f"{a}-{b}"},
        "annotations": {
            "summary": (
                f"Portfolio weighted {noun} diverges ({title}) by more than "
                f"{threshold:g} for {FOR}. See dashboard: Strategy PnL — L5 Portfolio."
            ),
        },
        "notification_settings": {"receiver": "telegram-divergence"},
        "data": [
            {
                "refId": "A",
                "relativeTimeRange": {"from": WINDOW_MIN * 60, "to": 0},
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
                    "intervalMs": 60000,
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
                            "evaluator": {
                                "type": "outside_range",
                                "params": [-threshold, threshold],
                            },
                        }
                    ],
                },
            },
        ],
    }


rules = [rule(*r) for r in RULES]
out = os.path.join(os.path.dirname(__file__), "rules-divergence.json")
with open(out, "w") as f:
    json.dump(rules, f, indent=2, ensure_ascii=False)
    f.write("\n")
print(f"wrote {out} ({len(rules)} rules)")
