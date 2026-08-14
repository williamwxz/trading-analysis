"""Generate Grafana provisioning alert-rule JSON for the divergence rules. Writes
infra/grafana/alerting/rules-divergence.json (an array of rule objects for
POST/PUT /api/v1/provisioning/alert-rules).

Two rules:

  divergence-bt-prod-per-underlying — position, PER UNDERLYING, one alert
    instance per (underlying, sid, sno) naming that coin's 5 worst contributors.
  divergence-pnl-prod-rt            — cumulative_pnl, portfolio-aggregate.

folderUID is left as the literal __FOLDER_UID__ so the provisioning script can
substitute the real folder uid after ensuring the folder exists.
"""

import json
import os

DS_UID = "dfjc5vjyfcc8wf"  # grafana-clickhouse-datasource (same as L4/L5 panels)
WINDOW_MIN = 17  # query lookback for the divergence value; 15 complete minutes + slack
RANK_WINDOW_MIN = 60  # lookback for RANKING the top contributors — see below
TOP_N = 5  # contributors named per underlying
FOR = "10m"  # sustained breach before firing

# Rules replaced by divergence-bt-prod-per-underlying / deleted outright. The
# provisioning script DELETEs these explicitly — it upserts by uid and never
# prunes, so dropping a rule from this file is not enough to retire it.
RETIRED_UIDS = ["divergence-bt-prod", "divergence-prod-rt"]

# Per-coin position-divergence thresholds, measured 2026-08-14 over the preceding
# 21 days of 1-min data. A flat threshold is not viable: the portfolio-tuned 0.05
# would have been in sustained breach for 14% of all minutes on AVAX and 12% on
# ADA, because a coin with 35-66 strategies moves its own weighted average far
# more on one late bar than the 695-strategy portfolio does. These values sit just
# above each coin's routine band and together produced 6 firings in 21 days.
THRESHOLDS = {
    "BTC": 0.15,
    "ETH": 0.15,
    "SOL": 0.20,
    "ADA": 0.25,
    "DOGE": 0.25,
    "AVAX": 0.30,
    "XRP": 0.30,
    "FET": 0.40,
}
DEFAULT_THRESHOLD = 0.25  # a newly added underlying, until it is measured

PNL_THRESHOLD = 0.015  # prod-rt cumulative_pnl spread; unchanged


def threshold_case(col: str) -> str:
    """ClickHouse CASE mapping an underlying column to its threshold."""
    whens = " ".join(f"WHEN '{k}' THEN {v:g}" for k, v in THRESHOLDS.items())
    return f"CASE {col} {whens} ELSE {DEFAULT_THRESHOLD:g} END"


def roster_size(tbl: str, final: bool = False) -> str:
    """Scalar subquery: how many underlyings are currently live in `tbl`.

    Derived from the window itself, never hardcoded. An exact `countDistinct(...)
    = 8` rejects EVERY minute the day a ninth instrument is onboarded — and since
    both rules use noDataState=OK, that silently disables all alerting rather than
    failing loudly. It would also make DEFAULT_THRESHOLD unreachable: the gate
    breaks before a new coin's default could ever apply.

    A coin that stops writing for the whole window shrinks the roster instead of
    wedging the gate. That is the better failure: the per-underlying rule simply
    has no instances for the missing coin, and the other coins keep alerting.
    Genuinely absent source data is the source-freshness rules' job, not this one.
    """
    f = " FINAL" if final else ""
    return (
        f"SELECT countDistinct(underlying) FROM analytics.{tbl}{f} "
        f"WHERE ts >= now() - INTERVAL {WINDOW_MIN} MINUTE"
    )


def _pairs(minutes: int, gated: bool) -> str:
    """Per-strategy (bt - prod) position delta over the last `minutes` minutes.

    INNER JOIN on (underlying, strategy, minute) keeps the comparison
    apples-to-apples: only strategies present in both modes contribute, and since
    both sides carry the same weighting, sum(dp*w)/sum(w) equals
    wagg_bt - wagg_prod exactly.
    """
    gate = "AND toStartOfMinute(ts) IN (SELECT t FROM complete) " if gated else ""

    def side(tbl: str) -> str:
        return (
            f"SELECT underlying u, strategy_table_name s, toStartOfMinute(ts) t, "
            f"argMax(position, updated_at) p, argMax(weighting, updated_at) w "
            f"FROM analytics.{tbl} "
            f"WHERE ts >= now() - INTERVAL {minutes} MINUTE {gate}"
            f"GROUP BY u, s, t"
        )

    return (
        f"SELECT bt.u AS u, bt.s AS s, bt.t AS t, bt.p - pr.p AS dp, pr.w AS w "
        f"FROM ({side('strategy_pnl_1min_bt_v2')}) bt "
        f"INNER JOIN ({side('strategy_pnl_1min_prod_v2')}) pr USING (u, s, t)"
    )


def per_underlying_sql() -> str:
    """One row per (minute, underlying, sid, sno) for each coin's TOP_N contributors.

    `value` is the coin's |divergence| divided by its own threshold, so a single
    flat Grafana condition (> 1) expresses eight different thresholds.

    Rows are emitted for every coin at every minute, not only breaching ones, so
    each (underlying, sid, sno) alert instance exists continuously and its `for`
    timer is not restarted by the instance blinking in and out of existence.

    Ranking uses RANK_WINDOW_MIN (60m), deliberately longer than the WINDOW_MIN
    (17m) value window, and carries an explicit `s ASC` tie-break. Both matter:
    Grafana keys instance identity off labels, so if top-N membership churns the
    10-minute `for` timer never matures and the rule never fires. Measured on the
    two real episodes in the 21d history, 60m gives one long stable run each
    (XRP 50 min, FET 21 min) where 17m fragments into runs as short as 2 minutes.
    """
    roster = roster_size("strategy_pnl_1min_prod_v2")
    return f"""
WITH complete AS (
  SELECT toStartOfMinute(ts) t FROM analytics.strategy_pnl_1min_prod_v2
  WHERE ts >= now() - INTERVAL {WINDOW_MIN} MINUTE
  GROUP BY t HAVING countDistinct(underlying) = ({roster})
),
coin AS (
  SELECT u, t, abs(sum(dp * w) / nullIf(sum(w), 0)) AS adiv
  FROM ({_pairs(WINDOW_MIN, True)}) GROUP BY u, t
),
top AS (
  SELECT u, s FROM (
    SELECT u, s, row_number() OVER (PARTITION BY u ORDER BY contrib DESC, s ASC) AS rn
    FROM (SELECT u, s, avg(abs(dp) * w) AS contrib
          FROM ({_pairs(RANK_WINDOW_MIN, False)}) GROUP BY u, s)
  ) WHERE rn <= {TOP_N}
)
SELECT coin.t AS time,
       coin.u AS underlying,
       extract(top.s, 'sid=([0-9]+)') AS sid,
       extract(top.s, 'sno=([0-9]+)') AS sno,
       coin.adiv / ({threshold_case('coin.u')}) AS value
FROM coin INNER JOIN top ON coin.u = top.u
ORDER BY time, underlying, sid, sno
""".strip()


def _query_node(sql: str, window_min: int) -> dict:
    return {
        "refId": "A",
        "relativeTimeRange": {"from": window_min * 60, "to": 0},
        "datasourceUid": DS_UID,
        "model": {
            "refId": "A",
            "editorType": "sql",
            "rawSql": sql,
            "format": 1,
            "queryType": "timeseries",
            "datasource": {"type": "grafana-clickhouse-datasource", "uid": DS_UID},
            "intervalMs": 60000,
            "maxDataPoints": 100,
        },
    }


def _reduce_node() -> dict:
    return {
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
    }


def _threshold_node(evaluator: dict) -> dict:
    return {
        "refId": "C",
        "datasourceUid": "__expr__",
        "model": {
            "refId": "C",
            "type": "threshold",
            "datasource": {"type": "__expr__", "uid": "__expr__"},
            "expression": "B",
            "conditions": [{"type": "query", "evaluator": evaluator}],
        },
    }


def per_underlying_rule() -> dict:
    thresholds = ", ".join(f"{k} {v:g}" for k, v in THRESHOLDS.items())
    return {
        "uid": "divergence-bt-prod-per-underlying",
        "title": "Position divergence per underlying: Backtest − Production",
        "folderUID": "__FOLDER_UID__",
        "ruleGroup": "divergence",
        "condition": "C",
        "for": FOR,
        "noDataState": "OK",
        "execErrState": "Alerting",
        "orgID": 1,
        "labels": {
            "alertgroup": "divergence",
            "metric": "position",
            "pair": "bt-prod",
        },
        "annotations": {
            "summary": (
                "{{ $labels.underlying }}: backtest vs production weighted position "
                "diverged past its threshold for " + FOR + " "
                "({{ $values.B }}x threshold). "
                "Among the top " + str(TOP_N) + " contributors: "
                "sid={{ $labels.sid }} sno={{ $labels.sno }}. "
                "See dashboard: Strategy PnL — L4 Underlying."
            ),
            "description": (
                "Per-underlying thresholds (|Δposition|): "
                + thresholds
                + f"; default {DEFAULT_THRESHOLD:g}. The alert value is the ratio "
                "of the observed divergence to that threshold, so it fires above 1."
            ),
        },
        "notification_settings": {"receiver": "telegram-divergence"},
        "data": [
            _query_node(per_underlying_sql(), WINDOW_MIN),
            _reduce_node(),
            _threshold_node({"type": "gt", "params": [1.0]}),
        ],
    }


def _pnl_wagg(tbl: str) -> str:
    return (
        f"SELECT toStartOfMinute(ts) t, "
        f"sum(cumulative_pnl * weighting) / nullIf(sum(weighting), 0) wp "
        f"FROM analytics.{tbl} FINAL "
        f"WHERE ts >= now() - INTERVAL {WINDOW_MIN} MINUTE "
        f"AND toStartOfMinute(ts) IN ("
        f"SELECT toStartOfMinute(ts) FROM analytics.{tbl} FINAL "
        f"WHERE ts >= now() - INTERVAL {WINDOW_MIN} MINUTE "
        f"GROUP BY toStartOfMinute(ts) "
        f"HAVING countDistinct(underlying) = ({roster_size(tbl, final=True)})) "
        f"GROUP BY t"
    )


def pnl_rule() -> dict:
    """prod - rt cumulative_pnl spread. Portfolio-aggregate, unchanged.

    This spread carries a legitimate structural offset (rt fills at revision_ts,
    prod at bar close) that has ranged within ~[-0.009, +0.003]; 0.015 clears that
    envelope so it fires only on a genuine break. bt is excluded — it stores raw
    row_json cpnl on a ~+4 scale, not comparable.
    """
    sql = (
        f"SELECT a.t AS time, a.wp - b.wp AS value "
        f"FROM ({_pnl_wagg('strategy_pnl_1min_prod_v2')}) a "
        f"INNER JOIN ({_pnl_wagg('strategy_pnl_1min_real_trade_v2')}) b "
        f"USING (t) ORDER BY time"
    )
    return {
        "uid": "divergence-pnl-prod-rt",
        "title": "PnL divergence: Production − Real-Trade",
        "folderUID": "__FOLDER_UID__",
        "ruleGroup": "divergence",
        "condition": "C",
        "for": FOR,
        "noDataState": "OK",
        "execErrState": "Alerting",
        "orgID": 1,
        "labels": {
            "alertgroup": "divergence",
            "metric": "cumulative_pnl",
            "pair": "prod-rt",
        },
        "annotations": {
            "summary": (
                f"Portfolio weighted PnL diverges (Production − Real-Trade) by more "
                f"than {PNL_THRESHOLD:g} for {FOR}. "
                f"See dashboard: Strategy PnL — L5 Portfolio."
            ),
        },
        "notification_settings": {"receiver": "telegram-divergence"},
        "data": [
            _query_node(sql, WINDOW_MIN),
            _reduce_node(),
            _threshold_node(
                {"type": "outside_range", "params": [-PNL_THRESHOLD, PNL_THRESHOLD]}
            ),
        ],
    }


def main() -> None:
    rules = [per_underlying_rule(), pnl_rule()]
    out = os.path.join(os.path.dirname(__file__), "rules-divergence.json")
    with open(out, "w") as f:
        json.dump(rules, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"wrote {out} ({len(rules)} rules)")


# Guarded, unlike the previous version: the tests import this module, and an
# unguarded write would silently regenerate rules-divergence.json on import —
# making the "generated file is in sync" test vacuously true.
if __name__ == "__main__":
    main()
