#!/usr/bin/env python3
"""
Patches all 5 Grafana dashboard JSON files so that each panel query
automatically selects strategy_pnl_1min_* or strategy_pnl_1hour_* based on
whether the dashboard time range start (${__from}) is within the last 6 hours.

Uses UNION ALL with mutually exclusive WHERE filters on ${__from} so that
exactly one branch contributes rows at runtime. ${__from} is Grafana's built-in
epoch-millisecond variable, which IS interpolated in panel queries.

Run once: python3 scripts/patch_dashboards_resolution.py
"""
import json
import re
from pathlib import Path

DASHBOARDS_DIR = Path(__file__).parent.parent / "infra" / "grafana" / "dashboards"

# ${__from} is epoch milliseconds; toUnixTimestamp() returns epoch seconds.
_WITHIN_6H = "${__from} >= toUnixTimestamp(now() - INTERVAL 6 HOUR) * 1000"
_BEYOND_6H = "${__from} < toUnixTimestamp(now() - INTERVAL 6 HOUR) * 1000"

# Matches bare "analytics.strategy_pnl_1min_<family>" and
# "analytics.strategy_pnl_1hour_<family>" references (pre-patch originals).
# Lookahead avoids matching inside an already-patched UNION ALL subquery.
_TABLE_RE = re.compile(
    r"analytics\.strategy_pnl_1(?:min|hour)_(\w+?)( FINAL)?(?= WHERE| FINAL WHERE|\)| GROUP| ORDER|$)",
    re.MULTILINE,
)


def _union_sql(family: str, final: str) -> str:
    return (
        f"(SELECT * FROM analytics.strategy_pnl_1min_{family}{final}"
        f" WHERE {_WITHIN_6H}"
        f" UNION ALL"
        f" SELECT * FROM analytics.strategy_pnl_1hour_{family}{final}"
        f" WHERE {_BEYOND_6H})"
    )


def patch_sql(sql: str) -> tuple[str, int]:
    count = 0

    def _repl(m: re.Match) -> str:
        nonlocal count
        count += 1
        return _union_sql(m.group(1), m.group(2) or "")

    return _TABLE_RE.sub(_repl, sql), count


def patch_panel(panel: dict) -> int:
    count = 0
    for target in panel.get("targets", []):
        if "rawSql" in target:
            new_sql, n = patch_sql(target["rawSql"])
            target["rawSql"] = new_sql
            count += n
    for nested in panel.get("panels", []):
        count += patch_panel(nested)
    return count


def patch_dashboard(path: Path) -> int:
    d = json.loads(path.read_text())

    text = json.dumps(d)
    if "UNION ALL" in text:
        print(f"  SKIP (already patched): {path.name}")
        return 0

    count = 0
    for panel in d.get("panels", []):
        count += patch_panel(panel)

    path.write_text(json.dumps(d, indent=2) + "\n")
    print(f"  PATCHED {path.name}: {count} queries rewritten")
    return count


def main() -> None:
    dashboards = sorted(DASHBOARDS_DIR.glob("*.json"))
    if not dashboards:
        raise SystemExit(f"No JSON files found in {DASHBOARDS_DIR}")

    total = 0
    for path in dashboards:
        total += patch_dashboard(path)
    print(f"\nDone. {total} total queries rewritten across {len(dashboards)} dashboards.")


if __name__ == "__main__":
    main()
