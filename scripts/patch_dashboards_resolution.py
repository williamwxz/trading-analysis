#!/usr/bin/env python3
"""
Patches all 5 Grafana strategy-pnl dashboards so each PnL family reference
becomes a 3-branch UNION ALL across analytics.strategy_pnl_1min_*,
strategy_pnl_1hour_*, and strategy_pnl_1day_*. Branches are time-disjoint
and gapless on hour/day boundaries:

  - 1min  : ts >= toStartOfHour(now() - INTERVAL 6 HOUR)
  - 1hour : toStartOfDay(now() - INTERVAL 30 DAY) <= ts
              < toStartOfHour(now() - INTERVAL 6 HOUR)
  - 1day  : ts < toStartOfDay(now() - INTERVAL 30 DAY)

The patcher is idempotent: re-running on a canonical 3-branch dashboard is a
no-op. A 2-branch (1min + 1hour) UNION ALL is upgraded to 3 branches in
place. A bare reference is wrapped into the 3-branch form.

This script is the source of truth for the dashboard SQL shape — any future
drift between script and committed JSON is a bug, fixed by re-running.

Run once: python3 scripts/patch_dashboards_resolution.py
"""
import json
import re
from pathlib import Path

DASHBOARDS_DIR = Path(__file__).parent.parent / "infra" / "grafana" / "dashboards"

# WHERE-clause fragments for each branch. Boundaries are mutually exclusive
# and gapless: 1min serves the last 6h (hour-aligned), 1hour serves
# [now-30d, now-6h) (day-aligned at the 30d cut), 1day serves earlier rows
# (day-aligned).
_1MIN_WHERE = "ts >= toStartOfHour(now() - INTERVAL 6 HOUR)"
_1HOUR_WHERE = (
    "ts >= toStartOfDay(now() - INTERVAL 30 DAY) "
    "AND ts < toStartOfHour(now() - INTERVAL 6 HOUR)"
)
_1DAY_WHERE = "ts < toStartOfDay(now() - INTERVAL 30 DAY)"


# Matches bare "analytics.strategy_pnl_1min_<family>" and
# "analytics.strategy_pnl_1hour_<family>" references that are NOT already
# wrapped inside a UNION ALL subquery. The lookbehind rejects matches whose
# preceding character is "(" (i.e., the leading paren of a UNION ALL block).
# Lookahead avoids matching inside an already-patched UNION ALL subquery.
_BARE_TABLE_RE = re.compile(
    r"(?<!\()analytics\.strategy_pnl_1(?:min|hour)_(\w+?)( FINAL)?"
    r"(?= WHERE| FINAL WHERE|\)| GROUP| ORDER|$)",
    re.MULTILINE,
)


def _union_sql(family: str, final: str) -> str:
    """Canonical 3-branch UNION ALL for one family + optional ' FINAL' suffix."""
    return (
        f"(SELECT * FROM analytics.strategy_pnl_1min_{family}{final}"
        f" WHERE {_1MIN_WHERE}"
        f" UNION ALL"
        f" SELECT * FROM analytics.strategy_pnl_1hour_{family}{final}"
        f" WHERE {_1HOUR_WHERE}"
        f" UNION ALL"
        f" SELECT * FROM analytics.strategy_pnl_1day_{family}{final}"
        f" WHERE {_1DAY_WHERE})"
    )


def patch_sql(sql: str) -> tuple[str, int]:
    count = 0

    def _repl(m: re.Match) -> str:
        nonlocal count
        count += 1
        return _union_sql(m.group(1), m.group(2) or "")

    return _BARE_TABLE_RE.sub(_repl, sql), count


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
