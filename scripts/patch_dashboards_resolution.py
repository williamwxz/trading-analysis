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
# inside a canonical 3-branch UNION ALL. Four negative lookaheads reject
# references already followed by one of the patcher-generated WHERE clauses
# (with or without a leading ' FINAL'), making the bare pass idempotent on
# 3-branch output. Both the FINAL and non-FINAL variants of each clause are
# excluded because the regex's optional ( FINAL)? group may not consume FINAL
# before the lookaheads run when the engine chooses zero-length for that group.
_BARE_TABLE_RE = re.compile(
    r"(?<!\()analytics\.strategy_pnl_1(?:min|hour)_(\w+?)( FINAL)?"
    r"(?! WHERE ts >= toStartOfHour)(?! WHERE ts >= toStartOfDay)"
    r"(?! FINAL WHERE ts >= toStartOfHour)(?! FINAL WHERE ts >= toStartOfDay)"
    r"(?= WHERE| FINAL WHERE|\)| GROUP| ORDER|$)",
    re.MULTILINE,
)

# Matches the 2-branch UNION ALL currently committed in the dashboards:
#   (SELECT * FROM analytics.strategy_pnl_1min_<family>[ FINAL]
#       WHERE ts >= toStartOfHour(now() - INTERVAL 6 HOUR)
#    UNION ALL
#    SELECT * FROM analytics.strategy_pnl_1hour_<family>[ FINAL]
#       WHERE ts < toStartOfHour(now() - INTERVAL 6 HOUR))
#
# Two separate patterns handle the optional ' FINAL' suffix: Python's re
# module does not allow backreferencing an unmatched optional group (\2 when
# group 2 didn't participate), so we split into a FINAL and a no-FINAL variant.
# The 1hour branch's FINAL is required to match the 1min branch's — the
# patcher never mixes them.
_TWO_BRANCH_RE_NOFINAL = re.compile(
    r"\(SELECT \* FROM analytics\.strategy_pnl_1min_(\w+?)"
    r" WHERE ts >= toStartOfHour\(now\(\) - INTERVAL 6 HOUR\)"
    r" UNION ALL"
    r" SELECT \* FROM analytics\.strategy_pnl_1hour_\1"
    r" WHERE ts < toStartOfHour\(now\(\) - INTERVAL 6 HOUR\)\)"
)
_TWO_BRANCH_RE_FINAL = re.compile(
    r"\(SELECT \* FROM analytics\.strategy_pnl_1min_(\w+?) FINAL"
    r" WHERE ts >= toStartOfHour\(now\(\) - INTERVAL 6 HOUR\)"
    r" UNION ALL"
    r" SELECT \* FROM analytics\.strategy_pnl_1hour_\1 FINAL"
    r" WHERE ts < toStartOfHour\(now\(\) - INTERVAL 6 HOUR\)\)"
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
    """Rewrite every PnL family reference in `sql` to the canonical 3-branch
    UNION ALL form. Handles bare references, 2-branch UNION ALL upgrades,
    and existing 3-branch (no-op). Returns (rewritten_sql, n_substitutions).
    """
    count = 0

    def _repl_nofinal(m: re.Match) -> str:
        nonlocal count
        count += 1
        return _union_sql(m.group(1), "")

    def _repl_final(m: re.Match) -> str:
        nonlocal count
        count += 1
        return _union_sql(m.group(1), " FINAL")

    def _repl_bare(m: re.Match) -> str:
        nonlocal count
        count += 1
        return _union_sql(m.group(1), m.group(2) or "")

    # Pass 1: upgrade deployed 2-branch UNION ALL to 3-branch.
    # FINAL variant first so the no-FINAL pattern cannot partially match it.
    sql = _TWO_BRANCH_RE_FINAL.sub(_repl_final, sql)
    sql = _TWO_BRANCH_RE_NOFINAL.sub(_repl_nofinal, sql)
    # Pass 2: wrap any remaining bare references.
    sql = _BARE_TABLE_RE.sub(_repl_bare, sql)

    return sql, count


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
    """Rewrite every PnL family reference in `path` to the canonical 3-branch
    form. patch_sql is idempotent: a dashboard already in 3-branch shape
    yields zero substitutions and the file is left untouched.
    """
    d = json.loads(path.read_text())

    count = 0
    for panel in d.get("panels", []):
        count += patch_panel(panel)

    if count == 0:
        print(f"  SKIP (already canonical): {path.name}")
        return 0

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
