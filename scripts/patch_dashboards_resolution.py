#!/usr/bin/env python3
"""
Patches all 5 Grafana dashboard JSON files to use a dynamic `resolution`
variable (1min or 1hour) based on the selected time range.
Run once: python3 scripts/patch_dashboards_resolution.py
"""
import json
from pathlib import Path

DASHBOARDS_DIR = Path(__file__).parent.parent / "infra" / "grafana" / "dashboards"

CLICKHOUSE_DS_UID = "dfjc5vjyfcc8wf"

RESOLUTION_VARIABLE = {
    "name": "resolution",
    "label": "",
    "type": "query",
    "datasource": {
        "type": "grafana-clickhouse-datasource",
        "uid": CLICKHOUSE_DS_UID,
    },
    "definition": "SELECT if(\n  toDateTime(${__from:date:seconds}) >= now() - INTERVAL 6 HOUR,\n  '1min',\n  '1hour'\n)",
    "query": "SELECT if(\n  toDateTime(${__from:date:seconds}) >= now() - INTERVAL 6 HOUR,\n  '1min',\n  '1hour'\n)",
    "refresh": 2,
    "sort": 0,
    "multi": False,
    "includeAll": False,
    "hide": 2,
}

TABLE_SUBSTITUTIONS = [
    (
        "analytics.strategy_pnl_1min_prod_v2",
        "analytics.strategy_pnl_${resolution:raw}_prod_v2",
    ),
    (
        "analytics.strategy_pnl_1min_bt_v2",
        "analytics.strategy_pnl_${resolution:raw}_bt_v2",
    ),
    (
        "analytics.strategy_pnl_1min_real_trade_v2",
        "analytics.strategy_pnl_${resolution:raw}_real_trade_v2",
    ),
]


def patch_sql(sql: str) -> str:
    for old, new in TABLE_SUBSTITUTIONS:
        sql = sql.replace(old, new)
    return sql


def patch_panel(panel: dict) -> None:
    for target in panel.get("targets", []):
        if "rawSql" in target:
            target["rawSql"] = patch_sql(target["rawSql"])
    for nested in panel.get("panels", []):
        patch_panel(nested)


def patch_dashboard(path: Path) -> int:
    d = json.loads(path.read_text())

    # Insert resolution variable at index 0 (must resolve before other vars use it)
    tvars = d.setdefault("templating", {}).setdefault("list", [])
    if any(v.get("name") == "resolution" for v in tvars):
        print(f"  SKIP (already patched): {path.name}")
        return 0

    tvars.insert(0, RESOLUTION_VARIABLE)

    # Patch all panel SQL
    for panel in d.get("panels", []):
        patch_panel(panel)

    # Count substitutions for reporting
    text_after = json.dumps(d)
    count = sum(text_after.count(new) for _, new in TABLE_SUBSTITUTIONS)

    path.write_text(json.dumps(d, indent=2) + "\n")
    print(f"  PATCHED {path.name}: ~{count} table references updated")
    return count


def main() -> None:
    dashboards = sorted(DASHBOARDS_DIR.glob("*.json"))
    if not dashboards:
        raise SystemExit(f"No JSON files found in {DASHBOARDS_DIR}")

    total = 0
    for path in dashboards:
        total += patch_dashboard(path)
    print(f"\nDone. Total ${{resolution:raw}} references inserted: {total}")


if __name__ == "__main__":
    main()
