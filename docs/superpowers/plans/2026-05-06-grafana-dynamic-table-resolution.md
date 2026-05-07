# Grafana Dynamic Table Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make all 5 Grafana dashboards automatically query `strategy_pnl_1min_*` tables when the selected time range starts within the last 6 hours, and `strategy_pnl_1hour_*` tables otherwise.

**Architecture:** A hidden Grafana template variable `resolution` evaluates to `'1min'` or `'1hour'` via a ClickHouse SQL query that checks `${__from:date:seconds}` against `now() - INTERVAL 6 HOUR`. All panel SQL references `${resolution:raw}` in the table name. No ClickHouse schema changes — all `1hour_*` tables already exist.

**Tech Stack:** Python 3 (for JSON manipulation script), Grafana dashboard JSON, ClickHouse SQL

---

## Files

| File | Change |
|---|---|
| `scripts/patch_dashboards_resolution.py` | Create — one-shot script that patches all 5 dashboard JSON files |
| `infra/grafana/dashboards/strategy-pnl-l1-instance.json` | Modified by script |
| `infra/grafana/dashboards/strategy-pnl-l2-sid-underlying.json` | Modified by script |
| `infra/grafana/dashboards/strategy-pnl-l3-sid.json` | Modified by script |
| `infra/grafana/dashboards/strategy-pnl-l4-underlying.json` | Modified by script |
| `infra/grafana/dashboards/strategy-pnl-l5-portfolio.json` | Modified by script |

---

### Task 1: Write and run the patch script

The script does three things to each dashboard JSON:
1. Inserts the `resolution` variable at index 0 of `templating.list`
2. Replaces all 4 table name patterns in every `rawSql` field
3. Writes the file back with 2-space indent (matching Grafana's export format)

**Files:**
- Create: `scripts/patch_dashboards_resolution.py`

- [ ] **Step 1: Write the patch script**

Create `scripts/patch_dashboards_resolution.py` with this exact content:

```python
#!/usr/bin/env python3
"""
Patches all 5 Grafana dashboard JSON files to use a dynamic `resolution`
variable (1min or 1hour) based on the selected time range.
Run once: python3 scripts/patch_dashboards_resolution.py
"""
import json
import re
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
        "analytics.strategy_pnl_1hour_bt_v2",
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
    count = 0
    for panel in d.get("panels", []):
        patch_panel(panel)

    # Count substitutions for reporting
    text_after = json.dumps(d)
    for _, new in TABLE_SUBSTITUTIONS:
        count += text_after.count(new)

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
    print(f"\nDone. Total ${'{resolution:raw}'} references inserted: {total}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the script**

```bash
cd /Users/wzhang/Desktop/trading-analysis
python3 scripts/patch_dashboards_resolution.py
```

Expected output (approximate counts):
```
  PATCHED strategy-pnl-l1-instance.json: ~15 table references updated
  PATCHED strategy-pnl-l2-sid-underlying.json: ~18 table references updated
  PATCHED strategy-pnl-l3-sid.json: ~14 table references updated
  PATCHED strategy-pnl-l4-underlying.json: ~13 table references updated
  PATCHED strategy-pnl-l5-portfolio.json: ~20 table references updated

Done. Total ${resolution:raw} references inserted: ~80
```

- [ ] **Step 3: Verify the resolution variable was inserted correctly in each file**

```bash
python3 -c "
import json, glob
for f in sorted(glob.glob('infra/grafana/dashboards/*.json')):
    d = json.load(open(f))
    tvars = d.get('templating', {}).get('list', [])
    first = tvars[0] if tvars else {}
    name = first.get('name')
    refresh = first.get('refresh')
    hide = first.get('hide')
    print(f'{f}: first_var={name!r} refresh={refresh} hide={hide}')
"
```

Expected — every line shows `first_var='resolution' refresh=2 hide=2`:
```
infra/grafana/dashboards/strategy-pnl-l1-instance.json: first_var='resolution' refresh=2 hide=2
infra/grafana/dashboards/strategy-pnl-l2-sid-underlying.json: first_var='resolution' refresh=2 hide=2
infra/grafana/dashboards/strategy-pnl-l3-sid.json: first_var='resolution' refresh=2 hide=2
infra/grafana/dashboards/strategy-pnl-l4-underlying.json: first_var='resolution' refresh=2 hide=2
infra/grafana/dashboards/strategy-pnl-l5-portfolio.json: first_var='resolution' refresh=2 hide=2
```

- [ ] **Step 4: Verify no bare 1min/1hour table names remain in panel SQL**

```bash
python3 -c "
import json, glob, sys
errors = []
bare_tables = [
    'strategy_pnl_1min_prod_v2',
    'strategy_pnl_1hour_prod_v2',
    'strategy_pnl_1min_bt_v2',
    'strategy_pnl_1hour_bt_v2',
    'strategy_pnl_1min_real_trade_v2',
    'strategy_pnl_1hour_real_trade_v2',
]
for f in sorted(glob.glob('infra/grafana/dashboards/*.json')):
    text = open(f).read()
    for tbl in bare_tables:
        # Only flag bare names — not ones already containing resolution:raw
        import re
        # Find occurrences not preceded by resolution:raw}_ pattern
        matches = [m.start() for m in re.finditer(re.escape(tbl), text)]
        for pos in matches:
            context = text[max(0,pos-30):pos+len(tbl)+5]
            if '\${resolution:raw}' not in context:
                errors.append(f'{f}: bare table ref: {tbl}')
if errors:
    print('ERRORS:')
    for e in errors: print(' ', e)
    sys.exit(1)
else:
    print('OK — no bare table names found in panel SQL')
"
```

Expected: `OK — no bare table names found in panel SQL`

- [ ] **Step 5: Verify substituted table names are present**

```bash
python3 -c "
import json, glob
patterns = [
    'strategy_pnl_\${resolution:raw}_prod_v2',
    'strategy_pnl_\${resolution:raw}_bt_v2',
    'strategy_pnl_\${resolution:raw}_real_trade_v2',
]
for f in sorted(glob.glob('infra/grafana/dashboards/*.json')):
    text = open(f).read()
    found = [p for p in patterns if p in text]
    print(f'{f}: {found}')
"
```

Expected — each dashboard shows all 3 patterns present:
```
infra/grafana/dashboards/strategy-pnl-l1-instance.json: ['strategy_pnl_${resolution:raw}_prod_v2', 'strategy_pnl_${resolution:raw}_bt_v2', 'strategy_pnl_${resolution:raw}_real_trade_v2']
...
```

- [ ] **Step 6: Commit**

```bash
git add scripts/patch_dashboards_resolution.py infra/grafana/dashboards/
git commit -m "feat: dynamic 1min/1hour table resolution in all Grafana dashboards

Add hidden 'resolution' variable that switches between 1min and 1hour
strategy_pnl tables based on whether the time range start is within
the last 6 hours."
```

---

### Task 2: Manual smoke-test in Grafana Cloud

CI/CD will deploy the JSON automatically on push to `main`. Before pushing, do a quick sanity check that the JSON is valid and the variable query is syntactically correct.

- [ ] **Step 1: Validate all dashboard JSON files are valid JSON**

```bash
python3 -c "
import json, glob, sys
errors = []
for f in sorted(glob.glob('infra/grafana/dashboards/*.json')):
    try:
        json.load(open(f))
    except Exception as e:
        errors.append(f'{f}: {e}')
if errors:
    for e in errors: print('ERROR:', e)
    sys.exit(1)
print('All dashboard JSON files are valid.')
"
```

Expected: `All dashboard JSON files are valid.`

- [ ] **Step 2: Push to main to trigger deploy**

```bash
git push origin main
```

Watch the `deploy-grafana-cloud` job in GitHub Actions. It POSTs each dashboard JSON to Grafana Cloud with `"overwrite": true`. A successful deploy shows HTTP 200 responses for all 5 files.

- [ ] **Step 3: Verify in Grafana Cloud UI — short range**

Open any dashboard in Grafana Cloud and set the time range to **Last 1 hour**. Open the Variables inspector (dashboard settings → Variables or the variable dropdown). Confirm `resolution` resolves to `1min`. Confirm panels load data without errors.

- [ ] **Step 4: Verify in Grafana Cloud UI — long range**

Change the time range to **Last 7 days**. Confirm `resolution` resolves to `1hour`. Confirm panels load data without errors (the 1hour tables cover the same date range).

- [ ] **Step 5: Verify the boundary — exactly 6 hours**

Set time range to **Last 6 hours** (start = now - 6h). The variable query is `>= now() - INTERVAL 6 HOUR`, so this is right at the boundary. At exactly 6h, the start equals the threshold — `resolution` should return `1min`. Set to **Last 7 hours** — `resolution` should return `1hour`.
