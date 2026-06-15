# Grafana Cloud alerting — position divergence

Telegram alerts when the portfolio's weighted-average **position** diverges
between PnL modes for a sustained stretch. Pairs with the L5 dashboard's
"Position Divergence" panels (`strategy-pnl-l5-portfolio.json`).

## What gets provisioned

| Resource | UID | Notes |
|----------|-----|-------|
| Contact point (Telegram) | `telegram-divergence` | bot token + chat id injected from secrets |
| Alert rule — Backtest − Production | `divergence-bt-prod` | fires when \|Δposition\| > 0.05 for 10m |
| Alert rule — Production − Real-Trade | `divergence-prod-rt` | fires when \|Δposition\| > 0.05 for 10m |
| Folder | `divergence-alerts` | holds the rule group `divergence` (60s eval) |

Each rule queries the ClickHouse datasource for the last 15 complete minutes
(`countDistinct(underlying)=8`, same completeness gate as the panels), computes
`wpos(a) − wpos(b)` where `wpos = sum(position*weighting)/sum(weighting)`,
reduces to the latest minute, and thresholds on **outside [−0.05, 0.05]**. The
`for: 10m` makes it a sustained-breach alert, not a single-minute blip. Rules
route straight to the Telegram contact point via per-rule notification settings,
so the global notification policy tree is left untouched.

### Why threshold 0.05

`prod−rt` normally sits at ~0 (same source). `bt−prod` carries a structural
~0.01–0.02 baseline (known position-source disagreements on a handful of
strategies). 0.05 clears both the baseline and transient recompute artifacts
(the largest seen was 0.019) while still catching a real regime split. Tune via
`THRESHOLD` in `gen_rules.py` (then re-run it) and `for`/window there too.

## Required secrets (GitHub Actions repo secrets)

| Secret | Purpose |
|--------|---------|
| `GRAFANA_CLOUD_URL` | already set (dashboard deploy) |
| `GRAFANA_CLOUD_TOKEN` | already set; needs Alerting **write** scope |
| `TELEGRAM_BOT_TOKEN` | **add** — `@falcon_finance_data_devops_bot` token |
| `TELEGRAM_CHAT_ID` | **add** — destination chat id |

```bash
gh secret set TELEGRAM_BOT_TOKEN --repo williamwxz/trading-analysis
gh secret set TELEGRAM_CHAT_ID   --repo williamwxz/trading-analysis
```

## Deployment

Provisioned by the `deploy-grafana-cloud` job on push to `main` (whenever
`infra/grafana/**` changes), as a **non-fatal** step — an alerting error never
blocks the dashboard/deploy pipeline. The step skips cleanly until the two
Telegram secrets exist.

Re-run on demand: `workflow_dispatch` on `ci-cd.yml`, or locally:

```bash
export GRAFANA_CLOUD_URL=...  GRAFANA_CLOUD_TOKEN=...
export TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=...
bash scripts/provision_grafana_alerts.sh
```

The script is idempotent (fixed UIDs, PUT-then-POST upserts).

## Editing the rules

`rules-divergence.json` is generated — edit `gen_rules.py` and re-run it:

```bash
python3 infra/grafana/alerting/gen_rules.py
```

Validate the rule SQL against ClickHouse before deploying (returns one value per
minute; `|last|` is what the alert thresholds on).
