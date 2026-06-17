# Grafana Cloud alerting — position divergence

Telegram alerts when the portfolio's weighted-average **position** diverges
between PnL modes for a sustained stretch. Pairs with the L5 dashboard's
"Position Divergence" panels (`strategy-pnl-l5-portfolio.json`).

## What gets provisioned

| Resource | UID | Notes |
|----------|-----|-------|
| Contact point (Telegram) | `telegram-divergence` | bot token + chat id injected from secrets |
| Position rule — Backtest − Production | `divergence-bt-prod` | fires when \|Δposition\| > 0.05 for 10m |
| Position rule — Production − Real-Trade | `divergence-prod-rt` | fires when \|Δposition\| > 0.05 for 10m |
| PnL rule — Production − Real-Trade | `divergence-pnl-prod-rt` | fires when \|Δcumulative_pnl\| > 0.015 for 10m |
| Folder | `divergence-alerts` | holds the rule group `divergence` (60s eval) |

Each rule queries the ClickHouse datasource for the last 15 complete minutes
(`countDistinct(underlying)=8`, same completeness gate as the panels), computes
`wagg(a) − wagg(b)` where `wagg = sum(metric*weighting)/sum(weighting)` (metric =
`position` or `cumulative_pnl`), reduces to the latest minute, and thresholds on
**outside [−T, T]**. The `for: 10m` makes it a sustained-breach alert, not a
single-minute blip. Rules route straight to the Telegram contact point via
per-rule notification settings, so the global notification policy tree is left
untouched.

### Thresholds

- **Position 0.05** — `prod−rt` normally sits at ~0 (positions match); `bt−prod`
  carries a structural ~0.01–0.02 baseline (known position-source disagreements
  on a handful of strategies). 0.05 clears both and transient recompute artifacts
  (largest seen 0.019) while still catching a real regime split.
- **PnL 0.015** (`prod−rt` cumulative_pnl) — this spread carries a *legitimate*
  structural offset: rt fills at `revision_ts`, prod at bar close, so when
  revisions move a position the two accrue different PnL. The offset has ranged
  within ~[−0.009, +0.003] historically; 0.015 clears that envelope so the alert
  fires only on a genuine break (e.g. a large re-anchor seam), not the baseline.
  bt is excluded — its `cumulative_pnl` is raw row_json (~+4 scale), not comparable.

Tune the per-rule threshold (and `FOR`/`WINDOW_MIN`) in the `RULES` table in
`gen_rules.py`, then re-run it to regenerate `rules-divergence.json`.

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
