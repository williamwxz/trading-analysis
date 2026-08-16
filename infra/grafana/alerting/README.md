# Grafana Cloud alerting — divergence + source freshness

Two families of Telegram alert:

- **`divergence`** — weighted-average **position** or **PnL** diverges between
  modes for a sustained stretch. Position is judged **per underlying** and pairs
  with the L4 dashboard's "Position Divergence" panel
  (`strategy-pnl-l4-underlying.json`); PnL stays portfolio-wide and pairs with
  the L5 "PnL Divergence" panel (`strategy-pnl-l5-portfolio.json`).
- **`source-freshness`** — the upstream table the PnL is computed *from* stopped
  publishing. These cannot be inferred from the divergence rules; see below.

## What gets provisioned

| Resource | UID | Notes |
|----------|-----|-------|
| Contact point (Telegram) | `telegram-divergence` | bot token + chat id injected from secrets |
| Position rule — Backtest − Production, **per underlying** | `divergence-bt-prod-per-underlying` | one alert instance per (underlying, sid, sno); fires when that coin's \|Δposition\| exceeds its own threshold for 10m |
| PnL rule — Production − Real-Trade | `divergence-pnl-prod-rt` | fires when \|Δcumulative_pnl\| > 0.015 for 10m |
| Source rule — bar coverage | `source-bar-coverage` | fires when a settled bar-hour has < 98.5% of the usual 1h strategies, for 30m |
| Source rule — revision arrival | `source-revision-arrival` | fires when publishing breadth drops below 50% of the 24h median, for 1h |
| Folder | `divergence-alerts` | holds rule groups `divergence` (60s eval) and `source-freshness` (300s eval) |

### Why the source-freshness rules exist

When a source bar is missing the consumer carries the last position forward and
still writes a complete row for every strategy every minute. Row counts,
strategy counts and underlying-completeness all stay perfect — the position is
just silently stale, and because `cumulative_pnl` is a chained anchor quantity
the resulting error is **permanent**. The divergence rules compare PnL modes
against each other and therefore cannot see this at all.

Three incidents in the week of 2026-07-24 raised nothing:

| When | What | Impact |
|------|------|--------|
| 07-25 05:00–21:00 (17h) | 12 strategies stopped publishing bars | −0.0019% wtd prod PnL |
| 07-26 08:00–22:00 (15h) | 87 strategies stopped publishing bars | **+0.1276%** wtd prod PnL |
| 07-28 03:00–07:00 (4h) | publisher stall (16–18 strategies emitting), then ~168k revisions carrying bars up to 144h old | −0.0111% wtd prod PnL |

Incidents 1–2 are bar-coverage failures; incident 3 is an arrival failure with
eventually-complete coverage — hence two detectors. Backtested over that week:
the coverage rule flags 32 hours (exactly Gaps A and B) and the arrival rule
flags 4 hours (exactly the stall), with no other hits.

Both are **ratio-based against a trailing 24h baseline**, not absolute counts,
so they survive roster changes (695 total = 614 `1h` + 2 `10m` + 79 `1d`)
without a threshold edit. Coverage judges a bar-hour only after a 3h settle,
since first revisions land p50 ~16m / p90 ~72m / p99 ~96m past bar close.

Both rules query the ClickHouse datasource for the last 15 complete minutes
(every underlying written), reduce to the latest minute, and threshold. The
`for: 10m` makes them sustained-breach alerts, not single-minute blips. Rules
route straight to the Telegram contact point via per-rule notification settings,
so the global notification policy tree is left untouched.

**The completeness gate derives the roster from the window; never hardcode it.**
The L5 panels and `divergence_alert.py` still use a literal
`countDistinct(underlying) = 8`, but the alert rules compare against
`(SELECT countDistinct(underlying) FROM <same table, same window>)`. An exact
`= 8` matches **zero** minutes the day a ninth instrument is onboarded (verified
against live data 2026-08-14 by injecting a synthetic 9th underlying), and since
both rules set `noDataState: OK` that silently switches alerting off rather than
failing loudly — it would also make `DEFAULT_THRESHOLD` unreachable, since the
gate breaks before a new coin's default could ever apply. Onboarding an
instrument already means editing `INSTRUMENTS` in two places (see CLAUDE.md); it
must not also mean quietly losing divergence alerting.

A coin that stops writing for a whole window shrinks the derived roster instead
of wedging the gate. That is the better failure mode: the per-underlying rule
simply has no instances for the missing coin and the other seven keep alerting.
Detecting genuinely absent source data is the source-freshness rules' job.

### Thresholds

**Position — per underlying.** A flat threshold is not viable. The old 0.05 was
tuned on the *portfolio* weighted average, where per-coin excursions cancel;
applied per coin it would have been in sustained breach for 14% of all minutes on
AVAX and 12% on ADA. A coin with 35–66 strategies moves its own weighted average
far more on one late bar than the 695-strategy portfolio does.

| coin | strategies | max sustained \|Δ\| (21d) | threshold | firings in 21d |
|------|-----------:|--------------------------:|----------:|----------------|
| BTC  | 116 | 0.137 | 0.15 | 0 |
| ETH  | 137 | 0.220 | 0.15 | 1 (1 min) |
| SOL  | 136 | 0.190 | 0.20 | 0 |
| ADA  | 86  | 0.242 | 0.25 | 0 |
| DOGE | 35  | 0.279 | 0.25 | 1 (1 min) |
| AVAX | 66  | 0.280 | 0.30 | 0 |
| XRP  | 75  | 0.500 | 0.30 | 2 (longest 51 min) |
| FET  | 44  | 0.870 | 0.40 | 2 (longest 6 min) |

Six firings over 21 days, every one a discrete episode. A new underlying falls
back to `DEFAULT_THRESHOLD` (0.25) until measured. Measured 2026-08-14; re-measure
when the roster changes materially, chunking **per coin and per week** — a query
joining per-strategy rows across 21 days OOMs ClickHouse Cloud at 7.2 GiB (code
241), and per-coin chunking alone still OOMs on the largest coins (ETH, 137
strategies) when the cluster is under concurrent load. `max_threads: 2` and
`join_algorithm: 'grace_hash'` help; retry on 241 rather than assuming drift.

Because a Grafana rule applies one threshold to all its instances, the per-coin
numbers live in the SQL: the rule's value is `|Δposition| / T_coin` and the
condition is a flat `> 1`.

**Why the alert names strategies as dimensions.** Grafana derives alert-instance
identity from labels, so a "top 5" rendered as one churning string label would
mint a fresh instance on every evaluation and the 10-minute `for` timer would
never mature. Instead the query returns one row per `(underlying, sid, sno)` for
each coin's 5 largest contributors, so each instance has a stable identity.
`sid` and `sno` are extracted straight from `strategy_table_name`
(`sid=20|sno=301|u=BTC|...`) — verified against all 695 live strategies with zero
mismatches, so no join to `strategy_output_history_v2` is needed.

Two details are load-bearing and must not be "simplified" away:

- Ranking uses a **60-minute** window, longer than the 17-minute value window.
  On the two real episodes in the 21d history a 17-minute ranking window
  fragments membership into runs as short as 2 minutes; 60 minutes gives one
  stable run each (XRP 50 min, FET 21 min).
- Ranking carries an explicit **`ORDER BY contrib DESC, s ASC`** tie-break. Many
  strategies sit at exactly zero contribution, and without a deterministic second
  key `row_number()` is arbitrary among them.

Expect the first notification ~16 minutes into a breach rather than 10 —
membership is still settling in an episode's opening minutes. The coin-level
condition itself is met at 10; only the naming of offenders lags.

**PnL 0.015** (`prod−rt` cumulative_pnl) — unchanged, portfolio-aggregate. This
spread carries a *legitimate* structural offset: rt fills at `revision_ts`, prod
at bar close, so when revisions move a position the two accrue different PnL. The
offset has ranged within ~[−0.009, +0.003] historically; 0.015 clears that
envelope so the alert fires only on a genuine break (e.g. a large re-anchor
seam), not the baseline. bt is excluded — its `cumulative_pnl` is raw row_json
(~+4 scale), not comparable.

Tune the thresholds (and `FOR`/`WINDOW_MIN`/`RANK_WINDOW_MIN`) in `gen_rules.py`,
then re-run it to regenerate `rules-divergence.json`.

### Notification format

The Telegram contact point carries an explicit `message` template. Grafana's
default renders, **per alert instance**, the value, the full label set, every
annotation, a Source link and a Silence link (a long query string). A breaching
coin fires all 5 of its instances at once and they group into one notification,
so the default produced ~90 lines for what is really five short facts.

The template renders one line per instance from `summary` alone:

```
Position divergence per underlying: Backtest − Production
🔴 AVAX sid=12 sno=171 — 1.034x threshold
🔴 AVAX sid=12 sno=180 — 1.034x threshold
...
```

Two consequences for anyone editing rules:

- **Every rule routed to this contact point must set a `summary` annotation**,
  and it should be one short line — it is the only thing rendered. Standing
  context (thresholds, what the ratio means, which dashboard) belongs in this
  README, not in an annotation that repeats five times per notification.
- **Do not add other annotations to the per-underlying rule** expecting them to
  show up; they will not be rendered.

The alert value is rounded to 3dp in SQL rather than formatted in the template,
so `{{ $values.B }}` needs no template functions. `tests/test_contact_point.py`
pins the template shape and checks every routed rule still provides a `summary`.

### Retired rules

`divergence-bt-prod` and `divergence-prod-rt` (both portfolio-aggregate position)
were retired on 2026-08-14. Because the provisioning script upserts by uid and
never prunes, it DELETEs these two uids explicitly; a 404 means already gone, so
it is idempotent. Do not re-add them to `rules-divergence.json`.

**The delete runs after the upsert, and only if `divergence-bt-prod-per-underlying`
was confirmed installed.** Deleting first would mean a transient PUT+POST failure
leaves no position-divergence coverage at all — and because the provisioning step
is `continue-on-error: true` in `ci-cd.yml`, that outage would persist silently
until some later successful run. If the replacement does not install, the script
logs `SKIPPED` and leaves the old rules in place.

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

Both rule files are generated — edit the generator and re-run it:

```bash
python3 infra/grafana/alerting/gen_rules.py                    # rules-divergence.json
python3 infra/grafana/alerting/gen_source_freshness_rules.py   # rules-source-freshness.json
```

Always execute the generated SQL against live ClickHouse before deploying — the
unit tests do not run it, and a rule that fails to parse silently goes to
`Alerting` via `execErrState`. The per-underlying divergence query should return
exactly 40 rows per minute (8 underlyings × 5 contributors) with ratios below 1
on a healthy system; the PnL query returns one value per minute (`last` is
thresholded); the source-freshness queries return a single ratio that should read
~1.0 on a healthy system.
