"""Position-divergence watcher → Telegram.

Computes the portfolio weighted-average POSITION for prod / real_trade / bt over
the last N complete minutes and alerts when two modes trade meaningfully
different positions for a SUSTAINED stretch:

    wpos(mode, minute) = sum(position * weighting) / sum(weighting)
    bt_prod  = wpos(bt)   - wpos(prod)
    prod_rt  = wpos(prod) - wpos(real_trade)

A pair fires when |divergence| > THRESHOLD for >= SUSTAINED_MIN of the last
WINDOW_MIN complete minutes. "Complete" = a minute carrying all 8 underlyings
(mirrors the L5 dashboard's countDistinct(underlying)=8 filter), so a half-written
live edge never trips a false alarm.

Env:
  CLICKHOUSE_*            connection (see libs/clickhouse_client)
  TELEGRAM_BOT_TOKEN      bot token
  TELEGRAM_CHAT_ID        destination chat id
  DIVERGENCE_THRESHOLD    default 0.05
  DIVERGENCE_WINDOW_MIN   default 15
  DIVERGENCE_SUSTAINED_MIN default 10
  DIVERGENCE_DRY_RUN      "1" → compute + print, never POST
"""

from __future__ import annotations

import os
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass

PAIRS = [("bt", "prod"), ("prod", "real_trade")]
TBL = {
    "prod": "strategy_pnl_1min_prod_v2",
    "real_trade": "strategy_pnl_1min_real_trade_v2",
    "bt": "strategy_pnl_1min_bt_v2",
}
LABEL = {"prod": "Production", "real_trade": "Real-Trade", "bt": "Backtest"}


@dataclass
class Breach:
    pair: tuple[str, str]
    sustained: int
    window: int
    latest: float
    peak: float


def _wpos_series(client, window_min: int) -> dict[str, dict]:
    """Return {mode: {minute: wpos}} over complete minutes in the last window."""
    out: dict[str, dict] = {}
    for mode, tbl in TBL.items():
        rows = client.query(f"""
            SELECT toStartOfMinute(ts) AS t,
                   sum(position * weighting) / nullIf(sum(weighting), 0) AS wp
            FROM analytics.{tbl} FINAL
            WHERE ts >= now() - INTERVAL {window_min + 2} MINUTE
              AND toStartOfMinute(ts) IN (
                SELECT toStartOfMinute(ts) FROM analytics.{tbl} FINAL
                WHERE ts >= now() - INTERVAL {window_min + 2} MINUTE
                GROUP BY toStartOfMinute(ts) HAVING countDistinct(underlying) = 8)
            GROUP BY t ORDER BY t
            """).result_rows
        out[mode] = {t: float(w) for t, w in rows if w is not None}
    return out


def detect(
    series: dict[str, dict], threshold: float, window_min: int, sustained_min: int
) -> list[Breach]:
    breaches = []
    for a, b in PAIRS:
        common = sorted(set(series[a]) & set(series[b]))[-window_min:]
        diffs = [(t, series[a][t] - series[b][t]) for t in common]
        over = [d for _, d in diffs if abs(d) > threshold]
        if len(over) >= sustained_min and diffs:
            breaches.append(
                Breach(
                    pair=(a, b),
                    sustained=len(over),
                    window=len(diffs),
                    latest=diffs[-1][1],
                    peak=max((d for _, d in diffs), key=abs),
                )
            )
    return breaches


def _send_telegram(token: str, chat_id: str, text: str) -> None:
    data = urllib.parse.urlencode(
        {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    ).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage", data=data
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        if r.status != 200:
            raise RuntimeError(f"telegram HTTP {r.status}")


def format_alert(b: Breach, threshold: float) -> str:
    a, c = b.pair
    return (
        f"⚠️ <b>Position divergence: {LABEL[a]} − {LABEL[c]}</b>\n"
        f"|Δposition| &gt; {threshold:g} for <b>{b.sustained}/{b.window}</b> of the "
        f"last minutes.\n"
        f"latest = <code>{b.latest:+.4f}</code>   peak = <code>{b.peak:+.4f}</code>\n"
        f"Dashboard: Strategy PnL — L5 Portfolio"
    )


def run(
    client,
    *,
    token: str,
    chat_id: str,
    threshold: float,
    window_min: int,
    sustained_min: int,
    dry_run: bool,
) -> list[Breach]:
    series = _wpos_series(client, window_min)
    breaches = detect(series, threshold, window_min, sustained_min)
    for b in breaches:
        msg = format_alert(b, threshold)
        print(
            msg.replace("<b>", "")
            .replace("</b>", "")
            .replace("<code>", "")
            .replace("</code>", "")
            .replace("&gt;", ">")
        )
        if not dry_run:
            _send_telegram(token, chat_id, msg)
    if not breaches:
        print(
            f"OK — no sustained divergence > {threshold:g} "
            f"in last {window_min} complete minutes"
        )
    return breaches


def main() -> int:
    from libs.clickhouse_client import get_client

    threshold = float(os.getenv("DIVERGENCE_THRESHOLD", "0.05"))
    window_min = int(os.getenv("DIVERGENCE_WINDOW_MIN", "15"))
    sustained_min = int(os.getenv("DIVERGENCE_SUSTAINED_MIN", "10"))
    dry_run = os.getenv("DIVERGENCE_DRY_RUN", "0") == "1"
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not dry_run and (not token or not chat_id):
        print("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set", file=sys.stderr)
        return 2
    breaches = run(
        get_client(),
        token=token,
        chat_id=chat_id,
        threshold=threshold,
        window_min=window_min,
        sustained_min=sustained_min,
        dry_run=dry_run,
    )
    return 1 if breaches else 0


if __name__ == "__main__":
    raise SystemExit(main())
