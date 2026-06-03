"""Unit tests for the Grafana dashboard SQL patcher.

The patcher rewrites PnL family references into a 3-branch UNION ALL:
- 1min for ts >= toStartOfHour(now() - 6h)
- 1hour for toStartOfDay(now() - 30d) <= ts < toStartOfHour(now() - 6h)
- 1day for ts < toStartOfDay(now() - 30d)

All tests target the pure function ``patch_sql(raw_sql) -> (rewritten, n)``.
"""

import sys
from pathlib import Path

# Make scripts/ importable.
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "scripts"))

from patch_dashboards_resolution import patch_sql  # noqa: E402


_1MIN_WHERE = "ts >= toStartOfHour(now() - INTERVAL 6 HOUR)"
_1HOUR_WHERE = (
    "ts >= toStartOfDay(now() - INTERVAL 30 DAY) "
    "AND ts < toStartOfHour(now() - INTERVAL 6 HOUR)"
)
_1DAY_WHERE = "ts < toStartOfDay(now() - INTERVAL 30 DAY)"


def _three_branch(family: str, final: str = "") -> str:
    """Canonical 3-branch UNION ALL for one PnL family."""
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


class TestBareReferences:
    def test_bare_1min_prod_rewrites_to_3_branch(self):
        sql = "SELECT * FROM analytics.strategy_pnl_1min_prod_v2 WHERE foo = 1"
        out, n = patch_sql(sql)
        expected = (
            f"SELECT * FROM {_three_branch('prod_v2')} WHERE foo = 1"
        )
        assert out == expected
        assert n == 1
