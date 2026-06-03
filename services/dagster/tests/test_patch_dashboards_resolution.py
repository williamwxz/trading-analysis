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


def _two_branch_deployed(family: str, final: str = "") -> str:
    """The 2-branch UNION ALL shape currently committed in dashboards."""
    return (
        f"(SELECT * FROM analytics.strategy_pnl_1min_{family}{final}"
        f" WHERE ts >= toStartOfHour(now() - INTERVAL 6 HOUR)"
        f" UNION ALL"
        f" SELECT * FROM analytics.strategy_pnl_1hour_{family}{final}"
        f" WHERE ts < toStartOfHour(now() - INTERVAL 6 HOUR))"
    )


class TestBareReferences:
    def test_bare_1min_prod_rewrites_to_3_branch(self):
        sql = "SELECT * FROM analytics.strategy_pnl_1min_prod_v2 WHERE foo = 1"
        out, n = patch_sql(sql)
        expected = f"SELECT * FROM {_three_branch('prod_v2')} WHERE foo = 1"
        assert out == expected
        assert n == 1

    def test_bare_1hour_bt_rewrites_to_3_branch(self):
        sql = "SELECT * FROM analytics.strategy_pnl_1hour_bt_v2 WHERE foo = 1"
        out, n = patch_sql(sql)
        expected = f"SELECT * FROM {_three_branch('bt_v2')} WHERE foo = 1"
        assert out == expected
        assert n == 1

    def test_bare_with_final_preserves_final_in_all_branches(self):
        sql = (
            "SELECT * FROM analytics.strategy_pnl_1min_real_trade_v2 FINAL"
            " WHERE foo = 1"
        )
        out, n = patch_sql(sql)
        expected = (
            f"SELECT * FROM {_three_branch('real_trade_v2', ' FINAL')} WHERE foo = 1"
        )
        assert out == expected
        assert n == 1
        # Each branch keeps FINAL exactly once.
        assert out.count(" FINAL") == 3

    def test_two_bare_refs_in_one_sql_get_both_rewritten(self):
        sql = (
            "SELECT * FROM analytics.strategy_pnl_1min_prod_v2 WHERE ts > now()"
            " UNION ALL"
            " SELECT * FROM analytics.strategy_pnl_1hour_bt_v2 WHERE ts < now()"
        )
        out, n = patch_sql(sql)
        assert n == 2
        assert out.count("UNION ALL") == 5  # original + 2 refs × 2 UNION ALLs each
        assert "strategy_pnl_1day_prod_v2" in out
        assert "strategy_pnl_1day_bt_v2" in out


class TestUnrelatedSQL:
    def test_sql_without_pnl_refs_is_unchanged(self):
        sql = "SELECT 1 AS x FROM analytics.futures_price_1min WHERE ts > now()"
        out, n = patch_sql(sql)
        assert out == sql
        assert n == 0

    def test_empty_string_is_unchanged(self):
        out, n = patch_sql("")
        assert out == ""
        assert n == 0


class TestTwoBranchUpgrade:
    def test_two_branch_prod_upgrades_to_3_branch(self):
        sql = f"SELECT time FROM {_two_branch_deployed('prod_v2')} GROUP BY time"
        out, n = patch_sql(sql)
        expected = f"SELECT time FROM {_three_branch('prod_v2')} GROUP BY time"
        assert out == expected
        assert n == 1

    def test_two_branch_with_final_preserves_final(self):
        sql = f"SELECT * FROM {_two_branch_deployed('real_trade_v2', ' FINAL')}"
        out, n = patch_sql(sql)
        expected = f"SELECT * FROM {_three_branch('real_trade_v2', ' FINAL')}"
        assert out == expected
        assert n == 1
        assert out.count(" FINAL") == 3

    def test_two_distinct_2branch_refs_in_one_sql_both_upgrade(self):
        """A single SQL with two distinct 2-branch UNION ALL blocks (e.g., a
        JOIN across two PnL families) upgrades both blocks to 3-branch.
        """
        sql = (
            f"SELECT a.* FROM {_two_branch_deployed('prod_v2')} a"
            f" JOIN {_two_branch_deployed('bt_v2')} b ON a.ts = b.ts"
        )
        out, n = patch_sql(sql)
        expected = (
            f"SELECT a.* FROM {_three_branch('prod_v2')} a"
            f" JOIN {_three_branch('bt_v2')} b ON a.ts = b.ts"
        )
        assert out == expected
        assert n == 2


class TestIdempotency:
    def test_three_branch_input_is_unchanged(self):
        sql = f"SELECT * FROM {_three_branch('bt_v2')}"
        out, n = patch_sql(sql)
        assert out == sql
        assert n == 0

    def test_three_branch_with_final_is_unchanged(self):
        sql = f"SELECT * FROM {_three_branch('prod_v2', ' FINAL')}"
        out, n = patch_sql(sql)
        assert out == sql
        assert n == 0
