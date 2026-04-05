"""
Automation Condition Sensors

Two sensors covering non-overlapping asset groups:
- market_data_automation_sensor: Binance OHLCV polling (2-min cron)
- strategy_pnl_automation_sensor: PnL refresh + rollup + safety scan (5/10-min cron)

Simplified from falcon-lakehouse (had 6 sensors for amber/starrocks/clickhouse).
"""

from typing import List
from dagster import AutomationConditionSensorDefinition, AssetSelection, DefaultSensorStatus


def build_automation_sensors() -> List[AutomationConditionSensorDefinition]:
    """Build automation condition sensors for the two asset groups."""

    return [
        # Market data sensor (Binance OHLCV polling)
        AutomationConditionSensorDefinition(
            name="market_data_automation_sensor",
            target=AssetSelection.groups("market_data"),
            default_status=DefaultSensorStatus.RUNNING,
            minimum_interval_seconds=60,
            description="Evaluates automation conditions for Binance futures OHLCV polling (2-min cron)",
        ),

        # Strategy PnL sensor (refresh + rollup + safety scan)
        AutomationConditionSensorDefinition(
            name="strategy_pnl_automation_sensor",
            target=AssetSelection.groups("strategy_pnl"),
            default_status=DefaultSensorStatus.RUNNING,
            minimum_interval_seconds=60,
            description="Evaluates automation conditions for PnL refresh (5/10-min), rollup (hourly), and safety scan (daily)",
        ),
    ]


__all__ = ["build_automation_sensors"]
