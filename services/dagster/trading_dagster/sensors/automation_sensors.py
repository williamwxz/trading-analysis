"""
Automation Condition Sensors

A single sensor that evaluates declarative automation conditions for the entire asset graph.
"""

from typing import List
from dagster import AutomationConditionSensorDefinition, AssetSelection, DefaultSensorStatus


def build_automation_sensors() -> List[AutomationConditionSensorDefinition]:
    """Build a single automation sensor for the project."""

    return [
        AutomationConditionSensorDefinition(
            name="trading_analysis_automation_sensor",
            target=AssetSelection.all(),
            default_status=DefaultSensorStatus.RUNNING,
            minimum_interval_seconds=30,
            description="Evaluates all automation conditions (Market Data -> PnL)",
        ),
    ]


__all__ = ["build_automation_sensors"]
