"""Unit tests for libs.clickhouse_client connection settings.

The consumer cold-start (bootstrap + walk) runs heavy per-strategy queries. With
ClickHouse Cloud's per-query memory left uncapped (max_memory_usage = 0), a single
bootstrap query can try to grab the whole server's memory and OOM the service
(`(total) memory limit exceeded`). Capping per-query memory + lowering the
external-aggregation/sort thresholds forces disk-spill, so the consumer cannot
monopolize the server and bootstraps fit alongside background merges.

These tests pin the default memory-control settings applied to every client.
"""

from unittest.mock import patch

import pytest


@pytest.mark.unit
def test_get_client_applies_memory_cap_defaults():
    """Every client is created with the memory-cap + disk-spill settings so no
    single consumer query can exhaust the Cloud server's memory."""
    with patch("clickhouse_connect.get_client") as mock_gc:
        from libs.clickhouse_client import _DEFAULT_QUERY_SETTINGS, get_client

        get_client()

    _, kwargs = mock_gc.call_args
    settings = kwargs.get("settings")
    assert settings is not None, "get_client must pass a settings dict"
    # per-query memory is bounded (not 0/unlimited)
    assert settings["max_memory_usage"] == _DEFAULT_QUERY_SETTINGS["max_memory_usage"]
    assert 0 < settings["max_memory_usage"] <= 4_000_000_000
    # GROUP BY / ORDER BY spill to disk below the per-query cap
    assert settings["max_bytes_before_external_group_by"] < settings["max_memory_usage"]
    assert settings["max_bytes_before_external_sort"] < settings["max_memory_usage"]


@pytest.mark.unit
def test_get_client_merges_caller_settings_over_defaults():
    """A caller can override or extend the defaults; explicit settings win, but
    the memory caps stay in place unless explicitly overridden."""
    with patch("clickhouse_connect.get_client") as mock_gc:
        from libs.clickhouse_client import get_client

        get_client(settings={"max_execution_time": 300})

    _, kwargs = mock_gc.call_args
    settings = kwargs["settings"]
    # caller-supplied setting present
    assert settings["max_execution_time"] == 300
    # default memory cap still applied
    assert settings["max_memory_usage"] > 0


@pytest.mark.unit
def test_get_client_caller_can_override_a_default():
    """Explicit caller value for a defaulted key takes precedence."""
    with patch("clickhouse_connect.get_client") as mock_gc:
        from libs.clickhouse_client import get_client

        get_client(settings={"max_memory_usage": 8_000_000_000})

    _, kwargs = mock_gc.call_args
    assert kwargs["settings"]["max_memory_usage"] == 8_000_000_000
