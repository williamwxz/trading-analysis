"""E2E tests for cold-start against a real Postgres (testcontainers).

These verify the full write → read → sanity-check → fallback decision tree
against an actual Postgres backend. ClickHouse and Kafka are mocked out —
this test exercises Supabase semantics end-to-end.
"""

from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest

pytestmark = pytest.mark.streaming_integration


@pytest.fixture(scope="module")
def pg_container():
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture
def pg_env(pg_container, monkeypatch):
    monkeypatch.setenv("SUPABASE_HOST", pg_container.get_container_host_ip())
    monkeypatch.setenv("SUPABASE_PORT", str(pg_container.get_exposed_port(5432)))
    monkeypatch.setenv("SUPABASE_USER", "test")
    monkeypatch.setenv("SUPABASE_PASSWORD", "test")
    monkeypatch.setenv("SUPABASE_DATABASE", "test")
    monkeypatch.setenv("SUPABASE_SSLMODE", "disable")
    yield


@pytest.fixture
def pg_client(pg_env):
    from libs.postgres_client import get_client

    client = get_client()
    # Apply schema.
    with open("infra/schemas/streaming_supabase.sql") as f:
        ddl = f.read()
    with client.cursor() as cur:
        cur.execute(ddl)
    client.commit()
    yield client
    # Clean up tables between tests (the container persists across module scope).
    with client.cursor() as cur:
        cur.execute("TRUNCATE TABLE streaming.pnl_checkpoint")
        cur.execute("TRUNCATE TABLE streaming.pnl_commit_state")
    client.commit()
    client.close()


def _make_anchor_state():
    from libs.computation.anchor_state import AnchorRecord, AnchorState

    s = AnchorState()
    s.set(
        "strat_a",
        AnchorRecord(
            pnl=1.0,
            price=100.0,
            position=1.0,
            bar_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
            revision_ts=datetime(2024, 5, 31, 12, 0, tzinfo=UTC),
            strategy_id=1,
            strategy_name="s",
            underlying="BTC",
            config_timeframe="1m",
            weighting=1.0,
            strategy_instance_id="i",
            final_signal=0.0,
            benchmark=0.0,
        ),
    )
    return s


def test_first_start_falls_back_to_bootstrap(pg_client):
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap

    from libs.computation.checkpoint_store import read_checkpoint

    bootstrap_state = _make_anchor_state()
    fake_bootstrap = mock.MagicMock(return_value=bootstrap_state)

    state, reason = load_or_bootstrap(
        mode="prod",
        pg_client=pg_client,
        kafka_committed_offset=101,
        now=datetime.now(UTC),
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=read_checkpoint,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason == FallbackReason.NO_CHECKPOINT
    fake_bootstrap.assert_called_once()
    assert state is bootstrap_state


def test_warm_restart_uses_checkpoint(pg_client):
    from pnl_consumer.cold_start import load_or_bootstrap

    from libs.computation.checkpoint_store import read_checkpoint, write_checkpoint

    state = _make_anchor_state()
    last_candle_ts = datetime.now(UTC) - timedelta(minutes=1)
    write_checkpoint(
        mode="prod",
        anchor_state=state,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=100,
        last_candle_ts=last_candle_ts,
        client=pg_client,
    )

    fake_bootstrap = mock.MagicMock()  # must not be called
    loaded, reason = load_or_bootstrap(
        mode="prod",
        pg_client=pg_client,
        kafka_committed_offset=101,  # one ahead, matches semantics
        now=datetime.now(UTC),
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=read_checkpoint,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason is None
    fake_bootstrap.assert_not_called()
    assert "strat_a" in loaded.keys()
    assert loaded.get("strat_a").pnl == 1.0


def test_offset_lag_after_write_failure_triggers_fallback(pg_client):
    """Simulate Supabase outage: checkpoint at offset 50, Kafka committed at 101."""
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap

    from libs.computation.checkpoint_store import read_checkpoint, write_checkpoint

    state = _make_anchor_state()
    write_checkpoint(
        mode="prod",
        anchor_state=state,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=50,
        last_candle_ts=datetime.now(UTC) - timedelta(minutes=1),
        client=pg_client,
    )

    fake_bootstrap = mock.MagicMock(return_value=_make_anchor_state())
    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=pg_client,
        kafka_committed_offset=101,
        now=datetime.now(UTC),
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=read_checkpoint,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason == FallbackReason.OFFSET_LAG
    fake_bootstrap.assert_called_once()


def test_schema_version_bump_falls_back(pg_client):
    """Manually write a row with version=99, then load with current SCHEMA_VERSION."""
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap

    from libs.computation.checkpoint_store import (
        SCHEMA_VERSION,
        read_checkpoint,
        write_checkpoint,
    )

    state = _make_anchor_state()
    write_checkpoint(
        mode="prod",
        anchor_state=state,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=100,
        last_candle_ts=datetime.now(UTC) - timedelta(minutes=1),
        client=pg_client,
    )
    # Tamper schema_version after the legitimate write.
    with pg_client.cursor() as cur:
        cur.execute(
            "UPDATE streaming.pnl_commit_state"
            " SET schema_version = %s WHERE mode = 'prod'",
            (SCHEMA_VERSION + 99,),
        )
    pg_client.commit()

    fake_bootstrap = mock.MagicMock(return_value=_make_anchor_state())
    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=pg_client,
        kafka_committed_offset=101,
        now=datetime.now(UTC),
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=read_checkpoint,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason == FallbackReason.SCHEMA_VERSION_MISMATCH


def test_hash_tamper_falls_back(pg_client):
    """Tamper one pnl value after write; verify hash check rejects the read."""
    from pnl_consumer.cold_start import FallbackReason, load_or_bootstrap

    from libs.computation.checkpoint_store import read_checkpoint, write_checkpoint

    state = _make_anchor_state()
    write_checkpoint(
        mode="prod",
        anchor_state=state,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=100,
        last_candle_ts=datetime.now(UTC) - timedelta(minutes=1),
        client=pg_client,
    )
    with pg_client.cursor() as cur:
        cur.execute(
            "UPDATE streaming.pnl_checkpoint"
            " SET pnl = pnl + 999.0"
            " WHERE mode = 'prod' AND strategy_table_name = 'strat_a'"
        )
    pg_client.commit()

    fake_bootstrap = mock.MagicMock(return_value=_make_anchor_state())
    _, reason = load_or_bootstrap(
        mode="prod",
        pg_client=pg_client,
        kafka_committed_offset=101,
        now=datetime.now(UTC),
        max_age_seconds=86400,
        invariant_check_enabled=False,
        read_checkpoint_fn=read_checkpoint,
        bootstrap_fn=fake_bootstrap,
        invariant_check_fn=None,
    )
    assert reason == FallbackReason.HASH_MISMATCH
