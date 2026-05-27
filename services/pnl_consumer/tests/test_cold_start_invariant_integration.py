"""Integration test for cold_start.clickhouse_invariant_check.

Requires CLICKHOUSE_* env vars pointing to a live ClickHouse with
analytics.strategy_output_history_v2 populated.

The test seeds a synthetic strategy_instance_id (uses a unique uuid suffix)
so it doesn't interfere with production data, then queries through the
invariant check function.
"""

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from libs.clickhouse_client import execute as ch_execute
from libs.clickhouse_client import get_client as ch_get_client
from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.checkpoint_store import (
    SCHEMA_VERSION,
    CheckpointLoadResult,
    CommitStateRow,
    compute_state_hash,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def ch_client():
    if not os.getenv("CLICKHOUSE_HOST"):
        pytest.skip("CLICKHOUSE_HOST not set — skipping integration test")
    client = ch_get_client()
    yield client
    client.close()


def _make_result(
    strategy_instance_id: str, position: float, as_of: datetime
) -> CheckpointLoadResult:
    state = AnchorState()
    state.set(
        "strat_a",
        AnchorRecord(
            pnl=1.0,
            price=100.0,
            position=position,
            bar_ts=as_of,
            revision_ts=as_of,
            strategy_id=1,
            strategy_name="invariant-test",
            underlying="BTC",
            config_timeframe="1m",
            weighting=1.0,
            strategy_instance_id=strategy_instance_id,
            final_signal=0.0,
            benchmark=0.0,
        ),
    )
    cs = CommitStateRow(
        mode="prod",
        last_candle_ts=as_of,
        kafka_topic="binance.price.ticks",
        kafka_partition=0,
        kafka_offset=100,
        state_hash=compute_state_hash(state),
        schema_version=SCHEMA_VERSION,
    )
    return CheckpointLoadResult(commit_state=cs, anchor_state=state)


def test_invariant_passes_when_position_matches(ch_client):
    from pnl_consumer.cold_start import clickhouse_invariant_check

    iid = f"invariant-test-{uuid.uuid4().hex[:8]}"
    as_of = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=10)
    rev_ts = as_of

    # Seed a single row in strategy_output_history_v2.
    row_json = '{"position": 2.5, "cumulative_pnl": 0, "price": 100}'
    ch_execute(
        f"""
        INSERT INTO analytics.strategy_output_history_v2
            (strategy_id, strategy_name, strategy_instance_id, underlying,
             config_timeframe, weighting, ts, revision_ts, row_json,
             source_label, final_signal, benchmark)
        VALUES (1, 'invariant-test', '{iid}', 'BTC', '1m', 1.0,
                toDateTime('{as_of.isoformat()}'),
                toDateTime('{rev_ts.isoformat()}'),
                '{row_json}', 'production', 0, 0)
        """,
        client=ch_client,
    )

    result = _make_result(iid, position=2.5, as_of=as_of)
    reason = clickhouse_invariant_check(mode="prod", result=result, ch_client=ch_client)
    assert reason is None


def test_invariant_fails_when_position_diverges(ch_client):
    from pnl_consumer.cold_start import FallbackReason, clickhouse_invariant_check

    iid = f"invariant-test-{uuid.uuid4().hex[:8]}"
    as_of = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=10)
    rev_ts = as_of

    row_json = '{"position": 2.5, "cumulative_pnl": 0, "price": 100}'
    ch_execute(
        f"""
        INSERT INTO analytics.strategy_output_history_v2
            (strategy_id, strategy_name, strategy_instance_id, underlying,
             config_timeframe, weighting, ts, revision_ts, row_json,
             source_label, final_signal, benchmark)
        VALUES (1, 'invariant-test', '{iid}', 'BTC', '1m', 1.0,
                toDateTime('{as_of.isoformat()}'),
                toDateTime('{rev_ts.isoformat()}'),
                '{row_json}', 'production', 0, 0)
        """,
        client=ch_client,
    )

    # Checkpoint claims position=3.0 but history has 2.5.
    result = _make_result(iid, position=3.0, as_of=as_of)
    reason = clickhouse_invariant_check(mode="prod", result=result, ch_client=ch_client)
    assert reason == FallbackReason.INVARIANT_MISMATCH
