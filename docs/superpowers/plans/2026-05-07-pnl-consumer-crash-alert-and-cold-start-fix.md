# PnL Consumer Crash Alert + Cold-Start Window Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a CloudWatch alarm that fires when any pnl-consumer ECS service crashes ≥ 3 times in 10 minutes, and fix the cold-start bootstrap to use the Redpanda committed offset timestamp (not wall-clock `now()`) as the reference point for the 3-day verification window.

**Architecture:** The crash alarm is a `for_each` Terraform resource over the 4 pnl-consumer sink keys, using the Container Insights `StoppedTaskCount` metric dimensioned by cluster + service name. The cold-start fix adds a `peek_reference_ts()` helper that creates a short-lived Kafka consumer, calls `committed()` to find this group's committed offsets, seeks to those offsets, polls one message per partition, and returns the minimum candle `ts` found — that timestamp replaces `now()` in the two ClickHouse queries inside `_recompute_and_verify`.

**Tech Stack:** Terraform (AWS provider ~5.0), Python 3.11, confluent-kafka, pytest

---

## File Map

| File | Change |
|------|--------|
| `infra/terraform/main.tf` | Add `aws_cloudwatch_metric_alarm.pnl_consumer_crash` (for_each over sink keys) |
| `pnl_consumer/pnl_consumer.py` | Add `peek_reference_ts()`, update `_recompute_and_verify` signature + SQL, update `_bootstrap_anchors` call |
| `tests/pnl_consumer/test_pnl_consumer.py` | Add tests for `peek_reference_ts`, update `_recompute_and_verify` tests for `reference_ts` param |

---

## Task 1: CloudWatch crash alarm in Terraform

**Files:**
- Modify: `infra/terraform/main.tf` (after the `aws_ecs_service.pnl_consumer` resource, before Outputs)

### Context

Container Insights is already enabled on the cluster (`containerInsights = "enabled"` at line ~265). The metric `StoppedTaskCount` is published under namespace `ECS/ContainerInsights` with dimensions `ClusterName` and `ServiceName`. Service names follow the pattern `trading-analysis-pnl-consumer-<key>` (matching `aws_ecs_service.pnl_consumer[each.key].name`).

The 4 sink keys are: `price`, `prod`, `real-trade`, `bt`.

Alarm spec:
- **Namespace:** `ECS/ContainerInsights`
- **MetricName:** `StoppedTaskCount`
- **Statistic:** `Sum`
- **Period:** `600` (10 minutes)
- **Evaluation periods:** `1`
- **Threshold:** `3`
- **Comparison:** `GreaterThanOrEqualToThreshold`
- **Treat missing data:** `notBreaching` (service may be desired_count=0)
- **No alarm actions** (no SNS)

- [ ] **Step 1: Add the alarm resource to main.tf**

Open `infra/terraform/main.tf`. Find the line:
```
# ─────────────────────────────────────────────────────────────────────────────
# Outputs
```

Insert the following block immediately before that section:

```hcl
# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Alarms — pnl-consumer crash loop detection
# Fires when StoppedTaskCount >= 3 in a 10-minute window for any pnl-consumer
# service. No notification action — visible in CloudWatch console only.
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "pnl_consumer_crash" {
  for_each = local.pnl_consumer_sinks

  alarm_name          = "${local.name_prefix}-pnl-consumer-${each.key}-crash-loop"
  alarm_description   = "pnl-consumer-${each.key} stopped >= 3 times in 10 min (crash loop)"
  namespace           = "ECS/ContainerInsights"
  metric_name         = "StoppedTaskCount"
  dimensions = {
    ClusterName = aws_ecs_cluster.main.name
    ServiceName = aws_ecs_service.pnl_consumer[each.key].name
  }
  statistic           = "Sum"
  period              = 600
  evaluation_periods  = 1
  threshold           = 3
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  tags = local.common_tags
}
```

- [ ] **Step 2: Validate Terraform**

```bash
cd infra/terraform
terraform init -backend=false 2>&1 | tail -5
terraform validate
```

Expected output:
```
Success! The configuration is valid.
```

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "feat: add CloudWatch crash-loop alarm for pnl-consumer services"
```

---

## Task 2: `peek_reference_ts()` — resolve reference timestamp from Redpanda committed offset

**Files:**
- Modify: `pnl_consumer/pnl_consumer.py`
- Test: `tests/pnl_consumer/test_pnl_consumer.py`

### Context

`peek_reference_ts()` creates a short-lived `Consumer` (same broker config, same `group.id`), calls `committed()` to get the offsets this group last committed per partition, seeks to those offsets, polls one message per partition, and returns `min(candle.ts)` across all partitions.

If the group has no committed offsets yet (brand-new group, `offset == OFFSET_INVALID`), fall back to the high-watermark offset for each partition (i.e., the latest message). This handles first-ever startup.

If a partition yields no message within the timeout, skip it — use whatever other partitions return. If **no** partitions return a message, return `None` so the caller falls back to wall-clock `now()`.

The function signature:
```python
def peek_reference_ts(
    brokers: str,
    group_id: str,
    topic: str = TOPIC,
    timeout: float = 5.0,
) -> datetime | None:
```

`OFFSET_INVALID` is available as `confluent_kafka.OFFSET_INVALID` (value `-1001`).

- [ ] **Step 1: Write the failing test**

Add this test to `tests/pnl_consumer/test_pnl_consumer.py` (after the existing `resolve_group_id` tests):

```python
# --- peek_reference_ts tests ---

from pnl_consumer.pnl_consumer import peek_reference_ts
from unittest.mock import call as mock_call

_MOCK_BROKERS = "localhost:9092"
_MOCK_GROUP = "test-group"


@pytest.mark.unit
def test_peek_reference_ts_returns_min_ts_across_partitions():
    """Returns the minimum candle ts from the committed-offset messages."""
    from confluent_kafka import OFFSET_INVALID
    from unittest.mock import MagicMock

    mock_consumer = MagicMock()

    # Two partitions, committed offsets 5 and 10
    tp0 = MagicMock()
    tp0.partition = 0
    tp0.offset = 5
    tp1 = MagicMock()
    tp1.partition = 1
    tp1.offset = 10
    mock_consumer.committed.return_value = [tp0, tp1]

    # get_watermark_offsets not needed (offsets are valid)
    ts0 = datetime(2026, 5, 4, 10, 0, 0)
    ts1 = datetime(2026, 5, 4, 11, 0, 0)

    msg0 = MagicMock()
    msg0.error.return_value = None
    msg0.value.return_value = json.dumps({
        "exchange": "binance", "instrument": "BTCUSDT",
        "ts": ts0.isoformat(), "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0,
    }).encode()

    msg1 = MagicMock()
    msg1.error.return_value = None
    msg1.value.return_value = json.dumps({
        "exchange": "binance", "instrument": "BTCUSDT",
        "ts": ts1.isoformat(), "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0,
    }).encode()

    mock_consumer.poll.side_effect = [msg0, msg1]

    with patch(f"{_MOD}.Consumer", return_value=mock_consumer):
        result = peek_reference_ts(_MOCK_BROKERS, _MOCK_GROUP)

    assert result == ts0  # min of ts0, ts1


@pytest.mark.unit
def test_peek_reference_ts_falls_back_to_high_watermark_when_no_committed_offset():
    """When committed offset is OFFSET_INVALID, use high-watermark offset."""
    from confluent_kafka import OFFSET_INVALID
    from unittest.mock import MagicMock

    mock_consumer = MagicMock()

    tp0 = MagicMock()
    tp0.partition = 0
    tp0.offset = OFFSET_INVALID  # no committed offset
    mock_consumer.committed.return_value = [tp0]
    mock_consumer.get_watermark_offsets.return_value = (0, 42)  # low=0, high=42

    ts0 = datetime(2026, 5, 4, 10, 0, 0)
    msg0 = MagicMock()
    msg0.error.return_value = None
    msg0.value.return_value = json.dumps({
        "exchange": "binance", "instrument": "BTCUSDT",
        "ts": ts0.isoformat(), "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0,
    }).encode()
    mock_consumer.poll.return_value = msg0

    with patch(f"{_MOD}.Consumer", return_value=mock_consumer):
        result = peek_reference_ts(_MOCK_BROKERS, _MOCK_GROUP)

    assert result == ts0
    # Should have seeked to high watermark - 1 = 41
    mock_consumer.seek.assert_called_once()
    seek_tp = mock_consumer.seek.call_args[0][0]
    assert seek_tp.offset == 41


@pytest.mark.unit
def test_peek_reference_ts_returns_none_when_no_messages():
    """Returns None when poll yields no messages (e.g. empty topic)."""
    from unittest.mock import MagicMock

    mock_consumer = MagicMock()
    tp0 = MagicMock()
    tp0.partition = 0
    tp0.offset = 5
    mock_consumer.committed.return_value = [tp0]
    mock_consumer.poll.return_value = None  # timeout, no message

    with patch(f"{_MOD}.Consumer", return_value=mock_consumer):
        result = peek_reference_ts(_MOCK_BROKERS, _MOCK_GROUP)

    assert result is None
```

- [ ] **Step 2: Run the failing test**

```bash
source .venv/bin/activate
pytest tests/pnl_consumer/test_pnl_consumer.py::test_peek_reference_ts_returns_min_ts_across_partitions -v
```

Expected: `FAILED` with `ImportError: cannot import name 'peek_reference_ts'`

- [ ] **Step 3: Implement `peek_reference_ts` in pnl_consumer.py**

Add the following import at the top of `pnl_consumer/pnl_consumer.py` alongside the existing confluent_kafka import:

```python
from confluent_kafka import Consumer, KafkaError, KafkaException, OFFSET_INVALID, TopicPartition
```

Then add this function after the `resolve_group_id` function (around line 61), before `PRICE_COLUMNS`:

```python
def peek_reference_ts(
    brokers: str,
    group_id: str,
    topic: str = TOPIC,
    timeout: float = 5.0,
) -> "datetime | None":
    """Return the min candle ts at this group's committed offsets across all partitions.

    Used on cold start to anchor the ClickHouse verification window to where Kafka
    will actually replay from, rather than wall-clock now(). Falls back to the
    high-watermark offset when no committed offset exists (first-ever start).
    Returns None if no messages can be read (empty topic or timeout).
    """
    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": group_id,
        "enable.auto.commit": False,
    })
    try:
        meta = consumer.list_topics(topic, timeout=timeout)
        if topic not in meta.topics:
            logger.warning("peek_reference_ts: topic %s not found", topic)
            return None
        partitions = [
            TopicPartition(topic, p)
            for p in meta.topics[topic].partitions
        ]
        committed = consumer.committed(partitions, timeout=timeout)

        timestamps: list[datetime] = []
        for tp in committed:
            offset = tp.offset
            if offset == OFFSET_INVALID or offset <= 0:
                # No committed offset — fall back to latest message.
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, tp.partition), timeout=timeout
                )
                offset = max(high - 1, low)
                if offset < 0:
                    continue  # empty partition
            seek_tp = TopicPartition(topic, tp.partition, offset)
            consumer.assign([seek_tp])
            msg = consumer.poll(timeout=timeout)
            if msg is None or msg.error():
                continue
            raw = msg.value()
            data = json.loads(raw.decode() if raw is not None else "{}")
            ts = datetime.fromisoformat(data["ts"])
            timestamps.append(ts)

        return min(timestamps) if timestamps else None
    except Exception:
        logger.warning("peek_reference_ts failed — falling back to now()", exc_info=True)
        return None
    finally:
        consumer.close()
```

- [ ] **Step 4: Run the tests**

```bash
pytest tests/pnl_consumer/test_pnl_consumer.py::test_peek_reference_ts_returns_min_ts_across_partitions tests/pnl_consumer/test_pnl_consumer.py::test_peek_reference_ts_falls_back_to_high_watermark_when_no_committed_offset tests/pnl_consumer/test_pnl_consumer.py::test_peek_reference_ts_returns_none_when_no_messages -v
```

Expected: all 3 `PASSED`

- [ ] **Step 5: Commit**

```bash
git add pnl_consumer/pnl_consumer.py tests/pnl_consumer/test_pnl_consumer.py
git commit -m "feat: add peek_reference_ts to resolve cold-start window from Kafka committed offset"
```

---

## Task 3: Pass `reference_ts` into `_recompute_and_verify` and `_bootstrap_anchors`

**Files:**
- Modify: `pnl_consumer/pnl_consumer.py`
- Test: `tests/pnl_consumer/test_pnl_consumer.py`

### Context

`_recompute_and_verify` currently uses `now()` in both SQL queries. The fix replaces `now()` with a `reference_ts` parameter (a Python `datetime`). When `reference_ts` is `None`, the function falls back to `now()` exactly as before — this keeps all existing unit tests passing without modification.

`_bootstrap_anchors` gains a `reference_ts: datetime | None = None` parameter and passes it through. `run()` calls `peek_reference_ts` before calling `_bootstrap_anchors` and passes the result.

- [ ] **Step 1: Write the failing tests**

Add to `tests/pnl_consumer/test_pnl_consumer.py` (after the `peek_reference_ts` tests):

```python
@pytest.mark.unit
def test_recompute_and_verify_uses_reference_ts_in_sql():
    """When reference_ts is provided, SQL uses it instead of now()."""
    ref_ts = datetime(2026, 5, 1, 12, 0, 0)
    captured_sqls = []

    def mock_query(sql):
        captured_sqls.append(sql)
        return []

    with patch("pnl_consumer.pnl_consumer.query_dicts", side_effect=mock_query):
        _recompute_and_verify("analytics.strategy_pnl_1min_prod_v2", AnchorState(), reference_ts=ref_ts)

    assert len(captured_sqls) == 2
    # Seed query: ts < reference_ts - 3 days
    assert "2026-04-28" in captured_sqls[0]   # ref_ts - 3d = 2026-04-28 12:00:00
    assert "< now()" not in captured_sqls[0]
    # Window query: ts >= reference_ts - 3 days AND ts <= reference_ts
    assert "2026-04-28" in captured_sqls[1]
    assert "2026-05-01" in captured_sqls[1]
    assert ">= now()" not in captured_sqls[1]


@pytest.mark.unit
def test_recompute_and_verify_falls_back_to_now_when_no_reference_ts():
    """When reference_ts is None, SQL uses now() as before."""
    captured_sqls = []

    def mock_query(sql):
        captured_sqls.append(sql)
        return []

    with patch("pnl_consumer.pnl_consumer.query_dicts", side_effect=mock_query):
        _recompute_and_verify("analytics.strategy_pnl_1min_prod_v2", AnchorState(), reference_ts=None)

    assert "now()" in captured_sqls[0]
    assert "now()" in captured_sqls[1]
```

- [ ] **Step 2: Run the failing tests**

```bash
pytest tests/pnl_consumer/test_pnl_consumer.py::test_recompute_and_verify_uses_reference_ts_in_sql tests/pnl_consumer/test_pnl_consumer.py::test_recompute_and_verify_falls_back_to_now_when_no_reference_ts -v
```

Expected: `FAILED` — `_recompute_and_verify` does not accept `reference_ts` keyword argument yet.

- [ ] **Step 3: Update `_recompute_and_verify` in pnl_consumer.py**

Find the `_recompute_and_verify` function signature and update it:

```python
def _recompute_and_verify(
    table: str,
    state: AnchorState,
    reference_ts: "datetime | None" = None,
) -> int:
```

Then replace the two SQL string definitions inside the function. Replace:

```python
    seed_sql = f"""\
SELECT
    strategy_table_name,
    cumulative_pnl  AS anchor_pnl,
    price           AS anchor_price,
    position        AS anchor_position,
    ts              AS anchor_ts
FROM {table} FINAL
WHERE ts < now() - INTERVAL {_COLD_START_DAYS} DAY
ORDER BY strategy_table_name, ts DESC, updated_at DESC
LIMIT 1 BY strategy_table_name
"""
```

With:

```python
    if reference_ts is not None:
        window_start = reference_ts - timedelta(days=_COLD_START_DAYS)
        window_start_str = window_start.strftime("%Y-%m-%d %H:%M:%S")
        window_end_str = reference_ts.strftime("%Y-%m-%d %H:%M:%S")
        seed_clause = f"ts < '{window_start_str}'"
        window_clause = f"ts >= '{window_start_str}' AND ts <= '{window_end_str}'"
    else:
        seed_clause = f"ts < now() - INTERVAL {_COLD_START_DAYS} DAY"
        window_clause = f"ts >= now() - INTERVAL {_COLD_START_DAYS} DAY"

    seed_sql = f"""\
SELECT
    strategy_table_name,
    cumulative_pnl  AS anchor_pnl,
    price           AS anchor_price,
    position        AS anchor_position,
    ts              AS anchor_ts
FROM {table} FINAL
WHERE {seed_clause}
ORDER BY strategy_table_name, ts DESC, updated_at DESC
LIMIT 1 BY strategy_table_name
"""
```

And replace:

```python
    window_sql = f"""\
SELECT
    strategy_table_name,
    ts,
    cumulative_pnl,
    price,
    position
FROM {table} FINAL
WHERE ts >= now() - INTERVAL {_COLD_START_DAYS} DAY
ORDER BY strategy_table_name, ts ASC, updated_at DESC
"""
```

With:

```python
    window_sql = f"""\
SELECT
    strategy_table_name,
    ts,
    cumulative_pnl,
    price,
    position
FROM {table} FINAL
WHERE {window_clause}
ORDER BY strategy_table_name, ts ASC, updated_at DESC
"""
```

- [ ] **Step 4: Update `_bootstrap_anchors` signature and call sites**

Find `_bootstrap_anchors` and update its signature:

```python
def _bootstrap_anchors(
    state_prod: AnchorState,
    state_real_trade: AnchorState,
    state_bt: AnchorState,
    cfg: SinkConfig | None = None,
    reference_ts: "datetime | None" = None,
) -> None:
```

Update the loop inside to pass `reference_ts` through:

```python
    for table, state, enabled in candidates:
        if not enabled:
            continue
        _recompute_and_verify(table, state, reference_ts=reference_ts)
```

Update the docstring to mention `reference_ts`:

```python
    """On cold start, seed anchor states from ClickHouse for enabled PnL sinks.

    Fetches the last _COLD_START_DAYS of stored PnL rows relative to reference_ts
    (defaults to now() when None), re-walks the anchor chain, and crashes if any
    stored value deviates from the recomputed value — ensuring the consumer never
    silently diverges from a manual backfill.
    """
```

- [ ] **Step 5: Update `run()` to call `peek_reference_ts` before bootstrap**

Find the `run()` function. After `sink_cfg = SinkConfig.from_env()` and its log line, and before the first `_bootstrap_anchors` call, add:

```python
    reference_ts = peek_reference_ts(
        os.environ["REDPANDA_BROKERS"],
        resolve_group_id(),
    )
    if reference_ts is not None:
        logger.info("Cold-start reference_ts from Redpanda committed offset: %s", reference_ts)
    else:
        logger.info("Cold-start reference_ts: no committed offset found, using now()")
```

Then update the `_bootstrap_anchors` call in `run()` from:

```python
    _bootstrap_anchors(state_prod, state_real_trade, state_bt, sink_cfg)
```

To:

```python
    _bootstrap_anchors(state_prod, state_real_trade, state_bt, sink_cfg, reference_ts)
```

Note: The second `_bootstrap_anchors` call (inside `_flush_and_reseed`) does **not** get `reference_ts` — reseeds during live operation should always use `now()` since the consumer is running in real time.

- [ ] **Step 6: Run all tests**

```bash
pytest tests/pnl_consumer/ -m unit -v
```

Expected: all tests pass. Specifically verify:
- `test_recompute_and_verify_uses_reference_ts_in_sql` PASSED
- `test_recompute_and_verify_falls_back_to_now_when_no_reference_ts` PASSED
- All previously passing tests still PASSED

- [ ] **Step 7: Commit**

```bash
git add pnl_consumer/pnl_consumer.py tests/pnl_consumer/test_pnl_consumer.py
git commit -m "feat: use Redpanda committed offset timestamp as cold-start verification window anchor"
```

---

## Self-Review

**Spec coverage:**
- ✅ CloudWatch alarm: `≥ 3 StoppedTaskCount` in 10-min per pnl-consumer service → Task 1
- ✅ No notifications → `alarm_actions = []` (omitted, which is the Terraform default)
- ✅ Cold-start uses committed offset timestamp → Task 2 + 3
- ✅ Falls back to `now()` when no committed offset → `peek_reference_ts` returns `None`, `_recompute_and_verify` uses `now()` branch
- ✅ `_flush_and_reseed` reseeds continue to use `now()` (live operation) → Task 3 Step 5 explicitly noted

**Placeholder scan:** None found.

**Type consistency:**
- `peek_reference_ts` returns `datetime | None` — matches parameter type in `_bootstrap_anchors(reference_ts: datetime | None = None)` ✅
- `_recompute_and_verify(table, state, reference_ts=None)` — matches all call sites ✅
- `TopicPartition` imported alongside `OFFSET_INVALID` in same import line ✅
