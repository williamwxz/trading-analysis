"""Unit tests for crash-to-recover wedge detection (incident 2026-07-13).

The Redpanda broker task lives on Fargate Spot with ephemeral storage: every
task replacement is a brand-new cluster (new ClusterId, new topic ids).
librdkafka only WARNs (CLUSTERID / PARTCNT -> 0) and stops fetching — poll()
returns None forever and the consumer wedges silently. These tests pin the
detection of that condition so the process exits and ECS restarts it fresh.

Log lines in these tests are verbatim from the 2026-07-13 incident streams.
confluent-kafka forwards librdkafka logs to the configured Python logger as
``logger.log(level, "%s [%s] %s", facility, client_name, message)``.
"""

import logging

import pytest
from pnl_consumer.pnl_consumer import (
    ConsumerWedgedError,
    PollSilenceWatchdog,
    RdkafkaWedgeDetector,
    _raise_if_wedged,
)

pytestmark = pytest.mark.unit

_CLUSTERID_MSG = (
    "Broker GroupCoordinator/0 reports different ClusterId "
    '"redpanda.f47cc280-10be-4ea0-ad79-1862d8e8e05e" than previously known '
    '"redpanda.b0a8f342-327a-4cb2-a08b-81a1e4543abd": a client must not be '
    "simultaneously connected to multiple clusters"
)
_PARTCNT_ZERO_MSG = (
    "Topic binance.price.ticks (id yZXfSqlQR0Wjb5xAuseVjQ) "
    "partition count changed from 1 to 0"
)


def _detector_logger(name: str) -> tuple[logging.Logger, RdkafkaWedgeDetector]:
    lg = logging.getLogger(name)
    lg.propagate = False
    detector = RdkafkaWedgeDetector()
    lg.addHandler(detector)
    return lg, detector


def _rdkafka_log(lg: logging.Logger, facility: str, message: str) -> None:
    """Emit exactly like confluent-kafka's log forwarding does."""
    lg.log(logging.WARNING, "%s [%s] %s", facility, "rdkafka#consumer-2", message)


class TestRdkafkaWedgeDetector:
    def test_clusterid_change_sets_reason(self):
        lg, detector = _detector_logger("t_clusterid")
        _rdkafka_log(lg, "CLUSTERID", _CLUSTERID_MSG)
        assert detector.reason is not None
        assert "different ClusterId" in detector.reason

    def test_partition_count_drop_to_zero_sets_reason(self):
        lg, detector = _detector_logger("t_partcnt0")
        _rdkafka_log(lg, "PARTCNT", _PARTCNT_ZERO_MSG)
        assert detector.reason is not None
        assert "partition count changed from 1 to 0" in detector.reason

    def test_partition_count_increase_is_ignored(self):
        lg, detector = _detector_logger("t_partcnt_up")
        _rdkafka_log(
            lg,
            "PARTCNT",
            "Topic binance.price.ticks (id abc) partition count changed from 1 to 3",
        )
        assert detector.reason is None

    def test_benign_broker_failures_are_ignored(self):
        # Transient FAIL/SESSTMOUT lines happen on every broker restart and
        # network blip; librdkafka recovers those on its own — no crash.
        lg, detector = _detector_logger("t_benign")
        _rdkafka_log(
            lg,
            "FAIL",
            "redpanda.trading-analysis.local:9092/0: Disconnected: connection "
            "closed by peer: receive 0 after POLLIN (after 13314796ms in state UP)",
        )
        _rdkafka_log(
            lg,
            "FAIL",
            "redpanda.trading-analysis.local:9092/bootstrap: Failed to resolve "
            "'redpanda.trading-analysis.local:9092': Name or service not known "
            "(after 1ms in state CONNECT)",
        )
        _rdkafka_log(
            lg,
            "SESSTMOUT",
            "Consumer group session timed out (in join-state steady) after "
            "45018 ms without a successful response from the group coordinator "
            "(broker 0, last error was Success): revoking assignment and "
            "rejoining group",
        )
        assert detector.reason is None

    def test_first_reason_is_kept(self):
        lg, detector = _detector_logger("t_first")
        _rdkafka_log(lg, "PARTCNT", _PARTCNT_ZERO_MSG)
        _rdkafka_log(lg, "CLUSTERID", _CLUSTERID_MSG)
        assert "partition count" in detector.reason

    def test_plain_string_record_still_detected(self):
        # Fallback if the binding ever changes its "%s [%s] %s" forwarding shape.
        lg, detector = _detector_logger("t_plain")
        lg.warning("CLUSTERID [rdkafka#consumer-2] " + _CLUSTERID_MSG)
        assert detector.reason is not None


class TestPollSilenceWatchdog:
    def test_quiet_within_limit_is_fine(self):
        clock = [0.0]
        wd = PollSilenceWatchdog(600, now=lambda: clock[0])
        clock[0] = 599.0
        assert wd.check() is None

    def test_silence_beyond_limit_triggers(self):
        clock = [0.0]
        wd = PollSilenceWatchdog(600, now=lambda: clock[0])
        clock[0] = 601.0
        reason = wd.check()
        assert reason is not None
        assert "601" in reason

    def test_activity_resets_the_clock(self):
        clock = [0.0]
        wd = PollSilenceWatchdog(600, now=lambda: clock[0])
        clock[0] = 599.0
        wd.record_activity()
        clock[0] = 1150.0
        assert wd.check() is None
        clock[0] = 1200.0
        assert wd.check() is not None

    def test_zero_limit_disables_watchdog(self):
        clock = [0.0]
        wd = PollSilenceWatchdog(0, now=lambda: clock[0])
        clock[0] = 1e9
        assert wd.check() is None


class TestRaiseIfWedged:
    def test_noop_when_healthy(self):
        _, detector = _detector_logger("t_healthy")
        wd = PollSilenceWatchdog(600, now=lambda: 0.0)
        _raise_if_wedged(detector, wd)  # must not raise

    def test_raises_on_detector_reason(self):
        lg, detector = _detector_logger("t_raise_det")
        wd = PollSilenceWatchdog(600, now=lambda: 0.0)
        _rdkafka_log(lg, "CLUSTERID", _CLUSTERID_MSG)
        with pytest.raises(ConsumerWedgedError, match="different ClusterId"):
            _raise_if_wedged(detector, wd)

    def test_raises_on_watchdog_silence(self):
        _, detector = _detector_logger("t_raise_wd")
        clock = [0.0]
        wd = PollSilenceWatchdog(600, now=lambda: clock[0])
        clock[0] = 1800.0
        with pytest.raises(ConsumerWedgedError, match="no Kafka events"):
            _raise_if_wedged(detector, wd)
