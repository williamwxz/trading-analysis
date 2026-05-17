"""Flink job entrypoint: StreamExecutionEnvironment setup + job graph registration."""

import logging
import os

from pyflink.common.configuration import Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from flink_pnl.pnl_job import PnlProcessFunction

logger = logging.getLogger(__name__)

TOPIC = "binance.price.ticks"
CHECKPOINT_INTERVAL_MS = 60_000
CHECKPOINT_URI = "s3p://trading-analysis-flink-checkpoints-068704208855/checkpoints/"


def build_env() -> StreamExecutionEnvironment:
    config = Configuration()
    config.set_string("execution.checkpointing.storage", "filesystem")
    config.set_string("execution.checkpointing.dir", CHECKPOINT_URI)

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    # Checkpoint every 60s to S3.
    # At-least-once + idempotent CH upsert = effectively exactly-once.
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.AT_LEAST_ONCE)

    return env


def build_kafka_source() -> KafkaSource:
    brokers = os.environ["REDPANDA_BROKERS"]
    group_id = os.environ.get("KAFKA_GROUP_ID", "flink-pnl-consumer-v2")

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(brokers)
        .set_topics(TOPIC)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    env = build_env()
    source = build_kafka_source()
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    stream.process(PnlProcessFunction())
    env.execute("flink-pnl-job")


if __name__ == "__main__":
    main()
