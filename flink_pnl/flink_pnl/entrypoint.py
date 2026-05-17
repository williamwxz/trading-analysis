"""Flink job entrypoint: StreamExecutionEnvironment setup + job graph registration."""

import logging
import os

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer

from flink_pnl.pnl_job import PnlProcessFunction

logger = logging.getLogger(__name__)

TOPIC = "binance.price.ticks"
CHECKPOINT_INTERVAL_MS = 60_000
CHECKPOINT_URI = "s3://trading-analysis-flink-checkpoints-068704208855/checkpoints/"


def build_env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Checkpoint every 60s to S3.
    # At-least-once + idempotent CH upsert = effectively exactly-once.
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.AT_LEAST_ONCE)
    env.get_checkpoint_config().set_checkpoint_storage(FileSystemCheckpointStorage(CHECKPOINT_URI))

    return env


def build_kafka_source(env: StreamExecutionEnvironment) -> FlinkKafkaConsumer:
    brokers = os.environ["REDPANDA_BROKERS"]
    group_id = os.environ.get("KAFKA_GROUP_ID", "flink-pnl-consumer-v2")

    props = {
        "bootstrap.servers": brokers,
        "group.id": group_id,
        "auto.offset.reset": "latest",
    }

    source = FlinkKafkaConsumer(
        topics=TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=props,
    )
    return source


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    env = build_env()
    source = build_kafka_source(env)
    stream = env.add_source(source)
    stream.process(PnlProcessFunction())
    env.execute("flink-pnl-job")


if __name__ == "__main__":
    main()
