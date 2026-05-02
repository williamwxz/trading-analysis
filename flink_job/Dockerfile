# flink_job/Dockerfile
FROM apache/flink:1.19.0-scala_2.12-java11

# Install Python 3.11 and PyFlink deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 python3.11-dev python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf python3.11 /usr/bin/python3 && ln -sf python3.11 /usr/bin/python

WORKDIR /app

COPY flink_job/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy connector JARs into Flink lib directory
COPY flink_job/connectors/*.jar /opt/flink/lib/

# Copy application code
COPY streaming/ ./streaming/
COPY flink_job/ ./flink_job/
COPY trading_dagster/utils/clickhouse_client.py ./trading_dagster/utils/clickhouse_client.py
COPY trading_dagster/utils/__init__.py ./trading_dagster/utils/__init__.py
COPY trading_dagster/utils/pnl_compute.py ./trading_dagster/utils/pnl_compute.py
# Stub __init__.py — the real one imports Dagster definitions not present in this image
RUN echo "" > ./trading_dagster/__init__.py

ENV PYTHONUNBUFFERED=1
ENV PYFLINK_PYTHON=python3
ENV FLINK_CONF_DIR=/opt/flink/conf
# Point Flink's log4j config to the console variant so JVM logs go to stdout
RUN cp /opt/flink/conf/log4j-console.properties /opt/flink/conf/log4j.properties

CMD ["python", "-m", "flink_job.pnl_stream_job"]
