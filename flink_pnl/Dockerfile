# flink_pnl/Dockerfile
# The official apache/flink image no longer ships a python3 variant after 1.x.
# Install Python + PyFlink pip package on top of the Java image.
# IMPORTANT: apache/flink image version and apache-flink PyPI version must match exactly.
# Update both together when upgrading — a mismatch fails job startup with a version error.
FROM apache/flink:2.2.1-java21

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        wget \
    && wget -q -O /opt/flink/lib/flink-sql-connector-kafka.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar \
    && mkdir -p /opt/flink/plugins/s3-fs-presto \
    && cp /opt/flink/opt/flink-s3-fs-presto-*.jar /opt/flink/plugins/s3-fs-presto/ \
    && rm -rf /var/lib/apt/lists/*
USER flink

COPY flink_pnl/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /tmp/requirements.txt

WORKDIR /opt/flink/usrlib

COPY libs/ ./libs/
COPY streaming/ ./streaming/
COPY pnl_consumer/ ./pnl_consumer/
COPY flink_pnl/flink_pnl/ ./flink_pnl/

ENV PYTHONPATH=/opt/flink/usrlib
ENV PYTHONUNBUFFERED=1

# PyFlink embedded mode writes to stdout — no /opt/flink/log/ black hole.
CMD ["python3", "-m", "flink_pnl.entrypoint"]
