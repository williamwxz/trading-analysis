# flink_pnl/Dockerfile
# The official apache/flink image no longer ships a python3 variant after 1.x.
# Install Python + PyFlink pip package on top of the Java image.
# IMPORTANT: apache/flink image version and apache-flink PyPI version must match exactly.
# Update both together when upgrading — a mismatch fails job startup with a version error.
FROM apache/flink:2.2.1-java21

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY flink_pnl/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/flink/usrlib

COPY libs/ ./libs/
COPY streaming/ ./streaming/
COPY pnl_consumer/ ./pnl_consumer/
COPY flink_pnl/flink_pnl/ ./flink_pnl/

ENV PYTHONPATH=/opt/flink/usrlib
ENV PYTHONUNBUFFERED=1

# PyFlink embedded mode writes to stdout — no /opt/flink/log/ black hole.
CMD ["python3", "-m", "flink_pnl.entrypoint"]
