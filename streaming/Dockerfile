# streaming/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system deps for confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir websockets>=12.0 confluent-kafka>=2.4

COPY streaming/ ./streaming/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "streaming.binance_ws_consumer"]
