# services/backfill_prices/Dockerfile
# One-shot ECS Fargate task: scans futures_price_1min for gaps in the last N
# hours and backfills missing minutes from Binance Futures. Triggered daily by
# EventBridge Schedule. Must run in ap-northeast-1 (Tokyo) — Binance Futures
# geo-blocks US IPs.
FROM python:3.11-slim

WORKDIR /app

# No system deps beyond what pip wheels provide (ccxt + clickhouse-connect
# are pure-python; no librdkafka or libpq needed).
RUN pip install --no-cache-dir \
    ccxt>=4.5 \
    clickhouse-connect>=0.7

COPY services/backfill_prices/backfill_prices/ ./backfill_prices/

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "backfill_prices"]
