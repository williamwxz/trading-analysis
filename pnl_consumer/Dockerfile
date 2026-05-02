# pnl_consumer/Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY pnl_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming/ ./streaming/
COPY pnl_consumer/ ./pnl_consumer/
COPY trading_dagster/utils/clickhouse_client.py ./trading_dagster/utils/clickhouse_client.py
COPY trading_dagster/utils/__init__.py ./trading_dagster/utils/__init__.py
COPY trading_dagster/utils/pnl_compute.py ./trading_dagster/utils/pnl_compute.py
RUN echo "" > ./trading_dagster/__init__.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "pnl_consumer.pnl_consumer"]
