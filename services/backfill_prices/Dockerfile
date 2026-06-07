# services/backfill_prices/Dockerfile
# AWS Lambda container image. Triggered daily by EventBridge Rule; also
# invokable ad-hoc with {"window_start": "...", "window_end": "..."} payload.
# VPC-attached so it egresses through the fck-nat EIP (same outbound IP as
# the streaming consumer — Binance Futures geo-blocks US IPs, ap-northeast-1
# only).
FROM public.ecr.aws/lambda/python:3.11

# All deps are pure-Python; no system libs needed.
RUN pip install --no-cache-dir \
    ccxt>=4.5 \
    clickhouse-connect>=0.7 \
    boto3>=1.34

# Lambda task root is /var/task.
COPY services/backfill_prices/backfill_prices/ ${LAMBDA_TASK_ROOT}/backfill_prices/
# libs/clickhouse_client.get_client() is the shared CH factory; copy only the
# two files we actually import (skip computation/, tests/, etc.).
COPY libs/__init__.py libs/clickhouse_client.py ${LAMBDA_TASK_ROOT}/libs/

# Entry: module-path to the handler function.
CMD ["backfill_prices.handler.handler"]
