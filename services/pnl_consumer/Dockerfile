# pnl_consumer/Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY services/pnl_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/streaming/streaming/ ./streaming/
COPY services/pnl_consumer/pnl_consumer/ ./pnl_consumer/
COPY libs/ ./libs/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "pnl_consumer.pnl_consumer"]
