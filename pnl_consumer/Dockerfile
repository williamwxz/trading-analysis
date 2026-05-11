# pnl_consumer/Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY pnl_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming/ ./streaming/
COPY pnl_consumer/ ./pnl_consumer/
COPY libs/ ./libs/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "pnl_consumer.pnl_consumer"]
