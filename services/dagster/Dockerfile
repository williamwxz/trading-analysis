# ============================================================================
# trading-analysis Dagster — Multi-stage production build
# ============================================================================

# Use ECR Public to avoid Docker Hub rate limits (429 Too Many Requests)
FROM public.ecr.aws/docker/library/python:3.11-slim AS builder

WORKDIR /build
COPY pyproject.toml .
COPY services/dagster/trading_dagster/ trading_dagster/
COPY libs/ libs/

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir .


FROM public.ecr.aws/docker/library/python:3.11-slim AS runtime

WORKDIR /app

# Copy virtualenv from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/app

# Copy application code and config
COPY services/dagster/trading_dagster/ trading_dagster/
COPY libs/ libs/
COPY services/dagster/dagster.yaml dagster.yaml
COPY workspace.yaml .
# pyproject.toml provides [tool.dagster] module_name for dagster-daemon auto-discovery
COPY pyproject.toml .

# Non-root user
RUN useradd -m -s /bin/bash dagster && chown -R dagster:dagster /app
USER dagster

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:3000/server_info')" || exit 1

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
