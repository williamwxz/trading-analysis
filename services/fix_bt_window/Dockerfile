# services/fix_bt_window/Dockerfile
# AWS Lambda container image. Triggered daily by EventBridge Rule; also
# invokable ad-hoc with {"window_start": "...", "window_end": "...", "dry_run": true}.
# Repairs the trailing bt PnL window (audit_pnl --type bt --fix-window) after the
# late-publishing strategy_cum_pnl_bt_v2 batches land, pausing/resuming the bt
# consumer around the rewrite. No VPC attachment needed: talks only to
# ClickHouse Cloud (public HTTPS) and AWS APIs.
FROM public.ecr.aws/lambda/python:3.11

# All deps are pure-Python; no system libs needed. python-dotenv is imported
# unconditionally by scripts/audit_pnl.py (no-op without a .env file).
RUN pip install --no-cache-dir \
    clickhouse-connect>=0.7 \
    boto3>=1.34 \
    python-dotenv>=1.0

# Lambda task root is /var/task.
COPY services/fix_bt_window/fix_bt_window/ ${LAMBDA_TASK_ROOT}/fix_bt_window/
# The repair logic is scripts/audit_pnl.py invoked in-process (handler patches
# sys.argv and calls audit_pnl.main()) — same CLI contract as a manual run.
COPY scripts/audit_pnl.py ${LAMBDA_TASK_ROOT}/audit_pnl.py
# audit_pnl imports libs.clickhouse_client + libs.computation.*.
COPY libs/__init__.py libs/clickhouse_client.py ${LAMBDA_TASK_ROOT}/libs/
COPY libs/computation/ ${LAMBDA_TASK_ROOT}/libs/computation/

# Entry: module-path to the handler function.
CMD ["fix_bt_window.handler.handler"]
