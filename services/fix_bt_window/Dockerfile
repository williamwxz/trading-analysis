# services/fix_bt_window/Dockerfile
# AWS Lambda container image, deployed as three scheduled functions (bt daily,
# prod + real_trade 6-hourly) selected via AUDIT_TYPE/CONSUMER_SERVICE env; also
# invokable ad-hoc with {"window_start": "...", "window_end": "...", "dry_run": true}.
# Repairs the trailing PnL window (audit_pnl --type <mode> --fix-window) once
# late-arriving source rows land, pausing/resuming that mode's consumer around
# the rewrite. Functions are VPC-attached (ClickHouse Cloud only allows the NAT
# EIP; see infra/terraform/fix_bt_window.tf).
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
