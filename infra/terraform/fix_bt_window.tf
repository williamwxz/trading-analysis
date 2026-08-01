# Scheduled trailing-window PnL repairs — image-package Lambdas + EventBridge
# Rules, one function per mode (bt / prod / real-trade), all built from the
# same image (services/fix_bt_window). VPC-attached so they egress through the
# fck-nat EIP: ClickHouse Cloud only accepts connections from allowlisted IPs
# (the NAT EIP) — a non-VPC Lambda's public egress IPs are rejected at the TLS
# layer (verified on first deploy).
#
# Why: live-written PnL rows go stale when the position source arrives late.
#   bt:   strategy_cum_pnl_bt_v2 is published in batches hours-to-days after
#         bar time (p50 ~14h measured 2026-07-19) → daily repair over 49h.
#   prod / real_trade: first revisions land p50 ~16m / p99 ~96m past bar
#         close and live-written rows were never repaired (0.68% of the
#         bt−prod weighted-position divergence, the dominant term) →
#         6-hourly repair over a 7h window (window > cadence so runs overlap;
#         every minute's final repair happens at age ≥6h, past the p99 lag).
# Once the bars land, history is fully reconstructible — each function runs
# the standard repair (audit_pnl --type <mode> --fix-window), pausing its
# consumer so the post-resume bootstrap re-seeds from the repaired tail.
# See services/fix_bt_window/fix_bt_window/handler.py for the event schema.

locals {
  fix_window_modes = {
    # Keys double as the ECS service key (aws_ecs_service.pnl_consumer[...])
    # and the resource-name suffix: ${name_prefix}-fix-${key}-window.
    bt = {
      audit_type     = "bt"
      lookback_hours = 49                   # 48h coverage + 1h overlap
      schedule       = "cron(0 15 * * ? *)" # daily, after overnight cum batches
      memory_size    = 2048                 # 49h × 695 strategies
      arrival_driven = false
      description    = "Daily bt trailing-window PnL repair (cum-table publish lag retro-correction)"
    }
    prod = {
      audit_type     = "prod"
      lookback_hours = 7                      # cadence 6h + 1h overlap; > p99 arrival lag
      schedule       = "cron(10 3/6 * * ? *)" # 03:10/09:10/15:10/21:10 UTC
      memory_size    = 1024
      arrival_driven = true
      description    = "6-hourly prod trailing-window PnL repair (late first-revision retro-correction)"
    }
    real-trade = {
      audit_type     = "real_trade"
      lookback_hours = 7
      schedule       = "cron(40 3/6 * * ? *)" # offset from prod to avoid concurrent CH load
      memory_size    = 1024
      arrival_driven = true
      description    = "6-hourly real_trade trailing-window PnL repair (late revision retro-correction)"
    }
  }
}

# ── State moves from the pre-parameterization single-function layout ────────
# Same resource names/config for bt — moves only, no destroy/recreate.

moved {
  from = aws_cloudwatch_log_group.fix_bt_window
  to   = aws_cloudwatch_log_group.fix_window["bt"]
}
moved {
  from = aws_iam_role.fix_bt_window_lambda
  to   = aws_iam_role.fix_window_lambda["bt"]
}
moved {
  from = aws_iam_role_policy_attachment.fix_bt_window_lambda_basic
  to   = aws_iam_role_policy_attachment.fix_window_lambda_basic["bt"]
}
moved {
  from = aws_iam_role_policy_attachment.fix_bt_window_lambda_vpc
  to   = aws_iam_role_policy_attachment.fix_window_lambda_vpc["bt"]
}
moved {
  from = aws_iam_role_policy.fix_bt_window_lambda_secrets
  to   = aws_iam_role_policy.fix_window_lambda_secrets["bt"]
}
moved {
  from = aws_iam_role_policy.fix_bt_window_lambda_ecs
  to   = aws_iam_role_policy.fix_window_lambda_ecs["bt"]
}
moved {
  from = aws_lambda_function.fix_bt_window
  to   = aws_lambda_function.fix_window["bt"]
}
moved {
  from = aws_lambda_function_event_invoke_config.fix_bt_window
  to   = aws_lambda_function_event_invoke_config.fix_window["bt"]
}
moved {
  from = aws_cloudwatch_event_rule.fix_bt_window_daily
  to   = aws_cloudwatch_event_rule.fix_window["bt"]
}
moved {
  from = aws_cloudwatch_event_target.fix_bt_window_daily
  to   = aws_cloudwatch_event_target.fix_window["bt"]
}
moved {
  from = aws_lambda_permission.fix_bt_window_allow_events
  to   = aws_lambda_permission.fix_window_allow_events["bt"]
}

# ECR repo owned by the build-fix-bt-window CI job (see ci-cd.yml). One image
# serves all modes; AUDIT_TYPE / CONSUMER_SERVICE env select the behavior.
data "aws_ecr_repository" "fix_bt_window" {
  name = "trading-analysis-fix-bt-window"
}

resource "aws_cloudwatch_log_group" "fix_window" {
  for_each = local.fix_window_modes
  # Lambda's auto-created log group naming convention.
  name              = "/aws/lambda/${local.name_prefix}-fix-${each.key}-window"
  retention_in_days = 14
  tags              = local.common_tags
}

# ── Lambda execution roles (one per mode; ECS scope differs) ────────────────

resource "aws_iam_role" "fix_window_lambda" {
  for_each = local.fix_window_modes
  name     = "${local.name_prefix}-fix-${each.key}-window-lambda"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "fix_window_lambda_basic" {
  for_each   = local.fix_window_modes
  role       = aws_iam_role.fix_window_lambda[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# VPC ENI management (CreateNetworkInterface / DescribeNetworkInterfaces /
# DeleteNetworkInterface) — required for vpc_config.
resource "aws_iam_role_policy_attachment" "fix_window_lambda_vpc" {
  for_each   = local.fix_window_modes
  role       = aws_iam_role.fix_window_lambda[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# Read the ClickHouse secret bundle (handler fetches via boto3 on cold start).
resource "aws_iam_role_policy" "fix_window_lambda_secrets" {
  for_each = local.fix_window_modes
  name     = "secretsmanager-read"
  role     = aws_iam_role.fix_window_lambda[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = aws_secretsmanager_secret.clickhouse.arn
    }]
  })
}

# Pause/resume this mode's consumer around the window rewrite (scoped to that
# one service). The waiter uses DescribeServices on the same ARN.
resource "aws_iam_role_policy" "fix_window_lambda_ecs" {
  for_each = local.fix_window_modes
  name     = "ecs-pause-resume-${each.key}-consumer"
  role     = aws_iam_role.fix_window_lambda[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ecs:UpdateService", "ecs:DescribeServices"]
      Resource = aws_ecs_service.pnl_consumer[each.key].id
    }]
  })
}

# ── Lambda functions (image package) ────────────────────────────────────────
# Same digest-pinning flow as backfill_prices: CI pushes :latest before the
# terraform job runs; the data block resolves the digest so an apply updates
# the functions whenever a new image was pushed.

data "aws_ecr_image" "fix_bt_window_latest" {
  repository_name = data.aws_ecr_repository.fix_bt_window.name
  image_tag       = "latest"
}

resource "aws_lambda_function" "fix_window" {
  for_each      = local.fix_window_modes
  function_name = "${local.name_prefix}-fix-${each.key}-window"
  description   = each.value.description
  role          = aws_iam_role.fix_window_lambda[each.key].arn

  package_type = "Image"
  image_uri    = "${data.aws_ecr_repository.fix_bt_window.repository_url}@${data.aws_ecr_image.fix_bt_window_latest.image_digest}"

  memory_size = each.value.memory_size
  timeout     = 900 # seconds (15 min Lambda max); bt typical ~3-5 min, prod/rt faster

  # Never two repairs of the same mode at once (double pause/resume +
  # overlapping DELETEs). Different modes touch different tables/services and
  # may overlap freely.
  reserved_concurrent_executions = 1

  # Egress through the fck-nat EIP — the only IP ClickHouse Cloud allows.
  vpc_config {
    subnet_ids         = [aws_subnet.private.id]
    security_group_ids = [aws_security_group.ecs_tasks.id]
  }

  environment {
    variables = merge({
      CLICKHOUSE_USER       = "dagster"
      CLICKHOUSE_PORT       = "8443"
      CLICKHOUSE_SECURE     = "true"
      CLICKHOUSE_SECRET_ARN = aws_secretsmanager_secret.clickhouse.arn
      AUDIT_TYPE            = each.value.audit_type
      LOOKBACK_HOURS        = tostring(each.value.lookback_hours)
      ECS_CLUSTER           = aws_ecs_cluster.main.name
      CONSUMER_SERVICE      = aws_ecs_service.pnl_consumer[each.key].name
      }, each.value.arrival_driven ? {
      # prod/real_trade only. A fixed trailing window assumes bars arrive
      # promptly; on 2026-07-28 a publisher stall was followed by ~168k
      # revisions carrying bars up to 144h old, which a 7h window repairs none
      # of. The handler pulls the start back to the oldest bar revised in the
      # last ARRIVAL_WINDOW_HOURS, floored at LOOKBACK_HOURS.
      ARRIVAL_WINDOW_HOURS = "7"
      # Ceiling, and it is a Lambda-timeout constraint rather than a
      # correctness one: 900s max at ~2M rows / 4-5 min in-VPC means ~120h is
      # the largest window that reliably finishes. When a backfill reaches
      # further the handler logs "clamped to MAX_LOOKBACK_HOURS" and the alarm
      # below fires — repair the remainder with scripts/audit_pnl.py. If that
      # becomes routine, move these two to an ECS RunTask, don't raise the cap.
      MAX_LOOKBACK_HOURS = "120"
    } : {})
  }

  depends_on = [
    aws_cloudwatch_log_group.fix_window,
    aws_iam_role_policy_attachment.fix_window_lambda_basic,
    aws_iam_role_policy_attachment.fix_window_lambda_vpc,
  ]

  tags = local.common_tags
}

# A failed run must not auto-retry into a second pause/rewrite cycle — the next
# tick (lookback window > cadence) covers any missed span idempotently.
resource "aws_lambda_function_event_invoke_config" "fix_window" {
  for_each               = local.fix_window_modes
  function_name          = aws_lambda_function.fix_window[each.key].function_name
  maximum_retry_attempts = 0
}

# ── EventBridge Rules ───────────────────────────────────────────────────────
# bt: daily 15:00 UTC, after the cum publisher's overnight catch-up batches
# (observed 04:00-08:00 UTC). prod/real-trade: 6-hourly, offset 30 min apart.
# Every mode's lookback window exceeds its cadence, so consecutive runs
# overlap and slower arrivals are retro-corrected by the next tick.

resource "aws_cloudwatch_event_rule" "fix_window" {
  for_each            = local.fix_window_modes
  name                = "${local.name_prefix}-fix-${each.key}-window-schedule"
  description         = each.value.description
  schedule_expression = each.value.schedule
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "fix_window" {
  for_each  = local.fix_window_modes
  rule      = aws_cloudwatch_event_rule.fix_window[each.key].name
  target_id = "fix-${each.key}-window-lambda"
  arn       = aws_lambda_function.fix_window[each.key].arn
  # Empty event = rolling LOOKBACK_HOURS window (handler treats {} as default).
  input = "{}"
}

resource "aws_lambda_permission" "fix_window_allow_events" {
  for_each      = local.fix_window_modes
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fix_window[each.key].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.fix_window[each.key].arn
}

# ── Alarms ──────────────────────────────────────────────────────────────────
# Two failure modes worth knowing about: the repair stopped running, and the
# repair ran but could not reach far enough back.

resource "aws_cloudwatch_metric_alarm" "fix_window_errors" {
  for_each          = local.fix_window_modes
  alarm_name        = "${local.name_prefix}-fix-${each.key}-window-errors"
  alarm_description = "fix-${each.key}-window Lambda failed. The consumer is resumed on failure, but the window was NOT repaired — stale minutes stay baked into the chain until a later run covers them."

  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.fix_window[each.key].function_name }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  tags                = local.common_tags
}

# Fires when the arrival probe wanted to reach further back than
# MAX_LOOKBACK_HOURS. No dimensions: metric-filter dimension values must
# reference a parsed log field and the handler logs plain text, so the mode
# goes in the metric name instead.
resource "aws_cloudwatch_log_metric_filter" "fix_window_clamped" {
  for_each       = { for k, v in local.fix_window_modes : k => v if v.arrival_driven }
  name           = "${local.name_prefix}-fix-${each.key}-window-clamped"
  log_group_name = aws_cloudwatch_log_group.fix_window[each.key].name
  pattern        = "\"clamped to MAX_LOOKBACK_HOURS\""

  metric_transformation {
    name          = "FixWindowClamped-${each.key}"
    namespace     = "TradingAnalysis/PnLRepair"
    value         = "1"
    default_value = 0
  }
}

resource "aws_cloudwatch_metric_alarm" "fix_window_clamped" {
  for_each          = { for k, v in local.fix_window_modes : k => v if v.arrival_driven }
  alarm_name        = "${local.name_prefix}-fix-${each.key}-window-clamped"
  alarm_description = "A backfill landed bars older than MAX_LOOKBACK_HOURS (120h). This run repaired only the covered part — run scripts/audit_pnl.py --type ${each.value.audit_type} --fix-window manually for the remainder."

  namespace           = "TradingAnalysis/PnLRepair"
  metric_name         = "FixWindowClamped-${each.key}"
  statistic           = "Sum"
  period              = 21600
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  tags                = local.common_tags
}
