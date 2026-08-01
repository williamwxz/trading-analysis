# Scheduled prod / real_trade PnL window repair — same image as fix_bt_window,
# two more EventBridge-driven Lambdas. bt keeps its own file and resources
# untouched (it already works; renaming it into a for_each here would force a
# destroy/recreate of a production repair job for no benefit).
#
# Why these two exist at all — measured 2026-07-31 over 07-24→07-31, per-day
# write-lag on the 1-min tables (dateDiff(ts, updated_at)):
#   bt          p50 ~133,000s, 100% of rows rewritten  -> daily repair working
#   prod        p50 61s, p99 <= 222s, ZERO rows > 1h lag on any day
#   real_trade  p50 61s, p99 <= 281s, ZERO rows > 1h lag on any day
# prod and real_trade were pure live writes that were never revisited. Because
# cumulative_pnl is a chained anchor quantity, one minute computed on a stale
# position is permanent and every later minute inherits it — so prod/rt error
# accumulates indefinitely while bt self-heals. Three upstream incidents in that
# one week (07-25 17h gap, 07-26 15h gap, 07-28 4h stall + 168k-revision
# backfill) baked in a measured +0.13% / -0.01% portfolio-weighted PnL error.
#
# ── Lambda timeout ceiling (read before raising MAX_LOOKBACK_HOURS) ──────────
# Lambda hard-caps at 900s. Observed throughput ~2M rows per 4-5 min:
#    49h window ~= 2.0M rows ~=  4-5 min   (the common case — comfortable)
#   120h window ~= 5.0M rows ~= 11-13 min  (the ceiling set below)
#   336h window ~=14.0M rows ~= 28+  min   (would time out mid-run)
# MAX_LOOKBACK_HOURS is pinned to 120h rather than the handler's 336h default.
# When a backfill reaches further back the handler logs "clamped to
# MAX_LOOKBACK_HOURS" and the alarm below fires — repair the remainder by hand.
# If clamp alarms become routine, move these two to an ECS RunTask (no time
# limit) rather than raising the cap.

locals {
  # audit type -> the pnl_consumer_sinks key whose service must be paused.
  # Note the key is hyphenated ("real-trade") while AUDIT_TYPE is not.
  fix_window_modes = {
    prod = {
      consumer_key = "prod"
      # prod reads strategy_output_history_v2 first revisions, which land
      # p50 ~16m / p90 ~72m past bar close.
      schedule    = "cron(10 3/6 * * ? *)" # 03:10, 09:10, 15:10, 21:10 UTC
      description = "6-hourly prod PnL repair (arrival-driven; late first revisions)"
    }
    real_trade = {
      consumer_key = "real-trade"
      # Offset 30m from prod so the two recomputes don't overlap on ClickHouse.
      schedule    = "cron(40 3/6 * * ? *)" # 03:40, 09:40, 15:40, 21:40 UTC
      description = "6-hourly real_trade PnL repair (arrival-driven; late revisions)"
    }
  }
}

# ── Shared execution role (both functions) ──────────────────────────────────

resource "aws_iam_role" "fix_window_lambda" {
  name = "${local.name_prefix}-fix-window-lambda"
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
  role       = aws_iam_role.fix_window_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Must be VPC-attached: ClickHouse Cloud allowlists the fck-nat EIP, and a
# non-VPC Lambda's public egress IP is refused at the TLS layer.
resource "aws_iam_role_policy_attachment" "fix_window_lambda_vpc" {
  role       = aws_iam_role.fix_window_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "fix_window_lambda_secrets" {
  name = "secretsmanager-read"
  role = aws_iam_role.fix_window_lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = aws_secretsmanager_secret.clickhouse.arn
    }]
  })
}

# Pause/resume only the two consumers these functions own.
resource "aws_iam_role_policy" "fix_window_lambda_ecs" {
  name = "ecs-pause-resume-prod-rt-consumers"
  role = aws_iam_role.fix_window_lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["ecs:UpdateService", "ecs:DescribeServices"]
      Resource = [
        for m in local.fix_window_modes :
        aws_ecs_service.pnl_consumer[m.consumer_key].id
      ]
    }]
  })
}

# ── Functions ───────────────────────────────────────────────────────────────
# Reuses the fix-bt-window ECR repo + digest data sources from fix_bt_window.tf:
# one image serves all three modes, AUDIT_TYPE selects behaviour at invoke time.

resource "aws_cloudwatch_log_group" "fix_window" {
  for_each          = local.fix_window_modes
  name              = "/aws/lambda/${local.name_prefix}-fix-${replace(each.key, "_", "-")}-window"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "fix_window" {
  for_each = local.fix_window_modes

  function_name = "${local.name_prefix}-fix-${replace(each.key, "_", "-")}-window"
  description   = each.value.description
  role          = aws_iam_role.fix_window_lambda.arn

  package_type = "Image"
  image_uri    = "${data.aws_ecr_repository.fix_bt_window.repository_url}@${data.aws_ecr_image.fix_bt_window_latest.image_digest}"

  memory_size = 1024 # MiB — the recompute batches a whole window in memory
  timeout     = 900  # Lambda max; see the timeout-ceiling note above

  vpc_config {
    subnet_ids         = [aws_subnet.private.id]
    security_group_ids = [aws_security_group.ecs_tasks.id]
  }

  environment {
    variables = {
      CLICKHOUSE_USER       = "dagster"
      CLICKHOUSE_PORT       = "8443"
      CLICKHOUSE_SECURE     = "true"
      CLICKHOUSE_SECRET_ARN = aws_secretsmanager_secret.clickhouse.arn
      ECS_CLUSTER           = aws_ecs_cluster.main.name

      AUDIT_TYPE       = each.key
      CONSUMER_SERVICE = aws_ecs_service.pnl_consumer[each.value.consumer_key].name

      LOOKBACK_HOURS = "49" # floor: always repair the recent tail
      # How far back to look for *arrivals*. Slightly over the 6h cadence so
      # consecutive runs overlap; the repair is idempotent.
      ARRIVAL_WINDOW_HOURS = "7"
      MAX_LOOKBACK_HOURS   = "120"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.fix_window,
    aws_iam_role_policy_attachment.fix_window_lambda_basic,
    aws_iam_role_policy_attachment.fix_window_lambda_vpc,
  ]

  tags = local.common_tags
}

# ── Schedules ───────────────────────────────────────────────────────────────

resource "aws_cloudwatch_event_rule" "fix_window" {
  for_each            = local.fix_window_modes
  name                = "${local.name_prefix}-fix-${replace(each.key, "_", "-")}-window"
  description         = each.value.description
  schedule_expression = each.value.schedule
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "fix_window" {
  for_each  = local.fix_window_modes
  rule      = aws_cloudwatch_event_rule.fix_window[each.key].name
  target_id = "fix-${replace(each.key, "_", "-")}-window-lambda"
  arn       = aws_lambda_function.fix_window[each.key].arn
  # Empty event = rolling window (handler.compute_window treats {} as default).
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
  alarm_name        = "${local.name_prefix}-fix-${replace(each.key, "_", "-")}-window-errors"
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
# MAX_LOOKBACK_HOURS — a backfill landed bars older than the window can cover.
# No dimensions: metric-filter dimension values must reference a parsed log
# field and the handler logs plain text, so the mode goes in the metric name.
resource "aws_cloudwatch_log_metric_filter" "fix_window_clamped" {
  for_each       = local.fix_window_modes
  name           = "${local.name_prefix}-fix-${replace(each.key, "_", "-")}-window-clamped"
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
  for_each          = local.fix_window_modes
  alarm_name        = "${local.name_prefix}-fix-${replace(each.key, "_", "-")}-window-clamped"
  alarm_description = "A backfill landed bars older than MAX_LOOKBACK_HOURS (120h). This run repaired only the covered part — run scripts/audit_pnl.py --type ${each.key} --fix-window manually for the remainder."

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
