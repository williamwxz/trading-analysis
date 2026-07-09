# Daily bt trailing-window repair — image-package Lambda + EventBridge Rule.
#
# Why: analytics.strategy_cum_pnl_bt_v2 (the bt position source) is published in
# batches hours-to-days after bar time, so the live bt consumer holds stale
# positions between deliveries (measured 2026-07: 3.8-4.6% of strategy-minutes
# diverge from prod, concentrated in fast 1h-tf strategies). Once the bars land,
# history is fully reconstructible — this Lambda re-runs the standard repair
# (audit_pnl --type bt --fix-window) over the trailing 49h daily, pausing the bt
# consumer so its post-resume bootstrap re-seeds from the repaired tail.
# See services/fix_bt_window/fix_bt_window/handler.py for the event schema.

# ECR repo owned by the build-fix-bt-window CI job (see ci-cd.yml).
data "aws_ecr_repository" "fix_bt_window" {
  name = "trading-analysis-fix-bt-window"
}

resource "aws_cloudwatch_log_group" "fix_bt_window" {
  # Lambda's auto-created log group naming convention.
  name              = "/aws/lambda/${local.name_prefix}-fix-bt-window"
  retention_in_days = 14
  tags              = local.common_tags
}

# ── Lambda execution role ───────────────────────────────────────────────────

resource "aws_iam_role" "fix_bt_window_lambda" {
  name = "${local.name_prefix}-fix-bt-window-lambda"
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

resource "aws_iam_role_policy_attachment" "fix_bt_window_lambda_basic" {
  role       = aws_iam_role.fix_bt_window_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Read the ClickHouse secret bundle (handler fetches via boto3 on cold start).
resource "aws_iam_role_policy" "fix_bt_window_lambda_secrets" {
  name = "secretsmanager-read"
  role = aws_iam_role.fix_bt_window_lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = aws_secretsmanager_secret.clickhouse.arn
    }]
  })
}

# Pause/resume the bt consumer around the window rewrite (scoped to that one
# service). The waiter uses DescribeServices on the same ARN.
resource "aws_iam_role_policy" "fix_bt_window_lambda_ecs" {
  name = "ecs-pause-resume-bt-consumer"
  role = aws_iam_role.fix_bt_window_lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ecs:UpdateService", "ecs:DescribeServices"]
      Resource = aws_ecs_service.pnl_consumer["bt"].id
    }]
  })
}

# ── Lambda function (image package) ─────────────────────────────────────────
# Same digest-pinning flow as backfill_prices: CI pushes :latest before the
# terraform job runs; the data block resolves the digest so an apply updates
# the function whenever a new image was pushed.

data "aws_ecr_image" "fix_bt_window_latest" {
  repository_name = data.aws_ecr_repository.fix_bt_window.name
  image_tag       = "latest"
}

resource "aws_lambda_function" "fix_bt_window" {
  function_name = "${local.name_prefix}-fix-bt-window"
  description   = "Daily bt trailing-window PnL repair (cum-table publish lag retro-correction)"
  role          = aws_iam_role.fix_bt_window_lambda.arn

  package_type = "Image"
  image_uri    = "${data.aws_ecr_repository.fix_bt_window.repository_url}@${data.aws_ecr_image.fix_bt_window_latest.image_digest}"

  memory_size = 2048 # MiB — 49h × 695 strategies of anchors/prices/benchmarks
  timeout     = 900  # seconds (15 min Lambda max); typical run ~3-5 min

  # Never two repairs at once (double pause/resume + overlapping DELETEs).
  reserved_concurrent_executions = 1

  environment {
    variables = {
      CLICKHOUSE_USER       = "dagster"
      CLICKHOUSE_PORT       = "8443"
      CLICKHOUSE_SECURE     = "true"
      CLICKHOUSE_SECRET_ARN = aws_secretsmanager_secret.clickhouse.arn
      LOOKBACK_HOURS        = "49"
      ECS_CLUSTER           = aws_ecs_cluster.main.name
      BT_CONSUMER_SERVICE   = aws_ecs_service.pnl_consumer["bt"].name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.fix_bt_window,
    aws_iam_role_policy_attachment.fix_bt_window_lambda_basic,
  ]

  tags = local.common_tags
}

# A failed run must not auto-retry into a second pause/rewrite cycle — the next
# daily tick (49h window > 24h cadence) covers any missed span idempotently.
resource "aws_lambda_function_event_invoke_config" "fix_bt_window" {
  function_name          = aws_lambda_function.fix_bt_window.function_name
  maximum_retry_attempts = 0
}

# ── EventBridge Rule — daily 15:00 UTC trigger ──────────────────────────────
# After the cum publisher's overnight catch-up batches (observed 04:00-08:00
# UTC); the 49h window makes consecutive runs overlap, so slower batches are
# retro-corrected by the next tick.

resource "aws_cloudwatch_event_rule" "fix_bt_window_daily" {
  name                = "${local.name_prefix}-fix-bt-window-daily"
  description         = "Daily bt PnL trailing-window repair (cum-table publish lag)"
  schedule_expression = "cron(0 15 * * ? *)" # 15:00 UTC daily
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "fix_bt_window_daily" {
  rule      = aws_cloudwatch_event_rule.fix_bt_window_daily.name
  target_id = "fix-bt-window-lambda"
  arn       = aws_lambda_function.fix_bt_window.arn
  # Empty event = rolling LOOKBACK_HOURS window (handler treats {} as default).
  input = "{}"
}

resource "aws_lambda_permission" "fix_bt_window_allow_events" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fix_bt_window.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.fix_bt_window_daily.arn
}
