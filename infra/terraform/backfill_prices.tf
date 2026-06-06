# ──────────────────────────────────────────────────────────────────────────────
# Daily futures-price backfill — AWS Lambda (image package) + EventBridge Rule
#
# Replaces the deprecated Dagster `binance_futures_backfill` asset. Fetches the
# last 48h of 1-min OHLCV from Binance Futures and fills any gaps in
# analytics.futures_price_1min.
#
# VPC-attached so it egresses through the fck-nat EIP (same outbound IP as the
# streaming consumer). Must run in ap-northeast-1 (Tokyo) — Binance Futures
# geo-blocks US IPs.
#
# Cost: free tier (daily 60s × 512 MiB ≈ 900 GB-sec/month, well under 400k
# GB-sec free tier).
#
# Replaces the prior ECS RunTask + EventBridge Scheduler setup. The reasons:
#   - Cheaper (free tier vs ~$0.10/mo Fargate)
#   - No `scheduler:*` IAM perm needed on the GitHub Actions role
#   - EventBridge Rule is more widely supported than EventBridge Scheduler
#
# Ad-hoc historical run:
#   aws lambda invoke --function-name trading-analysis-backfill-prices \
#     --payload '{"window_start":"2026-06-04","window_end":"2026-06-06"}' \
#     --cli-binary-format raw-in-base64-out /dev/stdout
# ──────────────────────────────────────────────────────────────────────────────

# The ECR repo is owned by the `build-backfill-prices` CI job (it idempotently
# creates the repo before docker push) — not by Terraform. This breaks the
# chicken-and-egg between Terraform (which needs the image to exist when it
# creates the Lambda) and the CI build step (which needs the ECR repo). Build
# always runs before Terraform on this branch, so the image is guaranteed to
# exist by the time Terraform reaches aws_lambda_function below.
data "aws_ecr_repository" "backfill_prices" {
  name = "trading-analysis-backfill-prices"
}

resource "aws_cloudwatch_log_group" "backfill_prices" {
  # Lambda's auto-created log group naming convention.
  name              = "/aws/lambda/${local.name_prefix}-backfill-prices"
  retention_in_days = 14
  tags              = local.common_tags
}

# ── Lambda execution role ───────────────────────────────────────────────────

resource "aws_iam_role" "backfill_prices_lambda" {
  name = "${local.name_prefix}-backfill-prices-lambda"
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

# Logs + VPC ENI management (the VPC-access managed policy grants
# ec2:CreateNetworkInterface, DescribeNetworkInterfaces, DeleteNetworkInterface).
resource "aws_iam_role_policy_attachment" "backfill_prices_lambda_basic" {
  role       = aws_iam_role.backfill_prices_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "backfill_prices_lambda_vpc" {
  role       = aws_iam_role.backfill_prices_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# Read the ClickHouse secret bundle (handler.py fetches via boto3 on cold start).
resource "aws_iam_role_policy" "backfill_prices_lambda_secrets" {
  name = "secretsmanager-read"
  role = aws_iam_role.backfill_prices_lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = aws_secretsmanager_secret.clickhouse.arn
    }]
  })
}

# ── Lambda function (image package) ─────────────────────────────────────────

resource "aws_lambda_function" "backfill_prices" {
  function_name = "${local.name_prefix}-backfill-prices"
  description   = "Daily futures_price_1min gap fill (Binance Futures via ccxt)"
  role          = aws_iam_role.backfill_prices_lambda.arn

  package_type = "Image"
  # `:latest` is rebuilt by CI on every push to main; the deploy step then
  # calls update-function-code with the SHA-tagged image to force a refresh
  # (Lambda doesn't auto-pull a moved :latest tag).
  image_uri = "${data.aws_ecr_repository.backfill_prices.repository_url}:latest"

  memory_size = 512 # MiB — comfortably above the ~100MB working set
  timeout     = 900 # seconds (15 min Lambda max); typical daily run ~60s

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
      LOOKBACK_HOURS        = "48"
      # INSTRUMENTS defaults to the streaming consumer's list inside the script.
    }
  }

  # CI updates the image after every push; ignore drift in image_uri so
  # `terraform apply` doesn't fight the deploy step.
  lifecycle {
    ignore_changes = [image_uri]
  }

  depends_on = [
    aws_cloudwatch_log_group.backfill_prices,
    aws_iam_role_policy_attachment.backfill_prices_lambda_basic,
    aws_iam_role_policy_attachment.backfill_prices_lambda_vpc,
  ]

  tags = local.common_tags
}

# ── EventBridge Rule — daily 00:30 UTC trigger ──────────────────────────────

resource "aws_cloudwatch_event_rule" "backfill_prices_daily" {
  name                = "${local.name_prefix}-backfill-prices-daily"
  description         = "Daily futures_price_1min gap fill (replaces deprecated Dagster asset)"
  schedule_expression = "cron(30 0 * * ? *)" # 00:30 UTC daily
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "backfill_prices_daily" {
  rule      = aws_cloudwatch_event_rule.backfill_prices_daily.name
  target_id = "backfill-prices-lambda"
  arn       = aws_lambda_function.backfill_prices.arn
  # Empty event = rolling LOOKBACK_HOURS window (handler.py treats {} as default).
  input = "{}"
}

resource "aws_lambda_permission" "backfill_prices_allow_events" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.backfill_prices.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.backfill_prices_daily.arn
}
