# Daily futures-price backfill — image-package Lambda + EventBridge Rule.
# VPC-attached so it egresses through the fck-nat EIP (same outbound IP as
# the streaming consumer); must run in ap-northeast-1 — Binance Futures
# geo-blocks US IPs. See handler.py for the event-payload schema.

# ECR repo owned by the build-backfill-prices CI job (see ci-cd.yml).
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
#
# Image deploy flow:
#   1. CI build job pushes :latest (and :sha) to ECR before this job runs.
#   2. This data block resolves :latest to its current immutable digest.
#   3. `aws_lambda_function.image_uri` pins to that digest (repo@sha256:…),
#      so a Terraform apply triggers a Lambda update whenever the digest
#      changes — i.e. whenever a new image was pushed.
#
# Pinning to the digest (not the floating :latest tag) is required because
# Lambda treats image_uri as opaque and won't re-pull a moved tag on its own.
# This way the lifecycle is fully Terraform-managed — no `aws lambda
# update-function-code` shelling out from CI.

data "aws_ecr_image" "backfill_prices_latest" {
  repository_name = data.aws_ecr_repository.backfill_prices.name
  image_tag       = "latest"
}

resource "aws_lambda_function" "backfill_prices" {
  function_name = "${local.name_prefix}-backfill-prices"
  description   = "Daily futures_price_1min gap fill (Binance Futures via ccxt)"
  role          = aws_iam_role.backfill_prices_lambda.arn

  package_type = "Image"
  image_uri    = "${data.aws_ecr_repository.backfill_prices.repository_url}@${data.aws_ecr_image.backfill_prices_latest.image_digest}"

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
