# ──────────────────────────────────────────────────────────────────────────────
# Daily futures-price backfill — ECS RunTask triggered by EventBridge Schedule
#
# Replaces the deprecated Dagster binance_futures_backfill asset. One-shot
# Fargate task fetches the last 48h of 1-min OHLCV from Binance Futures and
# fills any gaps in analytics.futures_price_1min. Must run in ap-northeast-1
# (Tokyo) — Binance Futures geo-blocks US IPs.
#
# Cost: ≈ $0.10 / month (Fargate 0.25 vCPU × ~60s × 30 days).
# ──────────────────────────────────────────────────────────────────────────────

data "aws_caller_identity" "current" {}

resource "aws_ecr_repository" "backfill_prices" {
  name                 = "trading-analysis-backfill-prices"
  image_tag_mutability = "MUTABLE"
  tags                 = local.common_tags
}

resource "aws_cloudwatch_log_group" "backfill_prices" {
  name              = "/ecs/${local.name_prefix}-backfill-prices"
  retention_in_days = 14
  tags              = local.common_tags
}

# ── task role: SELECT/INSERT on analytics.* via existing CH creds ───────────
# Reuses the secretsmanager-stored ClickHouse creds via the same `dev_ro3`-
# style account that the rest of the stack uses. We do NOT need ECS pause/
# resume, so this role is read/write to CH only.

resource "aws_iam_role" "backfill_prices_task" {
  name = "${local.name_prefix}-backfill-prices-task"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.common_tags
}

# Reuse the existing execution role — it already grants secretsmanager pull
# and CloudWatch log push, which is all this container needs.

resource "aws_ecs_task_definition" "backfill_prices" {
  family                   = "${local.name_prefix}-backfill-prices"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256 # 0.25 vCPU
  memory                   = 512 # MiB
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.backfill_prices_task.arn

  container_definitions = jsonencode([{
    name      = "backfill-prices"
    image     = "${aws_ecr_repository.backfill_prices.repository_url}:latest"
    essential = true

    environment = [
      { name = "CLICKHOUSE_USER", value = "dagster" },
      { name = "CLICKHOUSE_PORT", value = "8443" },
      { name = "CLICKHOUSE_SECURE", value = "true" },
      { name = "LOOKBACK_HOURS", value = "48" },
      # INSTRUMENTS defaults to the streaming consumer's list inside the script.
    ]

    secrets = [
      { name = "CLICKHOUSE_HOST", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
      { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.backfill_prices.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "task"
      }
    }
  }])

  tags = local.common_tags
}

# ── EventBridge Scheduler — daily 00:30 UTC trigger ─────────────────────────
# Scheduler invokes ECS RunTask with the task definition above. Picks 00:30
# UTC so a Tokyo-region run covers the full prior calendar day in UTC plus a
# safety margin (the 48h lookback gives further redundancy).

resource "aws_iam_role" "backfill_prices_scheduler" {
  name = "${local.name_prefix}-backfill-prices-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy" "backfill_prices_scheduler" {
  name = "ecs-runtask"
  role = aws_iam_role.backfill_prices_scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Allow any revision of this task definition family
        Effect   = "Allow"
        Action   = "ecs:RunTask"
        Resource = "arn:aws:ecs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:task-definition/${aws_ecs_task_definition.backfill_prices.family}:*"
        Condition = {
          ArnLike = {
            "ecs:cluster" = aws_ecs_cluster.main.arn
          }
        }
      },
      {
        Effect = "Allow"
        Action = "iam:PassRole"
        Resource = [
          aws_iam_role.ecs_execution.arn,
          aws_iam_role.backfill_prices_task.arn,
        ]
      },
    ]
  })
}

resource "aws_scheduler_schedule" "backfill_prices_daily" {
  name        = "${local.name_prefix}-backfill-prices-daily"
  group_name  = "default"
  description = "Daily futures_price_1min gap fill (replaces deprecated Dagster asset)"

  schedule_expression          = "cron(30 0 * * ? *)" # 00:30 UTC daily
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_ecs_cluster.main.arn
    role_arn = aws_iam_role.backfill_prices_scheduler.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.backfill_prices.arn
      launch_type         = "FARGATE"
      task_count          = 1

      network_configuration {
        subnets          = [aws_subnet.private.id]
        security_groups  = [aws_security_group.ecs_tasks.id]
        assign_public_ip = false
      }
    }

    retry_policy {
      maximum_event_age_in_seconds = 3600 # 1h — re-fire if scheduler delayed
      maximum_retry_attempts       = 2
    }
  }
}
