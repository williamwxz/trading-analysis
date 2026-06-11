# ============================================================================
# trading-analysis Infrastructure — VPC + NAT + ECS
# ============================================================================
# Consolidated in ap-northeast-1 (Tokyo).
# ============================================================================

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "trading-analysis-tfstate-068704208855"
    key    = "infra/terraform.tfstate"
    region = "ap-northeast-1"
  }
}

provider "aws" {
  region = var.aws_region
}


# ─────────────────────────────────────────────────────────────────────────────
# Variables
# ─────────────────────────────────────────────────────────────────────────────

variable "aws_region" {
  default = "ap-northeast-1"
}

variable "environment" {
  default = "default"
}

variable "project" {
  default = "trading-analysis"
}

variable "github_repo" {
  description = "GitHub repository in org/repo format"
  type        = string
  default     = "williamwxz/trading-analysis"
}

locals {
  name_prefix = var.project
  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}



# ─────────────────────────────────────────────────────────────────────────────
# VPC & Networking (Replacing Default VPC with NAT setup)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = merge(local.common_tags, { Name = local.name_prefix })
}

data "aws_availability_zones" "available" {}

# Public Subnets (for ALB and ECS tasks)
resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.${count.index + 1}.0/24"
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  tags                    = merge(local.common_tags, { Name = "${local.name_prefix}-public-${count.index}" })
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = local.common_tags
}

# Routing for Public Subnets (to IGW)
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = merge(local.common_tags, { Name = "${local.name_prefix}-rt-public" })
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# Private Subnet (for ECS tasks — routes outbound through NAT instance)
resource "aws_subnet" "private" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.10.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]
  tags              = merge(local.common_tags, { Name = "${local.name_prefix}-private-0" })
}

# NAT Instance — t4g.nano with Elastic IP (static outbound IP for ClickHouse allowlist).
# AMI pinned (was `data "aws_ami" "nat" { most_recent = true }` — every new
# fck-nat release forced a NAT replacement and hit the account vCPU quota).
# To upgrade: change the literal below and expect a brief NAT outage.
resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = merge(local.common_tags, { Name = "${local.name_prefix}-nat-eip" })
}

resource "aws_security_group" "nat" {
  name_prefix = "${local.name_prefix}-nat-"
  vpc_id      = aws_vpc.main.id

  # NAT traffic from private subnet
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_subnet.private.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_instance" "nat" {
  ami                         = "ami-0e00b812422d8cf2c" # fck-nat-al2023-hvm-1.4.0-20260126-arm64-ebs
  instance_type               = "t4g.nano"
  subnet_id                   = aws_subnet.public[0].id
  vpc_security_group_ids      = [aws_security_group.nat.id]
  iam_instance_profile        = aws_iam_instance_profile.nat_instance.name
  source_dest_check           = false
  associate_public_ip_address = true

  # Minimal cloud-init: install AWS CLI + SSM agent. The Dagster DNAT-to-:3000
  # iptables script was removed when Dagster was deprecated; with no public
  # ingress on this instance, the NAT just routes outbound traffic from the
  # private subnet to its EIP.
  user_data = base64encode(<<EOF
#!/bin/bash
dnf install -y aws-cli jq amazon-ssm-agent
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent
EOF
  )

  # Intentionally NOT setting user_data_replace_on_change: the script is just
  # one-time bootstrap (installs CLI + SSM agent). Editing the user_data text
  # later doesn't need to recycle the instance, and forcing replacement was
  # tripping the account vCPU limit (create_before_destroy briefly needs 2x
  # NAT vCPUs, which exceeded the 16-vCPU bucket cap).

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-nat-instance" })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_eip_association" "nat" {
  instance_id   = aws_instance.nat.id
  allocation_id = aws_eip.nat.id
}

# Private route table — sends all outbound through the NAT instance
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block           = "0.0.0.0/0"
    network_interface_id = aws_instance.nat.primary_network_interface_id
  }
  tags = merge(local.common_tags, { Name = "${local.name_prefix}-rt-private" })
}

resource "aws_route_table_association" "private" {
  subnet_id      = aws_subnet.private.id
  route_table_id = aws_route_table.private.id
}


# ─────────────────────────────────────────────────────────────────────────────
# ECS Cluster (Fargate)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecs_cluster" "main" {
  name = local.name_prefix

  setting {
    name  = "containerInsights"
    value = "disabled"
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name = aws_ecs_cluster.main.name

  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }
}


# Dagster was deprecated 2026-06-09. The batch refresh assets were superseded
# by the streaming pnl_consumer + a daily Lambda backfill (backfill_prices.tf).
# Removed: aws_ecs_task_definition.dagster, aws_ecs_service.dagster,
# aws_ecr_repository.dagster, aws_cloudwatch_log_group.dagster, the NAT SG
# ingress :3000, the DNAT iptables script in the NAT instance user_data, the
# ecs_tasks SG ingress :3000, and the `dagster_url` / `ecr_repository_url`
# outputs. The services/dagster/ source remains in-tree for now.

# ─────────────────────────────────────────────────────────────────────────────
# Security Groups
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_security_group" "ecs_tasks" {
  name_prefix = "${local.name_prefix}-ecs-"
  vpc_id      = aws_vpc.main.id

  # All outbound
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# ECR Repository
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Logs
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "streaming" {
  name              = "/ecs/${local.name_prefix}"
  retention_in_days = 3
  tags              = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Metric Filter — pnl-consumer throughput
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_metric_filter" "pnl_messages_received" {
  name           = "pnl-consumer-messages-received"
  log_group_name = aws_cloudwatch_log_group.streaming.name
  pattern        = "\"Received \""

  metric_transformation {
    name      = "MessagesReceived"
    namespace = "trading-analysis"
    value     = "1"
    unit      = "Count"
  }
}

resource "aws_cloudwatch_log_metric_filter" "ws_messages_published" {
  name           = "ws-consumer-messages-published"
  log_group_name = aws_cloudwatch_log_group.streaming.name
  pattern        = "\"Published \""

  metric_transformation {
    name      = "MessagesPublished"
    namespace = "trading-analysis"
    value     = "1"
    unit      = "Count"
  }
}

resource "aws_cloudwatch_log_metric_filter" "clickhouse_sink_prod" {
  name           = "pnl-consumer-clickhouse-sink-prod"
  log_group_name = aws_cloudwatch_log_group.streaming.name
  pattern        = "\"ClickHouseSink prod\""

  metric_transformation {
    name      = "ClickHouseSinkProd"
    namespace = "trading-analysis"
    value     = "1"
    unit      = "Count"
  }
}

resource "aws_cloudwatch_log_metric_filter" "clickhouse_sink_real_trade" {
  name           = "pnl-consumer-clickhouse-sink-real-trade"
  log_group_name = aws_cloudwatch_log_group.streaming.name
  pattern        = "\"ClickHouseSink real_trade\""

  metric_transformation {
    name      = "ClickHouseSinkRealTrade"
    namespace = "trading-analysis"
    value     = "1"
    unit      = "Count"
  }
}

resource "aws_cloudwatch_log_metric_filter" "clickhouse_sink_bt" {
  name           = "pnl-consumer-clickhouse-sink-bt"
  log_group_name = aws_cloudwatch_log_group.streaming.name
  pattern        = "\"ClickHouseSink bt\""

  metric_transformation {
    name      = "ClickHouseSinkBt"
    namespace = "trading-analysis"
    value     = "1"
    unit      = "Count"
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Dashboard — streaming throughput
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_dashboard" "streaming_throughput" {
  dashboard_name = "streaming-throughput"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 24
        height = 6
        properties = {
          title  = "Messages Published / min (ws-consumer → Redpanda)"
          region = "ap-northeast-1"
          metrics = [
            ["trading-analysis", "MessagesPublished"]
          ]
          stat   = "Sum"
          period = 60
          view   = "timeSeries"
          yAxis  = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 24
        height = 6
        properties = {
          title  = "Messages Received / min (Redpanda → pnl-consumer)"
          region = "ap-northeast-1"
          metrics = [
            ["trading-analysis", "MessagesReceived"]
          ]
          stat   = "Sum"
          period = 60
          view   = "timeSeries"
          yAxis  = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 24
        height = 6
        properties = {
          title  = "Candle Lag (seconds) — pnl-consumer by sink"
          region = "ap-northeast-1"
          metrics = [
            ["trading-analysis", "CandleLagSeconds", "Sink", "price", { label = "price" }],
            ["trading-analysis", "CandleLagSeconds", "Sink", "prod", { label = "prod" }],
            ["trading-analysis", "CandleLagSeconds", "Sink", "real-trade", { label = "real_trade" }],
            ["trading-analysis", "CandleLagSeconds", "Sink", "bt", { label = "bt" }]
          ]
          stat   = "Maximum"
          period = 60
          view   = "timeSeries"
          yAxis  = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 18
        width  = 24
        height = 6
        properties = {
          title  = "Candle Processing Timestamp — current ts per sink (Unix epoch)"
          region = "ap-northeast-1"
          metrics = [
            ["trading-analysis", "CandleProcessingTs", "Sink", "price", { label = "price" }],
            ["trading-analysis", "CandleProcessingTs", "Sink", "prod", { label = "prod" }],
            ["trading-analysis", "CandleProcessingTs", "Sink", "real-trade", { label = "real_trade" }],
            ["trading-analysis", "CandleProcessingTs", "Sink", "bt", { label = "bt" }]
          ]
          stat   = "Maximum"
          period = 60
          view   = "timeSeries"
          yAxis  = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 24
        width  = 24
        height = 6
        properties = {
          title  = "ClickHouse Throughput — rows flushed / flush"
          region = "ap-northeast-1"
          metrics = [
            ["trading-analysis", "ClickHouseSinkProd", { label = "prod" }],
            ["trading-analysis", "ClickHouseSinkRealTrade", { label = "real_trade" }],
            ["trading-analysis", "ClickHouseSinkBt", { label = "bt" }]
          ]
          stat   = "Sum"
          period = 60
          view   = "timeSeries"
          yAxis  = { left = { min = 0 } }
        }
      }
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Secrets Manager
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_secretsmanager_secret" "clickhouse" {
  name = "${local.name_prefix}/clickhouse"
  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_secretsmanager_secret" "supabase" {
  name = "${local.name_prefix}/supabase"
  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}


# ─────────────────────────────────────────────────────────────────────────────
# IAM Roles
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "ecs_execution" {
  name = "${local.name_prefix}-ecs-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# IAM role for NAT instance — allows nginx upstream script to query ECS task IPs
resource "aws_iam_role" "nat_instance" {
  name = "${local.name_prefix}-nat-instance"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "nat_instance_ecs" {
  name = "ecs-read"
  role = aws_iam_role.nat_instance.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ecs:ListTasks", "ecs:DescribeTasks"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "nat_instance_ssm" {
  role       = aws_iam_role.nat_instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "nat_instance" {
  name = "${local.name_prefix}-nat-instance"
  role = aws_iam_role.nat_instance.name
}

resource "aws_iam_role_policy" "ecs_execution_secrets" {
  name = "secrets-access"
  role = aws_iam_role.ecs_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
        ]
        Resource = [
          aws_secretsmanager_secret.clickhouse.arn,
          aws_secretsmanager_secret.supabase.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role" "ecs_task" {
  name = "${local.name_prefix}-ecs-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_iam_role_policy" "ecs_task_policy" {
  name = "task-access"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # EcsRunLauncher: introspect task definition and launch/monitor/stop run tasks
        Effect = "Allow"
        Action = [
          "ecs:DescribeTaskDefinition",
          "ecs:RunTask",
          "ecs:StopTask",
          "ecs:DescribeTasks",
          "ecs:ListTasks",
          "ecs:DescribeServices",
          "ecs:UpdateService",
        ]
        Resource = "*"
      },
      {
        # EcsRunLauncher: pass execution + task roles to new run tasks
        Effect = "Allow"
        Action = ["iam:PassRole"]
        Resource = [
          aws_iam_role.ecs_execution.arn,
          aws_iam_role.ecs_task.arn,
        ]
      },
      {
        # Dagster EcsRunLauncher: discover secrets to inject into run tasks
        Effect   = "Allow"
        Action   = ["secretsmanager:ListSecrets"]
        Resource = "*"
      },
    ]
  })
}


# ─────────────────────────────────────────────────────────────────────────────
# IAM User — Grafana Cloud CloudWatch read access
# Grants read access to AWS/Billing, ECS/ContainerInsights, and trading-analysis namespaces.
# Does NOT grant Cost Explorer (ce:*) access — billing data comes from CloudWatch.
# ─────────────────────────────────────────────────────────────────────────────

# Grafana Cloud assumes this role via cross-account trust (Grafana Labs AWS account: 008923505280).
# Grafana Cloud CloudWatch datasource — uses Access & secret key auth (ARN/AssumeRole deprecated since Grafana 7.3).
# After terraform apply, retrieve credentials and configure in Grafana Cloud:
#   Connections → Data Sources → CloudWatch → Auth: Access & secret key
#   Access Key ID / Secret Access Key: from Secrets Manager (see output below)
#   Default Region: ap-northeast-1
resource "aws_iam_user" "grafana_cloudwatch" {
  name = "${local.name_prefix}-grafana-cloudwatch"
  tags = local.common_tags
}

resource "aws_iam_user_policy" "grafana_cloudwatch" {
  name = "cloudwatch-read"
  user = aws_iam_user.grafana_cloudwatch.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:ListMetrics",
          "cloudwatch:GetMetricData",
          "cloudwatch:DescribeAlarmsForMetric",
          "cloudwatch:DescribeAlarmHistory",
          "cloudwatch:DescribeAlarms",
          "cloudwatch:ListTagsForResource",
          "cloudwatch:GetInsightRuleReport",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
          "logs:GetLogEvents",
          "logs:StartQuery",
          "logs:StopQuery",
          "logs:GetQueryResults",
          "logs:GetLogGroupFields",
          "ec2:DescribeTags",
          "ec2:DescribeInstances",
          "ec2:DescribeRegions",
          "tag:GetResources",
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_access_key" "grafana_cloudwatch" {
  user = aws_iam_user.grafana_cloudwatch.name
}

resource "aws_secretsmanager_secret" "grafana_cloudwatch" {
  name                    = "${local.name_prefix}/grafana-cloudwatch"
  recovery_window_in_days = 0
  tags                    = local.common_tags
}

resource "aws_secretsmanager_secret_version" "grafana_cloudwatch" {
  secret_id = aws_secretsmanager_secret.grafana_cloudwatch.id
  secret_string = jsonencode({
    access_key_id     = aws_iam_access_key.grafana_cloudwatch.id
    secret_access_key = aws_iam_access_key.grafana_cloudwatch.secret
  })
}


# ─────────────────────────────────────────────────────────────────────────────
# ECR — Streaming images
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecr_repository" "ws_consumer" {
  name                 = "trading-analysis-ws-consumer"
  image_tag_mutability = "MUTABLE"
  tags                 = local.common_tags
}

resource "aws_ecr_repository" "pnl_consumer" {
  name                 = "trading-analysis-pnl-consumer"
  image_tag_mutability = "MUTABLE"
  tags                 = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM — pnl-consumer task role
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "pnl_consumer_task" {
  name = "${local.name_prefix}-pnl-consumer-task"
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

resource "aws_iam_role_policy" "pnl_consumer_cloudwatch" {
  name = "cloudwatch-put-metrics"
  role = aws_iam_role.pnl_consumer_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["cloudwatch:PutMetricData"]
      Resource = "*"
    }]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Security group — Redpanda (internal VPC only)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_security_group" "redpanda" {
  name        = "${local.name_prefix}-redpanda"
  description = "Redpanda broker - internal VPC only"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
    description = "Kafka API from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# ECS Task Definitions
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecs_task_definition" "redpanda" {
  family                   = "${local.name_prefix}-redpanda"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = aws_iam_role.ecs_execution.arn

  container_definitions = jsonencode([{
    name       = "redpanda"
    image      = "redpandadata/redpanda:latest"
    essential  = true
    entryPoint = ["/bin/bash", "-c"]
    command = [
      "rpk redpanda start --smp 1 --memory 1500M --reserve-memory 0M --node-id 0 --kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr PLAINTEXT://redpanda.${local.name_prefix}.local:9092 --set redpanda.log_segment_ms=604800000 & sleep 15 && rpk topic create binance.price.ticks --brokers localhost:9092 --partitions 1 --replicas 1 && rpk topic alter-config binance.price.ticks --brokers localhost:9092 --set retention.ms=2592000000; wait"
    ]
    portMappings = [{ containerPort = 9092, protocol = "tcp" }]
    healthCheck = {
      command     = ["CMD-SHELL", "rpk cluster info --brokers localhost:9092 > /dev/null 2>&1 || exit 1"]
      interval    = 15
      timeout     = 5
      retries     = 3
      startPeriod = 30
    }
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.streaming.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "redpanda"
      }
    }
  }])

  tags = local.common_tags
}

resource "aws_ecs_task_definition" "ws_consumer" {
  family                   = "${local.name_prefix}-ws-consumer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.ecs_execution.arn

  container_definitions = jsonencode([{
    name      = "ws-consumer"
    image     = "${aws_ecr_repository.ws_consumer.repository_url}:latest"
    essential = true
    environment = [
      { name = "REDPANDA_BROKERS", value = "redpanda.${local.name_prefix}.local:9092" }
    ]
    healthCheck = {
      command     = ["CMD-SHELL", "python -c 'import os; os.kill(1, 0)'"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 15
    }
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.streaming.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ws-consumer"
      }
    }
  }])

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# PnL consumer — one task definition + service per sink
# Each sink has its own Kafka consumer group so offsets are tracked independently.
# ─────────────────────────────────────────────────────────────────────────────

locals {
  pnl_consumer_sinks = {
    price = {
      enable_price      = "true"
      enable_prod       = "false"
      enable_real_trade = "false"
      enable_bt         = "false"
      group_id          = "pnl-consumer-price"
      desired_count     = 1
      clickhouse_user   = "dev_ro3"
    }
    prod = {
      enable_price      = "false"
      enable_prod       = "true"
      enable_real_trade = "false"
      enable_bt         = "false"
      group_id          = "pnl-consumer-prod-2"
      desired_count     = 1
      clickhouse_user   = "streaming"
    }
    real-trade = {
      enable_price      = "false"
      enable_prod       = "false"
      enable_real_trade = "true"
      enable_bt         = "false"
      group_id          = "pnl-consumer-real-trade-5"
      desired_count     = 1
      clickhouse_user   = "streaming"
    }
    bt = {
      enable_price      = "false"
      enable_prod       = "false"
      enable_real_trade = "false"
      enable_bt         = "true"
      group_id          = "pnl-consumer-bt-1"
      desired_count     = 1
      clickhouse_user   = "streaming"
    }
  }
}

resource "aws_ecs_task_definition" "pnl_consumer" {
  for_each                 = local.pnl_consumer_sinks
  family                   = "${local.name_prefix}-pnl-consumer-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 2048
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.pnl_consumer_task.arn

  container_definitions = jsonencode([{
    name      = "pnl-consumer"
    image     = "${aws_ecr_repository.pnl_consumer.repository_url}:latest"
    essential = true
    secrets = [
      { name = "CLICKHOUSE_HOST", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
      { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
    ]
    environment = [
      { name = "CLICKHOUSE_PORT", value = "8443" },
      { name = "CLICKHOUSE_USER", value = each.value.clickhouse_user },
      { name = "CLICKHOUSE_SECURE", value = "true" },
      { name = "REDPANDA_BROKERS", value = "redpanda.${local.name_prefix}.local:9092" },
      { name = "KAFKA_GROUP_ID", value = each.value.group_id },
      { name = "ENABLE_PRICE_SINK", value = each.value.enable_price },
      { name = "ENABLE_PROD_SINK", value = each.value.enable_prod },
      { name = "ENABLE_REAL_TRADE_SINK", value = each.value.enable_real_trade },
      { name = "ENABLE_BT_SINK", value = each.value.enable_bt },
    ]
    healthCheck = {
      command     = ["CMD-SHELL", "python -c 'import os; os.kill(1, 0)'"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 15
    }
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.streaming.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "pnl-consumer-${each.key}"
      }
    }
  }])

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Cloud Map — private DNS namespace for ECS service discovery
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_service_discovery_private_dns_namespace" "main" {
  name        = "${local.name_prefix}.local"
  vpc         = aws_vpc.main.id
  description = "Private DNS for ECS service discovery"
  tags        = local.common_tags
}

resource "aws_service_discovery_service" "redpanda" {
  name = "redpanda"

  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.main.id
    routing_policy = "MULTIVALUE"
    dns_records {
      ttl  = 10
      type = "A"
    }
  }

  health_check_custom_config {
    failure_threshold = 1
  }

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# ECS Services (FARGATE_SPOT)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecs_service" "redpanda" {
  name            = "${local.name_prefix}-redpanda"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.redpanda.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    subnets          = [aws_subnet.private.id]
    security_groups  = [aws_security_group.redpanda.id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.redpanda.arn
  }

  tags = local.common_tags
}

resource "aws_ecs_service" "ws_consumer" {
  name            = "${local.name_prefix}-ws-consumer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.ws_consumer.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    subnets          = [aws_subnet.private.id]
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  tags = local.common_tags
}

resource "aws_ecs_service" "pnl_consumer" {
  for_each        = local.pnl_consumer_sinks
  name            = "${local.name_prefix}-pnl-consumer-${each.key}"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.pnl_consumer[each.key].arn
  desired_count   = each.value.desired_count

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    subnets          = [aws_subnet.private.id]
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Alarms — pnl-consumer crash loop detection
# Fires when StoppedTaskCount >= 3 in a 10-minute window for any pnl-consumer
# service. No notification action — visible in CloudWatch console only.
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "pnl_consumer_crash" {
  for_each = local.pnl_consumer_sinks

  alarm_name        = "${local.name_prefix}-pnl-consumer-${each.key}-crash-loop"
  alarm_description = "pnl-consumer-${each.key} stopped >= 3 times in 10 min (crash loop)"
  namespace         = "ECS/ContainerInsights"
  metric_name       = "StoppedTaskCount"
  dimensions = {
    ClusterName = aws_ecs_cluster.main.name
    ServiceName = aws_ecs_service.pnl_consumer[each.key].name
  }
  statistic           = "Sum"
  period              = 600
  evaluation_periods  = 1
  threshold           = 3
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "ecs_cluster_name" {
  value = aws_ecs_cluster.main.name
}

output "nat_static_ip" {
  value       = aws_eip.nat.public_ip
  description = "Static IP (outbound ClickHouse allowlist)"
}

output "grafana_cloudwatch_credentials_secret" {
  value       = aws_secretsmanager_secret.grafana_cloudwatch.name
  description = "Secrets Manager secret name containing Grafana CloudWatch datasource access key credentials"
}
