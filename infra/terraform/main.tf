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

# NAT Instance — t4g.nano with Elastic IP (static outbound IP for ClickHouse allowlist)
data "aws_ami" "nat" {
  most_recent = true
  owners      = ["568608671756"] # fck-nat publisher
  filter {
    name   = "name"
    values = ["fck-nat-al2023-hvm-*-arm64-ebs"]
  }
  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

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
  ami                         = data.aws_ami.nat.id
  instance_type               = "t4g.nano"
  subnet_id                   = aws_subnet.public[0].id
  vpc_security_group_ids      = [aws_security_group.nat.id]
  iam_instance_profile        = aws_iam_instance_profile.nat_instance.name
  source_dest_check           = false
  associate_public_ip_address = true

  # iptables DNAT: forward :3000 on the EIP → ECS task private IP:3000
  # A cron job updates the DNAT rule when the ECS task IP changes
  user_data = base64encode(<<EOF
#!/bin/bash
dnf install -y aws-cli jq amazon-ssm-agent
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

# Enable IP forwarding (required for DNAT)
echo 'net.ipv4.ip_forward=1' >> /etc/sysctl.conf
sysctl -p

cat > /usr/local/bin/update-dagster-dnat.sh << 'SCRIPT'
#!/bin/bash
REGION="ap-northeast-1"
CLUSTER="trading-analysis"
SERVICE="trading-analysis-dagster"
PORT=3000
STATE_FILE="/var/run/dagster-dnat-ip"

TASK_ARN=$(aws ecs list-tasks --region $REGION --cluster $CLUSTER --service-name $SERVICE --query 'taskArns[0]' --output text 2>/dev/null)
[ -z "$TASK_ARN" ] || [ "$TASK_ARN" = "None" ] && exit 0

PRIVATE_IP=$(aws ecs describe-tasks --region $REGION --cluster $CLUSTER --tasks $TASK_ARN --query 'tasks[0].containers[0].networkInterfaces[0].privateIpv4Address' --output text 2>/dev/null)
[ -z "$PRIVATE_IP" ] || [ "$PRIVATE_IP" = "None" ] && exit 0

CURRENT_IP=$(cat $STATE_FILE 2>/dev/null || echo "")

# Skip update only if IP unchanged AND current target is reachable
if [ "$CURRENT_IP" = "$PRIVATE_IP" ]; then
    timeout 2 bash -c ">/dev/tcp/$PRIVATE_IP/$PORT" 2>/dev/null && exit 0
fi

# Remove old rule if exists
if [ -n "$CURRENT_IP" ]; then
    iptables -t nat -D PREROUTING -p tcp --dport $PORT -j DNAT --to-destination $CURRENT_IP:$PORT 2>/dev/null || true
    iptables -t nat -D POSTROUTING -d $CURRENT_IP -p tcp --dport $PORT -j MASQUERADE 2>/dev/null || true
fi

# Add new DNAT rule
iptables -t nat -A PREROUTING -p tcp --dport $PORT -j DNAT --to-destination $PRIVATE_IP:$PORT
iptables -t nat -A POSTROUTING -d $PRIVATE_IP -p tcp --dport $PORT -j MASQUERADE
echo "$PRIVATE_IP" > $STATE_FILE
echo "$(date): updated DNAT to $PRIVATE_IP:$PORT"
SCRIPT

chmod +x /usr/local/bin/update-dagster-dnat.sh
/usr/local/bin/update-dagster-dnat.sh || true
mkdir -p /etc/cron.d
# Run every 10 seconds via two staggered cron entries (cron minimum is 1 min)
echo "* * * * * root sleep 0  && /usr/local/bin/update-dagster-dnat.sh >> /var/log/dagster-dnat.log 2>&1" > /etc/cron.d/dagster-dnat
echo "* * * * * root sleep 10 && /usr/local/bin/update-dagster-dnat.sh >> /var/log/dagster-dnat.log 2>&1" >> /etc/cron.d/dagster-dnat
echo "* * * * * root sleep 20 && /usr/local/bin/update-dagster-dnat.sh >> /var/log/dagster-dnat.log 2>&1" >> /etc/cron.d/dagster-dnat
echo "* * * * * root sleep 30 && /usr/local/bin/update-dagster-dnat.sh >> /var/log/dagster-dnat.log 2>&1" >> /etc/cron.d/dagster-dnat
echo "* * * * * root sleep 40 && /usr/local/bin/update-dagster-dnat.sh >> /var/log/dagster-dnat.log 2>&1" >> /etc/cron.d/dagster-dnat
echo "* * * * * root sleep 50 && /usr/local/bin/update-dagster-dnat.sh >> /var/log/dagster-dnat.log 2>&1" >> /etc/cron.d/dagster-dnat
EOF
  )

  user_data_replace_on_change = true

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


# ─────────────────────────────────────────────────────────────────────────────
# ECS Task Definition — Dagster
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecs_task_definition" "dagster" {
  family                   = "${local.name_prefix}-dagster"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 4096
  memory                   = 8192
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "dagster-code-server"
      image     = "${aws_ecr_repository.dagster.repository_url}:latest"
      essential = true
      command   = ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4266", "-m", "trading_dagster"]

      environment = [
        { name = "DAGSTER_HOME", value = "/app" },
        { name = "CLICKHOUSE_USER", value = "dev_ro3" },
        { name = "CLICKHOUSE_PORT", value = "8443" },
        { name = "CLICKHOUSE_SECURE", value = "true" },
        { name = "DAGSTER_PG_DB", value = "postgres" },
      ]

      secrets = [
        { name = "CLICKHOUSE_HOST", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
        { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
        { name = "DAGSTER_PG_HOST", valueFrom = "${aws_secretsmanager_secret.supabase.arn}:host::" },
        { name = "DAGSTER_PG_PASSWORD", valueFrom = "${aws_secretsmanager_secret.supabase.arn}:password::" },
        { name = "DAGSTER_PG_USER", valueFrom = "${aws_secretsmanager_secret.supabase.arn}:user::" },
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.dagster.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "code-server"
        }
      }
    },
    {
      name      = "dagster-webserver"
      image     = "${aws_ecr_repository.dagster.repository_url}:latest"
      essential = true
      command   = ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]

      dependsOn = [
        { containerName = "dagster-code-server", condition = "START" }
      ]

      portMappings = [
        {
          containerPort = 3000
          protocol      = "tcp"
        }
      ]

      environment = [
        { name = "DAGSTER_HOME", value = "/app" },
        { name = "CLICKHOUSE_USER", value = "dev_ro3" },
        { name = "CLICKHOUSE_PORT", value = "8443" },
        { name = "CLICKHOUSE_SECURE", value = "true" },
        { name = "DAGSTER_PG_DB", value = "postgres" },
      ]

      secrets = [
        { name = "CLICKHOUSE_HOST", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
        { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
        { name = "DAGSTER_PG_HOST", valueFrom = "${aws_secretsmanager_secret.supabase.arn}:host::" },
        { name = "DAGSTER_PG_PASSWORD", valueFrom = "${aws_secretsmanager_secret.supabase.arn}:password::" },
        { name = "DAGSTER_PG_USER", valueFrom = "${aws_secretsmanager_secret.supabase.arn}:user::" },
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.dagster.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "webserver"
        }
      }
    },
    {
      name      = "dagster-daemon"
      image     = "${aws_ecr_repository.dagster.repository_url}:latest"
      essential = true
      command   = ["dagster-daemon", "run"]

      dependsOn = [
        { containerName = "dagster-code-server", condition = "START" }
      ]

      environment = [
        { name = "DAGSTER_HOME", value = "/app" },
        { name = "CLICKHOUSE_USER", value = "dev_ro3" },
        { name = "CLICKHOUSE_PORT", value = "8443" },
        { name = "CLICKHOUSE_SECURE", value = "true" },
        { name = "DAGSTER_PG_DB", value = "postgres" },
      ]

      secrets = [
        { name = "CLICKHOUSE_HOST", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
        { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
        { name = "DAGSTER_PG_HOST", valueFrom = "${aws_secretsmanager_secret.supabase.arn}:host::" },
        { name = "DAGSTER_PG_PASSWORD", valueFrom = "${aws_secretsmanager_secret.supabase.arn}:password::" },
        { name = "DAGSTER_PG_USER", valueFrom = "${aws_secretsmanager_secret.supabase.arn}:user::" },
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.dagster.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "daemon"
        }
      }
    }
  ])

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_ecs_service" "dagster" {
  name            = "${local.name_prefix}-dagster"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.dagster.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
  }

  network_configuration {
    subnets          = [aws_subnet.private.id]
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.dagster.arn
    container_name   = "dagster-webserver"
    container_port   = 3000
  }

  health_check_grace_period_seconds = 60

  tags = local.common_tags
}



# ─────────────────────────────────────────────────────────────────────────────
# Security Groups
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_security_group" "ecs_tasks" {
  name_prefix = "${local.name_prefix}-ecs-"
  vpc_id      = aws_vpc.main.id

  # Allow port 3000 only from the ALB
  ingress {
    from_port       = 3000
    to_port         = 3000
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

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

resource "aws_security_group" "alb" {
  name_prefix = "${local.name_prefix}-alb-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
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

# ─────────────────────────────────────────────────────────────────────────────
# ALB — Dagster UI
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_lb" "dagster" {
  name               = "${local.name_prefix}-dagster"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id

  tags = local.common_tags
}

resource "aws_lb_target_group" "dagster" {
  name        = "${local.name_prefix}-dagster"
  port        = 3000
  protocol    = "HTTP"
  vpc_id      = aws_vpc.main.id
  target_type = "ip"

  health_check {
    path                = "/"
    protocol            = "HTTP"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 30
    timeout             = 5
    matcher             = "200-399"
  }

  tags = local.common_tags
}

resource "aws_lb_listener" "dagster_http" {
  load_balancer_arn = aws_lb.dagster.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.dagster.arn
  }

  tags = local.common_tags
}


# ─────────────────────────────────────────────────────────────────────────────
# VPC Endpoints — ECR + S3 (bypass NAT for image pulls from private subnet)
# ─────────────────────────────────────────────────────────────────────────────

# Security group for interface endpoints (allow HTTPS from ECS tasks)
resource "aws_security_group" "vpc_endpoints" {
  name_prefix = "${local.name_prefix}-vpce-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
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

# ECR API endpoint (for Docker auth + manifest fetches)
resource "aws_vpc_endpoint" "ecr_api" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.ecr.api"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
  tags                = merge(local.common_tags, { Name = "${local.name_prefix}-ecr-api" })
}

# ECR DKR endpoint (for image layer pulls)
resource "aws_vpc_endpoint" "ecr_dkr" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.ecr.dkr"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
  tags                = merge(local.common_tags, { Name = "${local.name_prefix}-ecr-dkr" })
}

# S3 gateway endpoint (free — ECR layers are stored in S3)
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
  tags              = merge(local.common_tags, { Name = "${local.name_prefix}-s3" })
}

# ─────────────────────────────────────────────────────────────────────────────
# ECR Repository
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecr_repository" "dagster" {
  name                 = "${local.name_prefix}-dagster"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}



# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Logs
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "dagster" {
  name              = "/ecs/${local.name_prefix}-dagster"
  retention_in_days = 3
  tags              = local.common_tags
}

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
          title  = "Candle Lag (seconds) — pnl-consumer"
          region = "ap-northeast-1"
          metrics = [
            ["trading-analysis", "CandleLagSeconds"]
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
          title  = "Candle Processing Timestamp (Unix epoch)"
          region = "ap-northeast-1"
          metrics = [
            ["trading-analysis", "CandleProcessingTs"]
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
        Effect = "Allow"
        Action = ["secretsmanager:ListSecrets"]
        Resource = "*"
      },
    ]
  })
}


# ─────────────────────────────────────────────────────────────────────────────
# IAM User — Grafana Cloud CloudWatch read access (AWS/Billing namespace metrics)
# Grants read access to CloudWatch EstimatedCharges metrics in us-east-1 only.
# Does NOT grant Cost Explorer (ce:*) access — billing data comes from CloudWatch.
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_user" "grafana_cloudwatch" {
  name = "${local.name_prefix}-grafana-cloudwatch"
  tags = local.common_tags
}

resource "aws_iam_user_policy" "grafana_cloudwatch" {
  name = "cloudwatch-billing-read"
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
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = ["AWS/Billing"]
          }
        }
      }
    ]
  })
}

resource "aws_iam_access_key" "grafana_cloudwatch" {
  user = aws_iam_user.grafana_cloudwatch.name
}

# Store credentials in Secrets Manager so they are never exposed as Terraform outputs.
# Retrieve after terraform apply:
#   aws secretsmanager get-secret-value \
#     --secret-id trading-analysis/grafana-cloudwatch \
#     --region ap-northeast-1 \
#     --query SecretString --output text
resource "aws_secretsmanager_secret" "grafana_cloudwatch" {
  name        = "${local.name_prefix}/grafana-cloudwatch"
  description = "Grafana Cloud CloudWatch datasource credentials (access key ID + secret)"
  tags        = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
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
    name      = "redpanda"
    image     = "redpandadata/redpanda:latest"
    essential = true
    entryPoint = ["/bin/bash", "-c"]
    command = [
      "rpk redpanda start --smp 1 --memory 1500M --reserve-memory 0M --node-id 0 --kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr PLAINTEXT://redpanda.${local.name_prefix}.local:9092 --set redpanda.log_segment_ms=604800000 & sleep 15 && rpk topic create binance.price.ticks --brokers localhost:9092 --partitions 1 --replicas 1 && rpk topic alter-config binance.price.ticks --brokers localhost:9092 --set retention.ms=604800000; wait"
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

resource "aws_ecs_task_definition" "pnl_consumer" {
  family                   = "${local.name_prefix}-pnl-consumer"
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
      { name = "CLICKHOUSE_HOST",     valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
      { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
    ]
    environment = [
      { name = "CLICKHOUSE_PORT",     value = "8443" },
      { name = "CLICKHOUSE_USER",     value = "dev_ro3" },
      { name = "CLICKHOUSE_SECURE",   value = "true" },
      { name = "REDPANDA_BROKERS",    value = "redpanda.${local.name_prefix}.local:9092" },
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
        "awslogs-stream-prefix" = "pnl-consumer"
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
  name            = "${local.name_prefix}-pnl-consumer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.pnl_consumer.arn
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

# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "ecs_cluster_name" {
  value = aws_ecs_cluster.main.name
}

output "ecr_repository_url" {
  value = aws_ecr_repository.dagster.repository_url
}

output "dagster_url" {
  value       = "http://${aws_lb.dagster.dns_name}"
  description = "Dagster UI - ALB DNS name"
}

output "nat_static_ip" {
  value       = aws_eip.nat.public_ip
  description = "Static IP (outbound ClickHouse allowlist)"
}

output "grafana_cloudwatch_secret_arn" {
  value       = aws_secretsmanager_secret.grafana_cloudwatch.arn
  description = "ARN of the Secrets Manager secret holding Grafana CloudWatch datasource credentials"
}
