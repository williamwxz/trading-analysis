# ============================================================================
# trading-analysis Infrastructure — VPC + NAT + ECS + RDS + Kinesis
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
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  backend "s3" {
    bucket = "trading-analysis-tfstate"
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

# Public Subnets (for ALB and NAT GW)
resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.${count.index + 1}.0/24"
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  tags                    = merge(local.common_tags, { Name = "${local.name_prefix}-public-${count.index}" })
}

# Private Subnets (for ECS and RDS)
resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 10}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]
  tags              = merge(local.common_tags, { Name = "${local.name_prefix}-private-${count.index}" })
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = local.common_tags
}

# NAT Gateway & Elastic IP
resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = local.common_tags
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id # Place NAT in the first public subnet
  tags          = merge(local.common_tags, { Name = "${local.name_prefix}-nat" })
  depends_on    = [aws_internet_gateway.main]
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

# Routing for Private Subnets (to NAT Gateway)
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }
  tags = merge(local.common_tags, { Name = "${local.name_prefix}-rt-private" })
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}


# ─────────────────────────────────────────────────────────────────────────────
# S3 Bucket (data lake)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "data" {
  bucket = "${local.name_prefix}-data-v2"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}


# ─────────────────────────────────────────────────────────────────────────────
# RDS — Postgres for Dagster Metadata
# ─────────────────────────────────────────────────────────────────────────────

resource "random_password" "db_password" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "aws_db_instance" "dagster" {
  identifier           = "${local.name_prefix}-db-v2"
  allocated_storage    = 20
  engine               = "postgres"
  engine_version       = "16.6"
  instance_class       = "db.t4g.micro"
  db_name              = "dagster"
  username             = "dagster"
  password             = random_password.db_password.result
  parameter_group_name = "default.postgres16"
  skip_final_snapshot  = true
  publicly_accessible  = false
  vpc_security_group_ids = [aws_security_group.db.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_db_subnet_group" "main" {
  name       = "${local.name_prefix}-db-subnets-v2"
  subnet_ids = aws_subnet.private[*].id # Database lives in private subnets
  tags       = local.common_tags
}

resource "aws_security_group" "db" {
  name_prefix = "${local.name_prefix}-db-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_tasks.id]
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

resource "aws_secretsmanager_secret_version" "dagster_pg" {
  secret_id = aws_secretsmanager_secret.dagster_pg.id
  secret_string = jsonencode({
    password = random_password.db_password.result
  })
}


# ─────────────────────────────────────────────────────────────────────────────
# ECS Cluster (Fargate)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecs_cluster" "main" {
  name = local.name_prefix

  setting {
    name  = "containerInsights"
    value = "enabled"
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
  cpu                      = 8192
  memory                   = 16384
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "dagster-webserver"
      image     = "${aws_ecr_repository.dagster.repository_url}:latest"
      essential = true
      command   = ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "trading_dagster"]

      portMappings = [
        {
          containerPort = 3000
          protocol      = "tcp"
        }
      ]

      environment = [
        { name = "DAGSTER_HOME",        value = "/app" },
        { name = "CLICKHOUSE_USER",     value = "dev_ro3" },
        { name = "CLICKHOUSE_PORT",     value = "8443" },
        { name = "CLICKHOUSE_SECURE",   value = "true" },
        { name = "DAGSTER_PG_USERNAME", value = "dagster" },
        { name = "DAGSTER_PG_PORT",     value = "5432" },
        { name = "DAGSTER_PG_DATABASE", value = "dagster" },
        { name = "DAGSTER_PG_HOST",     value = aws_db_instance.dagster.address },
      ]

      secrets = [
        { name = "CLICKHOUSE_HOST",     valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
        { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
        { name = "DAGSTER_PG_PASSWORD", valueFrom = "${aws_secretsmanager_secret.dagster_pg.arn}:password::" },
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
      command   = ["dagster-daemon", "run", "-m", "trading_dagster"]

      environment = [
        { name = "DAGSTER_HOME",    value = "/app" },
        { name = "CLICKHOUSE_USER",    value = "dev_ro3" },
        { name = "CLICKHOUSE_PORT",    value = "8443" },
        { name = "CLICKHOUSE_SECURE",  value = "true" },
        { name = "DAGSTER_PG_USERNAME", value = "dagster" },
        { name = "DAGSTER_PG_PORT",     value = "5432" },
        { name = "DAGSTER_PG_DATABASE", value = "dagster" },
        { name = "DAGSTER_PG_HOST",     value = aws_db_instance.dagster.address },
      ]

      secrets = [
        { name = "CLICKHOUSE_HOST",     valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
        { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
        { name = "DAGSTER_PG_PASSWORD", valueFrom = "${aws_secretsmanager_secret.dagster_pg.arn}:password::" },
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
  launch_type     = "FARGATE"

  health_check_grace_period_seconds = 60

  load_balancer {
    target_group_arn = aws_lb_target_group.dagster.arn
    container_name   = "dagster-webserver"
    container_port   = 3000
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id # Tasks move to private subnets
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false # No longer need public IPs
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [aws_lb_listener.http]
}


# ─────────────────────────────────────────────────────────────────────────────
# ECS Task Definition — Grafana
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecs_task_definition" "grafana" {
  family                   = "${local.name_prefix}-grafana"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "grafana"
      image     = "${aws_ecr_repository.grafana.repository_url}:latest"
      essential = true

      portMappings = [
        {
          containerPort = 3000
          protocol      = "tcp"
        }
      ]

      environment = [
        { name = "GF_SECURITY_ADMIN_PASSWORD", value = "changeme" },
        { name = "GF_SERVER_HTTP_PORT",        value = "3000" },
        { name = "GF_SERVER_ROOT_URL",         value = "http://${aws_lb.main.dns_name}/grafana" },
        { name = "GF_SERVER_SERVE_FROM_SUB_PATH", value = "true" },
        { name = "GF_AUTH_ANONYMOUS_ENABLED",  value = "true" },
        { name = "GF_AUTH_ANONYMOUS_ORG_ROLE", value = "Admin" },
        { name = "CLICKHOUSE_USER",            value = "dev_ro3" },
      ]

      secrets = [
        { name = "CLICKHOUSE_HOST",     valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
        { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.grafana.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "grafana"
        }
      }
    }
  ])

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_ecs_service" "grafana" {
  name            = "${local.name_prefix}-grafana"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.grafana.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  health_check_grace_period_seconds = 60

  load_balancer {
    target_group_arn = aws_lb_target_group.grafana.arn
    container_name   = "grafana"
    container_port   = 3000
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id # Tasks move to private subnets
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false # No longer need public IPs
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [aws_lb_listener.http]
}




# ─────────────────────────────────────────────────────────────────────────────
# Application Load Balancer (ALB)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_lb" "main" {
  name               = "${local.name_prefix}-v2"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id # ALB lives in public subnets

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_lb_target_group" "dagster" {
  name        = "${local.name_prefix}-dagster-v2"
  port        = 3000
  protocol    = "HTTP"
  vpc_id      = aws_vpc.main.id
  target_type = "ip"

  health_check {
    path                = "/server_info"
    healthy_threshold   = 2
    unhealthy_threshold = 10
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_lb_target_group" "grafana" {
  name        = "${local.name_prefix}-grafana-v2"
  port        = 3000
  protocol    = "HTTP"
  vpc_id      = aws_vpc.main.id
  target_type = "ip"

  health_check {
    path                = "/grafana/api/health"
    healthy_threshold   = 2
    unhealthy_threshold = 10
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = "80"
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.dagster.arn
  }
}

resource "aws_lb_listener_rule" "grafana" {
  listener_arn = aws_lb_listener.http.arn
  priority     = 10

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.grafana.arn
  }

  condition {
    path_pattern {
      values = ["/grafana*"]
    }
  }
}


# ─────────────────────────────────────────────────────────────────────────────
# Security Groups
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_security_group" "alb" {
  name_prefix = "${local.name_prefix}-alb-"
  vpc_id      = aws_vpc.main.id

  # Public HTTP access
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

resource "aws_ecr_repository" "grafana" {
  name                 = "${local.name_prefix}-grafana"
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
  retention_in_days = 30
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "grafana" {
  name              = "/ecs/${local.name_prefix}-grafana"
  retention_in_days = 30
  tags              = local.common_tags
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

resource "aws_secretsmanager_secret" "dagster_pg" {
  name = "${local.name_prefix}/dagster-pg"
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
          aws_secretsmanager_secret.dagster_pg.arn,
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
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      },
    ]
  })
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

output "s3_bucket" {
  value = aws_s3_bucket.data.id
}

output "alb_dns_name" {
  value = "http://${aws_lb.main.dns_name}"
}

output "nat_static_ip" {
  value = aws_eip.nat.public_ip
}
