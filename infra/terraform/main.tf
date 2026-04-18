# ============================================================================
# trading-analysis Infrastructure — VPC + NAT + ECS + EFS
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
  source_dest_check           = false
  associate_public_ip_address = true

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
# EFS — SQLite storage for Dagster metadata
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_efs_file_system" "dagster" {
  encrypted = true

  lifecycle_policy {
    transition_to_ia = "AFTER_30_DAYS"
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-dagster-efs" })
}

resource "aws_security_group" "efs" {
  name_prefix = "${local.name_prefix}-efs-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 2049
    to_port         = 2049
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

resource "aws_efs_mount_target" "dagster" {
  file_system_id  = aws_efs_file_system.dagster.id
  subnet_id       = aws_subnet.private.id
  security_groups = [aws_security_group.efs.id]
}

resource "aws_efs_access_point" "dagster" {
  file_system_id = aws_efs_file_system.dagster.id

  posix_user {
    uid = 1000
    gid = 1000
  }

  root_directory {
    path = "/dagster-home"
    creation_info {
      owner_uid   = 1000
      owner_gid   = 1000
      permissions = "755"
    }
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-dagster-ap" })
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
  cpu                      = 1024
  memory                   = 2048
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

      mountPoints = [
        {
          sourceVolume  = "dagster-efs"
          containerPath = "/app"
          readOnly      = false
        }
      ]

      environment = [
        { name = "DAGSTER_HOME",      value = "/app" },
        { name = "CLICKHOUSE_USER",   value = "dev_ro3" },
        { name = "CLICKHOUSE_PORT",   value = "8443" },
        { name = "CLICKHOUSE_SECURE", value = "true" },
      ]

      secrets = [
        { name = "CLICKHOUSE_HOST",     valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
        { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
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

      mountPoints = [
        {
          sourceVolume  = "dagster-efs"
          containerPath = "/app"
          readOnly      = false
        }
      ]

      environment = [
        { name = "DAGSTER_HOME",      value = "/app" },
        { name = "CLICKHOUSE_USER",   value = "dev_ro3" },
        { name = "CLICKHOUSE_PORT",   value = "8443" },
        { name = "CLICKHOUSE_SECURE", value = "true" },
      ]

      secrets = [
        { name = "CLICKHOUSE_HOST",     valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
        { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
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

  volume {
    name = "dagster-efs"

    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.dagster.id
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = aws_efs_access_point.dagster.id
        iam             = "ENABLED"
      }
    }
  }

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
    subnets          = [aws_subnet.private.id]
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
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
    subnets          = [aws_subnet.private.id]
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
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
      {
        # DefaultRunLauncher introspects the current ECS task definition at startup
        Effect   = "Allow"
        Action   = ["ecs:DescribeTaskDefinition"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "elasticfilesystem:ClientMount",
          "elasticfilesystem:ClientWrite",
          "elasticfilesystem:ClientRootAccess",
        ]
        Resource = aws_efs_file_system.dagster.arn
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
  value       = aws_eip.nat.public_ip
  description = "Static outbound IP — add this to ClickHouse Cloud allowlist"
}
