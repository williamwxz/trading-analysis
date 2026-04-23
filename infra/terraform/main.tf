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

  # NAT traffic from private subnet
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_subnet.private.cidr_block]
  }

  # Public access to Dagster UI (iptables DNAT from :3000 to ECS task :3000)
  ingress {
    from_port   = 3000
    to_port     = 3000
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
dnf install -y aws-cli jq

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
[ "$CURRENT_IP" = "$PRIVATE_IP" ] && exit 0

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
echo "* * * * * root /usr/local/bin/update-dagster-dnat.sh >> /var/log/dagster-dnat.log 2>&1" > /etc/cron.d/dagster-dnat
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
    path = "/dagster-storage"
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
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "dagster-webserver"
      image     = "${aws_ecr_repository.dagster.repository_url}:latest"
      essential = true
      command   = ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]

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
    },
    {
      name      = "dagster-code-server"
      image     = "${aws_ecr_repository.dagster.repository_url}:latest"
      essential = true
      command   = ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4266", "-m", "trading_dagster", "--heartbeat-timeout", "3600"]

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
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
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
}



# ─────────────────────────────────────────────────────────────────────────────
# Security Groups
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_security_group" "ecs_tasks" {
  name_prefix = "${local.name_prefix}-ecs-"
  vpc_id      = aws_vpc.main.id

  # Allow port 3000 only from the NAT instance (nginx reverse proxy)
  ingress {
    from_port       = 3000
    to_port         = 3000
    protocol        = "tcp"
    security_groups = [aws_security_group.nat.id]
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



# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Logs
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "dagster" {
  name              = "/ecs/${local.name_prefix}-dagster"
  retention_in_days = 3
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

output "dagster_url" {
  value       = "http://${aws_eip.nat.public_ip}"
  description = "Dagster UI — static IP via NAT instance nginx proxy"
}

output "nat_static_ip" {
  value       = aws_eip.nat.public_ip
  description = "Static IP (inbound Dagster UI + outbound ClickHouse allowlist)"
}

output "grafana_cloudwatch_secret_arn" {
  value       = aws_secretsmanager_secret.grafana_cloudwatch.arn
  description = "ARN of the Secrets Manager secret holding Grafana CloudWatch datasource credentials"
}
