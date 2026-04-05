# ============================================================================
# AWS CodePipeline — trading-analysis CI/CD
# ============================================================================
# Pipeline lives in us-east-1 because the CodeConnections connection was
# created there. All build/deploy steps target ap-northeast-1 (Tokyo) via
# the DEPLOY_REGION environment variable in each CodeBuild project.
#
# Connection ARN (pre-existing, created outside Terraform):
#   arn:aws:codeconnections:us-east-1:339163283253:connection/31f9d517-f760-4d90-b7d8-b87cde5be67f
#
# Pipeline stages:
#   Source  → GitHub main branch (via CodeConnections)
#   Test    → pytest
#   Build   → docker build + push to ECR (ap-northeast-1)
#   Deploy  → terraform apply + ECS update + ClickHouse schema (if changed)
# ============================================================================

locals {
  connection_arn = "arn:aws:codeconnections:us-east-1:339163283253:connection/31f9d517-f760-4d90-b7d8-b87cde5be67f"
  deploy_region  = "ap-northeast-1"
  pipeline_region = "us-east-1"
}


# ─────────────────────────────────────────────────────────────────────────────
# S3 Artifact Bucket (must be in the same region as the pipeline)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "pipeline_artifacts" {
  provider = aws.us_east_1
  bucket   = "${local.name_prefix}-pipeline-artifacts"
  tags     = local.common_tags
}

resource "aws_s3_bucket_lifecycle_configuration" "pipeline_artifacts" {
  provider = aws.us_east_1
  bucket   = aws_s3_bucket.pipeline_artifacts.id

  rule {
    id     = "expire-artifacts"
    status = "Enabled"

    expiration {
      days = 14
    }
  }
}


# ─────────────────────────────────────────────────────────────────────────────
# CodeBuild — Test
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_codebuild_project" "test" {
  provider     = aws.us_east_1
  name         = "${local.name_prefix}-test"
  service_role = aws_iam_role.codebuild.arn
  tags         = local.common_tags

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type = "BUILD_GENERAL1_SMALL"
    image        = "aws/codebuild/standard:7.0"
    type         = "LINUX_CONTAINER"
  }

  source {
    type      = "CODEPIPELINE"
    buildspec = "buildspec/test.yml"
  }

  logs_config {
    cloudwatch_logs {
      group_name  = "/codebuild/${local.name_prefix}-test"
      stream_name = "build"
    }
  }
}


# ─────────────────────────────────────────────────────────────────────────────
# CodeBuild — Build (Docker image → ECR in Tokyo)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_codebuild_project" "build" {
  provider     = aws.us_east_1
  name         = "${local.name_prefix}-build"
  service_role = aws_iam_role.codebuild.arn
  tags         = local.common_tags

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type    = "BUILD_GENERAL1_SMALL"
    image           = "aws/codebuild/standard:7.0"
    type            = "LINUX_CONTAINER"
    privileged_mode = true  # Required for docker build

    environment_variable {
      name  = "DEPLOY_REGION"
      value = local.deploy_region
    }
    environment_variable {
      name  = "AWS_ACCOUNT_ID"
      value = data.aws_caller_identity.current.account_id
    }
    environment_variable {
      name  = "IMAGE_NAME"
      value = "trading-analysis-dagster"
    }
  }

  source {
    type      = "CODEPIPELINE"
    buildspec = "buildspec/build.yml"
  }

  logs_config {
    cloudwatch_logs {
      group_name  = "/codebuild/${local.name_prefix}-build"
      stream_name = "build"
    }
  }
}


# ─────────────────────────────────────────────────────────────────────────────
# CodeBuild — Deploy (Terraform + ECS update + schema)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_codebuild_project" "deploy" {
  provider     = aws.us_east_1
  name         = "${local.name_prefix}-deploy"
  service_role = aws_iam_role.codebuild.arn
  tags         = local.common_tags

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type = "BUILD_GENERAL1_SMALL"
    image        = "aws/codebuild/standard:7.0"
    type         = "LINUX_CONTAINER"

    environment_variable {
      name  = "DEPLOY_REGION"
      value = local.deploy_region
    }
    environment_variable {
      name  = "AWS_ACCOUNT_ID"
      value = data.aws_caller_identity.current.account_id
    }
    environment_variable {
      name  = "ECS_CLUSTER"
      value = "${local.name_prefix}"
    }
    environment_variable {
      name  = "ECS_SERVICE"
      value = "${local.name_prefix}-dagster"
    }
    environment_variable {
      name  = "IMAGE_NAME"
      value = "trading-analysis-dagster"
    }
    # ClickHouse credentials from Secrets Manager
    environment_variable {
      name  = "CLICKHOUSE_HOST_SECRET"
      value = aws_secretsmanager_secret.clickhouse.arn
      type  = "SECRETS_MANAGER"
    }
  }

  source {
    type      = "CODEPIPELINE"
    buildspec = "buildspec/deploy.yml"
  }

  logs_config {
    cloudwatch_logs {
      group_name  = "/codebuild/${local.name_prefix}-deploy"
      stream_name = "build"
    }
  }
}


# ─────────────────────────────────────────────────────────────────────────────
# CodePipeline
# ─────────────────────────────────────────────────────────────────────────────

data "aws_caller_identity" "current" {}

resource "aws_codepipeline" "main" {
  provider = aws.us_east_1
  name     = local.name_prefix
  role_arn = aws_iam_role.codepipeline.arn
  tags     = local.common_tags

  artifact_store {
    location = aws_s3_bucket.pipeline_artifacts.bucket
    type     = "S3"
  }

  stage {
    name = "Source"

    action {
      name             = "GitHub"
      category         = "Source"
      owner            = "AWS"
      provider         = "CodeStarSourceConnection"
      version          = "1"
      output_artifacts = ["source"]

      configuration = {
        ConnectionArn        = local.connection_arn
        FullRepositoryId     = var.github_repo          # e.g. "your-org/trading-analysis"
        BranchName           = "main"
        OutputArtifactFormat = "CODE_ZIP"
        DetectChanges        = "true"
      }
    }
  }

  stage {
    name = "Test"

    action {
      name             = "RunTests"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      version          = "1"
      input_artifacts  = ["source"]
      output_artifacts = ["tested"]

      configuration = {
        ProjectName = aws_codebuild_project.test.name
      }
    }
  }

  stage {
    name = "Build"

    action {
      name             = "BuildAndPush"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      version          = "1"
      input_artifacts  = ["source"]
      output_artifacts = ["built"]

      configuration = {
        ProjectName = aws_codebuild_project.build.name
      }
    }
  }

  stage {
    name = "Deploy"

    action {
      name            = "DeployToECS"
      category        = "Build"
      owner           = "AWS"
      provider        = "CodeBuild"
      version         = "1"
      input_artifacts = ["source", "built"]

      configuration = {
        ProjectName          = aws_codebuild_project.deploy.name
        PrimarySource        = "source"
      }
    }
  }
}


# ─────────────────────────────────────────────────────────────────────────────
# IAM — CodePipeline Role
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "codepipeline" {
  provider = aws.us_east_1
  name     = "${local.name_prefix}-codepipeline"
  tags     = local.common_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "codepipeline.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "codepipeline" {
  provider = aws.us_east_1
  name     = "codepipeline-policy"
  role     = aws_iam_role.codepipeline.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:GetBucketVersioning",
          "s3:GetObjectVersion", "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.pipeline_artifacts.arn,
          "${aws_s3_bucket.pipeline_artifacts.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["codebuild:BatchGetBuilds", "codebuild:StartBuild"]
        Resource = [
          aws_codebuild_project.test.arn,
          aws_codebuild_project.build.arn,
          aws_codebuild_project.deploy.arn,
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["codestar-connections:UseConnection"]
        Resource = local.connection_arn
      }
    ]
  })
}


# ─────────────────────────────────────────────────────────────────────────────
# IAM — CodeBuild Role (shared across all three projects)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "codebuild" {
  provider = aws.us_east_1
  name     = "${local.name_prefix}-codebuild"
  tags     = local.common_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "codebuild.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "codebuild" {
  provider = aws.us_east_1
  name     = "codebuild-policy"
  role     = aws_iam_role.codebuild.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Logs
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:us-east-1:${data.aws_caller_identity.current.account_id}:*"
      },
      # Pipeline artifact bucket
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:GetObjectVersion"]
        Resource = [
          aws_s3_bucket.pipeline_artifacts.arn,
          "${aws_s3_bucket.pipeline_artifacts.arn}/*"
        ]
      },
      # Terraform state bucket in Tokyo
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
        Resource = [
          "arn:aws:s3:::trading-analysis-tfstate",
          "arn:aws:s3:::trading-analysis-tfstate/*"
        ]
      },
      # ECR in Tokyo — push image
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload", "ecr:PutImage"
        ]
        Resource = "*"
      },
      # ECS in Tokyo — update service
      {
        Effect = "Allow"
        Action = [
          "ecs:DescribeServices", "ecs:UpdateService",
          "ecs:DescribeTaskDefinition", "ecs:RegisterTaskDefinition",
          "ecs:DescribeTasks", "ecs:ListTasks"
        ]
        Resource = "*"
      },
      # IAM PassRole — needed for ECS task definition registration
      {
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = "*"
        Condition = {
          StringLike = {
            "iam:PassedToService" = "ecs-tasks.amazonaws.com"
          }
        }
      },
      # Secrets Manager — ClickHouse credentials
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [
          aws_secretsmanager_secret.clickhouse.arn,
          aws_secretsmanager_secret.dagster_pg.arn,
        ]
      },
      # Full Terraform permissions (ECS, ECR, MSK, VPC, IAM for this project)
      {
        Effect   = "Allow"
        Action   = [
          "ec2:*", "elasticloadbalancing:*",
          "iam:GetRole", "iam:CreateRole", "iam:DeleteRole", "iam:AttachRolePolicy",
          "iam:DetachRolePolicy", "iam:PutRolePolicy", "iam:DeleteRolePolicy",
          "iam:GetRolePolicy", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
          "iam:CreateInstanceProfile", "iam:DeleteInstanceProfile",
          "iam:AddRoleToInstanceProfile", "iam:RemoveRoleFromInstanceProfile",
          "iam:GetInstanceProfile", "iam:ListInstanceProfilesForRole",
          "kafka:*", "msk:*"
        ]
        Resource = "*"
      }
    ]
  })
}


# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Log Groups for CodeBuild (in us-east-1)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "codebuild_test" {
  provider          = aws.us_east_1
  name              = "/codebuild/${local.name_prefix}-test"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "codebuild_build" {
  provider          = aws.us_east_1
  name              = "/codebuild/${local.name_prefix}-build"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "codebuild_deploy" {
  provider          = aws.us_east_1
  name              = "/codebuild/${local.name_prefix}-deploy"
  retention_in_days = 14
  tags              = local.common_tags
}


# ─────────────────────────────────────────────────────────────────────────────
# Variable: GitHub repo (e.g. "your-org/trading-analysis")
# ─────────────────────────────────────────────────────────────────────────────

variable "github_repo" {
  description = "GitHub repository in org/repo format, e.g. 'williamwxz/trading-analysis'"
  type        = string
  default     = "williamwxz/trading-analysis"
}


# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "codepipeline_name" {
  value = aws_codepipeline.main.name
}

output "codepipeline_url" {
  value = "https://us-east-1.console.aws.amazon.com/codesuite/codepipeline/pipelines/${aws_codepipeline.main.name}/view"
}
