# ============================================================================
# AWS CodePipeline — trading-analysis CI/CD
# ============================================================================
# Everything now consolidated in ap-northeast-1 (Tokyo).
#
# Connection ARN (pre-existing, created in Tokyo):
#   arn:aws:codeconnections:ap-northeast-1:339163283253:connection/ef9b3b42-c3f2-4a90-98a0-2c4a2869ade2
# ============================================================================

locals {
  connection_arn  = "arn:aws:codeconnections:ap-northeast-1:339163283253:connection/ef9b3b42-c3f2-4a90-98a0-2c4a2869ade2"
  deploy_region   = "ap-northeast-1"
  pipeline_region = "ap-northeast-1"
}


# ─────────────────────────────────────────────────────────────────────────────
# S3 Artifact Bucket
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "pipeline_artifacts" {
  bucket   = "${local.name_prefix}-pipeline-v2"
  tags     = local.common_tags
}

resource "aws_s3_bucket_lifecycle_configuration" "pipeline_artifacts" {
  bucket   = aws_s3_bucket.pipeline_artifacts.id

  rule {
    id     = "expire-artifacts"
    status = "Enabled"

    filter {
      prefix = ""
    }

    expiration {
      days = 14
    }
  }
}


# ─────────────────────────────────────────────────────────────────────────────
# CodeBuild — Test
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_codebuild_project" "test" {
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
# CodeBuild — Build (Docker image → ECR)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_codebuild_project" "build" {
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
    privileged_mode = true

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
# CodeBuild — Deploy
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_codebuild_project" "deploy" {
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
        FullRepositoryId     = var.github_repo
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
# IAM — CodeBuild Role
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "codebuild" {
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
  name     = "codebuild-policy"
  role     = aws_iam_role.codebuild.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "logs:*",
          "s3:*",
          "ecr:*",
          "ecs:*",
          "rds:*",
          "secretsmanager:*",
          "iam:GetRole",
          "iam:PassRole",
          "kafka:*",
          "msk:*",
          "ec2:*"
        ]
        Resource = "*"
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
# CloudWatch Log Groups for CodeBuild
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "codebuild_test" {
  name              = "/codebuild/${local.name_prefix}-test"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "codebuild_build" {
  name              = "/codebuild/${local.name_prefix}-build"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "codebuild_deploy" {
  name              = "/codebuild/${local.name_prefix}-deploy"
  retention_in_days = 14
  tags              = local.common_tags
}


# ─────────────────────────────────────────────────────────────────────────────
# Variable: GitHub repo
# ─────────────────────────────────────────────────────────────────────────────

variable "github_repo" {
  description = "GitHub repository in org/repo format"
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
  value = "https://${local.deploy_region}.console.aws.amazon.com/codesuite/codepipeline/pipelines/${aws_codepipeline.main.name}/view"
}
