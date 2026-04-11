# CLAUDE.md — Trading Analysis Project

## Build & Development Commands
- **Local Install**: `pip install -e ".[dev]"`
- **Run Dagster UI**: `DAGSTER_HOME=$(pwd)/local_home dagster-webserver -m trading_dagster`
- **Run Unit Tests**: `pytest tests/`
- **Lint Code**: `black . && flake8 .` (if installed)

## Infrastructure & Deployment
- **Region**: `ap-northeast-1` (Tokyo)
- **Terraform**: Located in `infra/terraform/`
  - `terraform init`
  - `terraform apply -var="github_repo=williamwxz/trading-analysis"`
- **CI/CD**: Managed by **AWS CodePipeline**
  - Triggers automatically on push to `main`
  - Stages: Source → Test (pytest) → Build (Docker) → Deploy (Terraform + ECS)

## Naming Conventions
- **Project Name**: `trading-analysis`
- **Environment**: Single environment (no suffix)
- **S3 Buckets**: `trading-analysis-data-v2`, `trading-analysis-pipeline-v2`
- **ECR Repos**: `trading-analysis-dagster`, `trading-analysis-grafana`

## Technical Context
- **Orchestration**: Dagster 1.11 (Declarative Automation)
- **Database**: ClickHouse Cloud (Analytics)
- **Networking**: VPC with NAT Gateway (Static IP: `18.182.133.240`)
- **Grafana**: Anonymous Admin enabled at `/grafana`
