# Project Context: Trading Analysis

## Infrastructure & Deployment

- **Primary Region**: `ap-northeast-1` (Tokyo)
- **CI/CD Region**: `ap-northeast-1` (Tokyo)
- **Native AWS CI/CD**:
  - **Source**: GitHub (via AWS CodeStar Connections)
  - **Connection ARN**: `arn:aws:codeconnections:ap-northeast-1:339163283253:connection/ef9b3b42-c3f2-4a90-98a0-2c4a2869ade2`
  - **Pipeline**: AWS CodePipeline
  - **Compute**: AWS CodeBuild (Native)
  - **Environment**: Single environment (simplified naming, no 'production' suffix)

## Regional Configuration

| Resource | Region |
| --- | --- |
| Terraform Backend (S3) | `ap-northeast-1` |
| ECR Repository | `ap-northeast-1` |
| ECS Cluster (Fargate) | `ap-northeast-1` |
| MSK Serverless | `ap-northeast-1` |
| S3 Data Bucket | `ap-northeast-1` |
| CI/CD Resources | `ap-northeast-1` |

## Deployment Workflow

1.  **CodePush**: Push to GitHub `main` branch.
2.  **CodePipeline**: Automatically triggers in Tokyo.
3.  **Test Stage**: Runs `pytest`.
4.  **Build Stage**: Docker build and push to ECR (`ap-northeast-1`).
5.  **Deploy Stage**: 
    - Terraform apply (managed infrastructure).
    - ECS Service update (force new deployment).
    - Database schema sync.

## S3 Naming (Bypassing DNS Propagation)

- Data Bucket: `trading-analysis-data-v2`
- Pipeline Artifacts: `trading-analysis-pipeline-v2`
