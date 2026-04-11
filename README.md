# Trading Analysis Platform

A simplified, automated trading analytics pipeline built with **Dagster**, **ClickHouse Cloud**, and **Grafana**. 

## Core Stack
- **Orchestrator**: Dagster (ECS Fargate)
- **Database**: ClickHouse Cloud (Analytics) + RDS Postgres (Metadata)
- **Visualization**: Grafana (L1-L5 PnL Dashboards)
- **Cloud**: AWS Tokyo (`ap-northeast-1`)
- **CI/CD**: Native AWS CodePipeline

## Key Features
- **Partitioned Data**: Daily partitioned Binance OHLCV fetching for easy historical backfills.
- **Incremental PnL**: Automated 5-minute PnL calculation with anchor-chained continuity.
- **Cost Optimized**: Reduced AWS bill by ~85% (~$91/mo) using NAT Gateway and direct ingestion.
- **Infrastructure as Code**: 100% managed via Terraform.

## Project Structure
- `trading_dagster/`: Core asset definitions and utility code.
- `infra/`: Terraform modules and Grafana provisioning.
- `schemas/`: Optimized ClickHouse Cloud schema.
- `buildspec/`: CI/CD definitions for Test, Build, and Deploy.

## Getting Started
### Local Development
1. `python -m venv venv`
2. `source venv/bin/activate`
3. `pip install -e ".[dev]"`
4. `export DAGSTER_HOME=$(pwd)/local_home`
5. `dagster-webserver -m trading_dagster`

### Deployment
Pushes to the `main` branch automatically trigger the AWS CodePipeline in Tokyo.

## Access Links
- **Dagster**: [Open Dashboard](http://trading-analysis-v2-1794112824.ap-northeast-1.elb.amazonaws.com/)
- **Grafana**: [Open PnL Charts](http://trading-analysis-v2-1794112824.ap-northeast-1.elb.amazonaws.com/grafana) (Anonymous Admin)
