# Redpanda Throughput Metrics via CloudWatch Metric Filter

**Date:** 2026-05-02

## Goal

Surface Redpanda message throughput (messages/min) in Grafana Cloud with zero new infrastructure — no sidecars, no Prometheus scraping.

## Approach

The `pnl-consumer` already emits a structured log line per consumed message:

```
INFO Received BTCUSDT close=95000.00 ts=2026-05-02T00:01:00 batch=3/10
```

These land in CloudWatch log group `/ecs/trading-analysis` under the `pnl-consumer` stream prefix.

A **CloudWatch Metric Filter** on this log group matches the `Received` pattern and increments a counter metric. Grafana Cloud reads it via the existing CloudWatch datasource.

## Changes

### 1. Terraform — `infra/terraform/main.tf`

Add one `aws_cloudwatch_metric_filter` resource:

```hcl
resource "aws_cloudwatch_metric_filter" "pnl_messages_received" {
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
```

> The pnl-consumer uses Python's `logging` module with the `awslogs` driver, which emits plain-text log lines (not JSON). The pattern `"Received "` matches the literal string in every per-message log line.

### 2. Grafana Dashboard (optional)

Add a panel to an existing dashboard (e.g. `strategy-pnl-l1-instance.json`) or create a new `streaming-health.json` dashboard querying:

- **Datasource:** CloudWatch (existing)
- **Namespace:** `trading-analysis`
- **Metric:** `MessagesReceived`
- **Stat:** `Sum`
- **Period:** 60s
- **Visualization:** Time series, showing messages/min

## What Does NOT Change

- ECS task definitions (no sidecar, no resource changes)
- IAM roles (existing CloudWatch datasource IAM user already has `cloudwatch:GetMetricData`)
- pnl-consumer application code

## Cost

- CloudWatch custom metric: ~$0.30/month
- No additional log ingestion cost (logs already flowing)

## Verification

After `terraform apply`, wait ~5 minutes for messages to flow, then:

```bash
aws cloudwatch get-metric-statistics \
  --namespace trading-analysis \
  --metric-name MessagesReceived \
  --statistics Sum \
  --period 60 \
  --start-time $(date -u -v-10M +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --region ap-northeast-1
```
