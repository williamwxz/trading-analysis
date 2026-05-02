# Redpanda Throughput Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface pnl-consumer message throughput (messages/min) as a CloudWatch metric, visible in Grafana Cloud.

**Architecture:** A CloudWatch Metric Filter on the existing `/ecs/trading-analysis` log group matches the `"Received "` string already emitted by the pnl-consumer on every consumed message. The filter increments a `MessagesReceived` counter in the `trading-analysis` CloudWatch namespace. A new Grafana dashboard (`streaming-health.json`) queries that metric via the existing CloudWatch datasource and displays a time series.

**Tech Stack:** Terraform (aws provider ~5.0), Grafana Cloud dashboard JSON (schemaVersion 39), AWS CloudWatch

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `infra/terraform/main.tf` | Modify (append) | Add `aws_cloudwatch_metric_filter` resource |
| `infra/grafana/dashboards/streaming-health.json` | Create | Grafana dashboard showing MessagesReceived time series |

---

### Task 1: Add CloudWatch Metric Filter in Terraform

**Files:**
- Modify: `infra/terraform/main.tf` (append after the CloudWatch Logs section, around line 586)

- [ ] **Step 1: Append the metric filter resource to main.tf**

Open `infra/terraform/main.tf`. After the `aws_cloudwatch_log_group.streaming` resource (around line 586), append:

```hcl
# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Metric Filter — pnl-consumer throughput
# ─────────────────────────────────────────────────────────────────────────────

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

- [ ] **Step 2: Validate Terraform config**

```bash
cd infra/terraform && terraform init -reconfigure && terraform validate
```

Expected output:
```
Success! The configuration is valid.
```

- [ ] **Step 3: Preview the plan**

```bash
terraform plan
```

Expected: exactly 1 resource to add — `aws_cloudwatch_metric_filter.pnl_messages_received`. No other changes.

- [ ] **Step 4: Apply**

```bash
terraform apply
```

Type `yes` when prompted. Expected output includes:
```
aws_cloudwatch_metric_filter.pnl_messages_received: Creating...
aws_cloudwatch_metric_filter.pnl_messages_received: Creation complete
Apply complete! Resources: 1 added, 0 changed, 0 destroyed.
```

- [ ] **Step 5: Commit**

```bash
cd ../..
git add infra/terraform/main.tf
git commit -m "feat: add CloudWatch metric filter for pnl-consumer throughput"
```

---

### Task 2: Create Grafana Streaming Health Dashboard

**Files:**
- Create: `infra/grafana/dashboards/streaming-health.json`

- [ ] **Step 1: Create the dashboard JSON file**

Create `infra/grafana/dashboards/streaming-health.json` with this content:

```json
{
  "uid": "streaming-health",
  "title": "Streaming — Health",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "1m",
  "time": {
    "from": "now-1h",
    "to": "now"
  },
  "timepicker": {},
  "panels": [
    {
      "id": 1,
      "type": "timeseries",
      "title": "Messages Received / min",
      "description": "Count of Redpanda messages consumed by pnl-consumer per minute, derived from CloudWatch log metric filter on the 'Received ' log pattern.",
      "gridPos": {
        "x": 0,
        "y": 0,
        "w": 24,
        "h": 8
      },
      "datasource": {
        "type": "cloudwatch",
        "uid": "cloudwatch"
      },
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "displayName": "msg/min",
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "lineWidth": 2,
            "fillOpacity": 10,
            "gradientMode": "none",
            "spanNulls": false
          }
        }
      },
      "options": {
        "tooltip": {
          "mode": "single",
          "sort": "none"
        },
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      },
      "targets": [
        {
          "refId": "A",
          "datasource": {
            "type": "cloudwatch",
            "uid": "cloudwatch"
          },
          "region": "ap-northeast-1",
          "namespace": "trading-analysis",
          "metricName": "MessagesReceived",
          "statistics": ["Sum"],
          "period": "60",
          "matchExact": true,
          "dimensions": {}
        }
      ]
    }
  ]
}
```

- [ ] **Step 2: Commit**

```bash
git add infra/grafana/dashboards/streaming-health.json
git commit -m "feat: add streaming health Grafana dashboard for message throughput"
```

---

### Task 3: Verify End-to-End

- [ ] **Step 1: Confirm metric is appearing in CloudWatch**

Wait 5–10 minutes for the pnl-consumer to produce log lines and the filter to emit data points. Then run:

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

Expected: JSON output with `Datapoints` array containing entries with `Sum` > 0. If `Datapoints` is empty, the pnl-consumer may be idle — check logs:

```bash
aws logs filter-log-events \
  --log-group-name /ecs/trading-analysis \
  --log-stream-name-prefix pnl-consumer \
  --filter-pattern "Received" \
  --region ap-northeast-1 \
  --limit 5
```

- [ ] **Step 2: Confirm dashboard appears in Grafana Cloud**

The CI/CD pipeline (`ci-cd.yml`) automatically deploys dashboards on push to `main`. After the GitHub Actions workflow completes, open your Grafana Cloud URL and navigate to Dashboards → search "Streaming". The "Streaming — Health" dashboard should appear with the time series panel.

If you want to verify before CI runs, push to main:

```bash
git push origin main
```

Then watch the `deploy-grafana-cloud` job in GitHub Actions.
