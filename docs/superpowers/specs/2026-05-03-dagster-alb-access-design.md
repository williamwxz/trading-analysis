# Design: ALB Access for Dagster UI

**Date:** 2026-05-03  
**Status:** Approved

## Problem

Dagster UI is currently accessed via SSM port-forwarding through the NAT instance. This is operationally painful — requires AWS CLI + session-manager-plugin, a script, and port availability. The NAT instance uses iptables DNAT to forward port 3000 to the ECS task's private IP, updated by a cron job every 10 seconds. Any ECS task restart causes a brief gap.

## Goal

Replace the inbound access mechanism with an Application Load Balancer so the Dagster UI is accessible at a stable HTTP URL from any browser, with no local tooling required.

## Constraints

- ECS tasks must continue routing outbound traffic through the NAT instance (its EIP is allowlisted in ClickHouse Cloud)
- Plain HTTP (no TLS, no custom domain) — simplest viable solution
- Single Terraform file (`infra/terraform/main.tf`) — no new files

## Architecture

### Traffic Flow

```
Internet (any IP)
  → ALB (internet-facing, public subnets 10.0.1.0/24 + 10.0.2.0/24, port 80)
  → Target Group (HTTP, port 3000, health check GET /)
  → dagster-webserver container (private subnet 10.0.10.0/24, port 3000)

ECS task outbound (ClickHouse, ECR, AWS APIs, Secrets Manager)
  → aws_route_table.private (0.0.0.0/0 → NAT instance ENI)  [UNCHANGED]
  → NAT instance EIP (static — ClickHouse allowlist)  [UNCHANGED]
  → Internet
```

The ECS task remains in the private subnet. Its outbound route table is not modified. ClickHouse sees the same static EIP as before.

## New Terraform Resources

| Resource | Type | Details |
|---|---|---|
| `aws_security_group.alb` | Security group | Inbound: 0.0.0.0/0 TCP 80; Outbound: all |
| `aws_lb.dagster` | ALB | Internet-facing, subnets: `aws_subnet.public[*]` |
| `aws_lb_target_group.dagster` | Target group | HTTP, port 3000, health check `GET /`, healthy threshold 2, interval 30s |
| `aws_lb_listener.dagster_http` | Listener | Port 80 → forward to `aws_lb_target_group.dagster` |

## Changes to Existing Resources

### `aws_security_group.ecs_tasks`
- **Remove** ingress rule: port 3000 from `aws_security_group.nat`
- **Add** ingress rule: port 3000 from `aws_security_group.alb`

### `aws_ecs_service.dagster`
- **Add** `load_balancer` block: target group = `aws_lb_target_group.dagster`, container = `dagster-webserver`, container_port = 3000
- **Add** `health_check_grace_period_seconds = 60` — Dagster webserver takes ~20-30s to start; without this ECS will kill the task before it's ready

### `output "dagster_url"`
- Change value from `"http://${aws_eip.nat.public_ip}"` to `"http://${aws_lb.dagster.dns_name}"`

## What Is Not Changed

- `aws_instance.nat` — stays as-is (outbound NAT for ECS tasks)
- `aws_eip.nat` — stays as-is (ClickHouse allowlist IP)
- `aws_security_group.nat` — stays as-is (the port 3000 ingress rule becomes unused but is harmless)
- NAT instance user_data / iptables DNAT cron — stays as-is (becomes unused but harmless)
- ECS task definition, IAM roles, VPC endpoints, subnets — all unchanged

## Cost Impact

- ALB: ~$16/month (LCU charges negligible for low-traffic internal UI)
- NAT instance (t4g.nano): unchanged at ~$3/month

## Deployment

Standard `terraform apply` via existing CI/CD pipeline (push to `main`). No manual steps required. The ECS service update requires the ALB target group to exist before the service references it — Terraform dependency graph handles this automatically via `aws_lb_target_group.dagster.arn` reference.
