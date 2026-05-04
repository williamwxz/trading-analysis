# Dagster ALB Access Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace NAT instance DNAT with an ALB so the Dagster UI is accessible at a stable HTTP URL from any browser.

**Architecture:** Add an internet-facing ALB in the existing public subnets that forwards port 80 to the Dagster webserver container on port 3000 in the private subnet. The ECS task stays in the private subnet so all outbound traffic (ClickHouse, ECR, AWS APIs) continues routing through the NAT instance's static EIP — which is allowlisted in ClickHouse Cloud.

**Tech Stack:** Terraform (AWS provider ~5.0), `infra/terraform/main.tf`

---

### Task 1: Add ALB security group

**Files:**
- Modify: `infra/terraform/main.tf` — add `aws_security_group.alb` resource after the existing `aws_security_group.ecs_tasks` block

- [ ] **Step 1: Add the ALB security group resource**

In [infra/terraform/main.tf](infra/terraform/main.tf), after the closing `}` of `resource "aws_security_group" "ecs_tasks"` (around line 478), insert:

```hcl
resource "aws_security_group" "alb" {
  name_prefix = "${local.name_prefix}-alb-"
  vpc_id      = aws_vpc.main.id

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
```

- [ ] **Step 2: Validate syntax**

```bash
cd infra/terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "feat(infra): add ALB security group for Dagster UI"
```

---

### Task 2: Add ALB, target group, and listener

**Files:**
- Modify: `infra/terraform/main.tf` — add three resources after `aws_security_group.alb`

- [ ] **Step 1: Add the ALB resource**

After the `aws_security_group.alb` resource block, insert:

```hcl
# ─────────────────────────────────────────────────────────────────────────────
# ALB — Dagster UI
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_lb" "dagster" {
  name               = "${local.name_prefix}-dagster"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id

  tags = local.common_tags
}

resource "aws_lb_target_group" "dagster" {
  name        = "${local.name_prefix}-dagster"
  port        = 3000
  protocol    = "HTTP"
  vpc_id      = aws_vpc.main.id
  target_type = "ip"

  health_check {
    path                = "/"
    protocol            = "HTTP"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 30
    timeout             = 5
    matcher             = "200-399"
  }

  tags = local.common_tags
}

resource "aws_lb_listener" "dagster_http" {
  load_balancer_arn = aws_lb.dagster.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.dagster.arn
  }

  tags = local.common_tags
}
```

- [ ] **Step 2: Validate syntax**

```bash
cd infra/terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "feat(infra): add ALB, target group, and HTTP listener for Dagster UI"
```

---

### Task 3: Update ECS task security group to accept traffic from ALB

**Files:**
- Modify: `infra/terraform/main.tf` — change the ingress rule in `aws_security_group.ecs_tasks`

- [ ] **Step 1: Replace the NAT-sourced ingress rule with ALB-sourced**

In `resource "aws_security_group" "ecs_tasks"`, find and replace the existing port 3000 ingress block:

Old (lines ~457-463):
```hcl
  # Allow port 3000 only from the NAT instance (nginx reverse proxy)
  ingress {
    from_port       = 3000
    to_port         = 3000
    protocol        = "tcp"
    security_groups = [aws_security_group.nat.id]
  }
```

New:
```hcl
  # Allow port 3000 only from the ALB
  ingress {
    from_port       = 3000
    to_port         = 3000
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }
```

- [ ] **Step 2: Validate syntax**

```bash
cd infra/terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "feat(infra): allow Dagster port 3000 from ALB SG instead of NAT SG"
```

---

### Task 4: Attach ALB target group to ECS service and update output

**Files:**
- Modify: `infra/terraform/main.tf` — update `aws_ecs_service.dagster` and `output "dagster_url"`

- [ ] **Step 1: Add load_balancer block and health check grace period to the ECS service**

In `resource "aws_ecs_service" "dagster"`, add after the `network_configuration` block (before `tags`):

```hcl
  load_balancer {
    target_group_arn = aws_lb_target_group.dagster.arn
    container_name   = "dagster-webserver"
    container_port   = 3000
  }

  health_check_grace_period_seconds = 60
```

The full `aws_ecs_service.dagster` resource should now look like:

```hcl
resource "aws_ecs_service" "dagster" {
  name            = "${local.name_prefix}-dagster"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.dagster.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
  }

  network_configuration {
    subnets          = [aws_subnet.private.id]
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.dagster.arn
    container_name   = "dagster-webserver"
    container_port   = 3000
  }

  health_check_grace_period_seconds = 60

  tags = local.common_tags
}
```

- [ ] **Step 2: Update the dagster_url output**

Find `output "dagster_url"` (near end of file) and change its value:

Old:
```hcl
output "dagster_url" {
  value       = "http://${aws_eip.nat.public_ip}"
  description = "Dagster UI - static IP via NAT instance nginx proxy"
}
```

New:
```hcl
output "dagster_url" {
  value       = "http://${aws_lb.dagster.dns_name}"
  description = "Dagster UI - ALB DNS name"
}
```

- [ ] **Step 3: Validate syntax**

```bash
cd infra/terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 4: Run terraform plan and verify expected changes**

```bash
cd infra/terraform && terraform plan
```

Expected additions (roughly):
```
+ aws_security_group.alb
+ aws_lb.dagster
+ aws_lb_target_group.dagster
+ aws_lb_listener.dagster_http
~ aws_security_group.ecs_tasks  (ingress rule change)
~ aws_ecs_service.dagster        (load_balancer + grace period added)
~ output.dagster_url             (value change)
```

Expected: **no destructions** of existing resources (NAT instance, EIP, subnets, task definition all unchanged).

- [ ] **Step 5: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "feat(infra): attach ALB to Dagster ECS service, update dagster_url output"
```

---

### Task 5: Deploy and verify

**Files:** None (deployment only)

- [ ] **Step 1: Push to main to trigger CI/CD**

```bash
git push origin main
```

CI/CD pipeline in `.github/workflows/ci-cd.yml` will run terraform apply automatically.

- [ ] **Step 2: Get the ALB DNS name after apply**

```bash
cd infra/terraform && terraform output dagster_url
```

Expected output: `"http://trading-analysis-dagster-<id>.ap-northeast-1.elb.amazonaws.com"`

- [ ] **Step 3: Wait for ECS service to stabilize (~2-3 minutes)**

The ECS service will be force-replaced because `load_balancer` cannot be updated in-place on an existing service — Terraform will destroy and recreate `aws_ecs_service.dagster`. This is expected. The new task must register with the ALB target group and pass 2 consecutive health checks (GET / returning 200-399) before traffic is routed.

Check ECS service status:
```bash
aws ecs describe-services \
  --cluster trading-analysis \
  --services trading-analysis-dagster \
  --region ap-northeast-1 \
  --query 'services[0].{status:status,running:runningCount,desired:desiredCount,deployments:deployments[*].{status:status,running:runningCount}}'
```

Expected: `runningCount: 1`, deployment status `PRIMARY`.

- [ ] **Step 4: Verify Dagster UI is accessible**

Open `http://<alb-dns-name>` in a browser. Expected: Dagster UI loads, showing the asset catalog or run history.

- [ ] **Step 5: Verify ClickHouse connectivity still works**

In the Dagster UI, manually trigger the `clickhouse_connectivity_check` asset. Expected: materializes successfully (green). This confirms outbound traffic is still routing through the NAT instance EIP to ClickHouse.
