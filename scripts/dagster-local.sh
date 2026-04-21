#!/bin/bash
# dagster-local.sh — forward Dagster UI to localhost:3000 via SSM port forwarding
#
# Usage: ./scripts/dagster-local.sh
# Then open: http://localhost:3000
#
# Requires: aws cli, session-manager-plugin
#   brew install awscli session-manager-plugin
#
# Install session-manager-plugin if missing:
#   https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html

set -euo pipefail

PROFILE="AdministratorAccess-339163283253"
REGION="ap-northeast-1"
CLUSTER="trading-analysis"
SERVICE="trading-analysis-dagster"
LOCAL_PORT="${1:-3000}"

AWS="aws --region $REGION --profile $PROFILE"

echo "Looking up NAT instance..."
NAT_ID=$($AWS ec2 describe-instances \
  --filters "Name=instance-state-name,Values=running" \
            "Name=tag:Name,Values=trading-analysis-nat-instance" \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)

if [ -z "$NAT_ID" ] || [ "$NAT_ID" = "None" ]; then
  echo "ERROR: NAT instance not found"
  exit 1
fi
echo "NAT instance: $NAT_ID"

echo "Looking up ECS task private IP..."
TASK_ARN=$($AWS ecs list-tasks --cluster $CLUSTER --service-name $SERVICE \
  --query 'taskArns[0]' --output text)
TASK_IP=$($AWS ecs describe-tasks --cluster $CLUSTER --tasks "$TASK_ARN" \
  --query 'tasks[0].containers[0].networkInterfaces[0].privateIpv4Address' --output text)

if [ -z "$TASK_IP" ] || [ "$TASK_IP" = "None" ]; then
  echo "ERROR: No running Dagster task found"
  exit 1
fi
echo "Dagster task IP: $TASK_IP"

echo ""
echo "Starting SSM port-forward: localhost:$LOCAL_PORT -> $TASK_IP:3000"
echo "Open http://localhost:$LOCAL_PORT in your browser"
echo "Press Ctrl+C to stop."
echo ""

$AWS ssm start-session \
  --target "$NAT_ID" \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters "{\"host\":[\"$TASK_IP\"],\"portNumber\":[\"3000\"],\"localPortNumber\":[\"$LOCAL_PORT\"]}"
