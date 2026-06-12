#!/usr/bin/env bash
# Build the AWS AgentCore agent image and push to ECR.
# Runtime lifecycle (create/delete) is managed via the Airflow DAGs in aws_agentcore/setup.py.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AIRFLOW_ENV="$REPO_ROOT/airflow/.env"

if [[ ! -f "$AIRFLOW_ENV" ]]; then
  echo "ERROR: $AIRFLOW_ENV not found."
  exit 1
fi

# shellcheck disable=SC1091
source "$AIRFLOW_ENV"

: "${AWS_REGION:?}"

# Derive ECR registry and image URI from the Airflow Variable.
AGENT_CONTAINER_URI="${AIRFLOW_VAR_AGENT_CONTAINER_URI:?AIRFLOW_VAR_AGENT_CONTAINER_URI not set in airflow/.env}"
ECR_REGISTRY="$(echo "$AGENT_CONTAINER_URI" | cut -d/ -f1)"

echo "==> Logging in to ECR ($ECR_REGISTRY)"
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_REGISTRY"

echo "==> Building image for linux/arm64 → $AGENT_CONTAINER_URI"
docker buildx build \
  --platform linux/arm64 \
  -t "$AGENT_CONTAINER_URI" \
  --push \
  "$REPO_ROOT/agent"

echo ""
echo "Image pushed: $AGENT_CONTAINER_URI"
echo ""
echo "Next steps to update the running runtime:"
echo "  1. In Airflow UI: trigger 'agentcore_delete_runtime' to tear down the existing one."
echo "  2. Trigger 'agentcore_create_runtime' to create a fresh runtime with the new image."
echo "  3. Copy the new runtime ARN from the task XCom and update AIRFLOW_VAR_AGENT_RUNTIME_ARN in airflow/.env."
echo "  4. docker compose -f airflow/docker-compose.yml up -d  (to pick up the new ARN)"
