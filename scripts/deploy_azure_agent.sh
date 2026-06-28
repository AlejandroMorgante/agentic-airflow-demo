#!/usr/bin/env bash
# Build the Azure AI Foundry Hosted Agent image and push to Azure Container Registry.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AIRFLOW_ENV="$REPO_ROOT/airflow/.env"

if [[ -f "$AIRFLOW_ENV" ]]; then
  # shellcheck disable=SC1091
  source "$AIRFLOW_ENV"
fi

ACR_NAME="${AZURE_CONTAINER_REGISTRY_NAME:-}"
IMAGE_URI="${AIRFLOW_VAR_AZURE_AI_AGENTS_CONTAINER_IMAGE:-}"
IMAGE_NAME="${AZURE_AI_AGENT_IMAGE_NAME:-airflow-troubleshooting-agent}"
TAG="${AZURE_AI_AGENT_IMAGE_TAG:-azure-$(date +%Y%m%d%H%M%S)}"

if [[ -z "$IMAGE_URI" ]]; then
  : "${ACR_NAME:?Set AZURE_CONTAINER_REGISTRY_NAME or AIRFLOW_VAR_AZURE_AI_AGENTS_CONTAINER_IMAGE.}"
  IMAGE_URI="${ACR_NAME}.azurecr.io/${IMAGE_NAME}:${TAG}"
else
  ACR_NAME="${ACR_NAME:-$(echo "$IMAGE_URI" | cut -d. -f1)}"
fi

echo "==> Logging in to ACR (${ACR_NAME})"
az acr login --name "$ACR_NAME"

echo "==> Building and pushing linux/amd64 image -> ${IMAGE_URI}"
docker buildx build \
  --platform linux/amd64 \
  -f "${REPO_ROOT}/agent/azure_ai_agents/Dockerfile" \
  -t "${IMAGE_URI}" \
  --push \
  "${REPO_ROOT}/agent"

echo ""
echo "Image pushed: ${IMAGE_URI}"
echo ""
echo "Next steps:"
echo "  1. Set AIRFLOW_VAR_AZURE_AI_AGENTS_CONTAINER_IMAGE=${IMAGE_URI} in airflow/.env."
echo "  2. Trigger 'azure_ai_agents_create_agent' or 'azure_ai_agents_full_lifecycle'."
