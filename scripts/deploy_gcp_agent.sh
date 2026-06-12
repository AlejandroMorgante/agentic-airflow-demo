#!/usr/bin/env bash
# Build the GCP Gemini Agent Platform agent image and push to Artifact Registry.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PROJECT_ID="${GCP_PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${GCP_REGION:-us-central1}"
REPOSITORY="${GCP_ARTIFACT_REPOSITORY:-airflow-agent-platform}"
IMAGE_NAME="${GCP_AGENT_IMAGE_NAME:-airflow-troubleshooting-agent}"
TAG="${GCP_AGENT_IMAGE_TAG:-gcp-$(date +%Y%m%d%H%M%S)}"

: "${PROJECT_ID:?GCP project is required. Set GCP_PROJECT_ID or run gcloud config set project.}"

REGISTRY="${REGION}-docker.pkg.dev"
IMAGE_URI="${REGISTRY}/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${TAG}"

echo "==> Configuring Docker auth for ${REGISTRY}"
gcloud auth configure-docker "${REGISTRY}" --quiet

echo "==> Building and pushing linux/amd64 image -> ${IMAGE_URI}"
docker buildx build \
  --platform linux/amd64 \
  -f "${REPO_ROOT}/agent/gcp_gemini_agent_platform/Dockerfile" \
  -t "${IMAGE_URI}" \
  --push \
  "${REPO_ROOT}/agent"

echo ""
echo "Image pushed: ${IMAGE_URI}"
