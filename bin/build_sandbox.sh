#!/bin/bash
set -euo pipefail

# Build and push the default sandbox image to AWS ECR Public
#
# Prerequisites:
#   - AWS CLI configured with credentials (SSO or env vars)
#   - Docker installed and running
#   - airstore-runners repo at ../airstore-runners (sibling directory)

REPO="public.ecr.aws/n4e0e1y0/airstore-default-sandbox"
TAG="${1:-latest}"
PROFILE="${AWS_PROFILE:-beam-engineering-prod}"
RUNNERS_SRC="${AIRSTORE_RUNNERS_SRC:-../airstore-runners}"
STAGING_DIR=".airstore-runners"

cleanup() { rm -rf "${STAGING_DIR}"; }
trap cleanup EXIT

# Stage airstore-runners source into the build context
if [ ! -d "${RUNNERS_SRC}" ]; then
    echo "ERROR: airstore-runners not found at ${RUNNERS_SRC}"
    echo "Set AIRSTORE_RUNNERS_SRC to the correct path"
    exit 1
fi
echo "==> Staging airstore-runners from ${RUNNERS_SRC}..."
rm -rf "${STAGING_DIR}"
rsync -a --exclude='.venv' --exclude='.git' --exclude='__pycache__' \
    --exclude='*.pyc' --exclude='.mypy_cache' \
    "${RUNNERS_SRC}/" "${STAGING_DIR}/"

echo "==> Authenticating with AWS ECR Public (profile: ${PROFILE})..."
aws ecr-public get-login-password --region us-east-1 --profile "${PROFILE}" | \
    docker login --username AWS --password-stdin public.ecr.aws/n4e0e1y0

echo "==> Building and pushing sandbox image: ${REPO}:${TAG} (amd64 + arm64)..."
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    -t ${REPO}:${TAG} \
    -f docker/Dockerfile.sandbox \
    --push \
    .

echo "==> Done! ${REPO}:${TAG}"
