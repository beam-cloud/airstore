#!/bin/bash
set -euo pipefail

# Build and push the default sandbox image to AWS ECR Public
#
# Prerequisites:
#   - AWS CLI configured with credentials
#   - Docker installed and running

REPO="public.ecr.aws/n4e0e1y0/airstore-default-sandbox"
TAG="${1:-latest}"

echo "==> Authenticating with AWS ECR Public..."
aws ecr-public get-login-password --region us-east-1 | \
    docker login --username AWS --password-stdin public.ecr.aws/n4e0e1y0

echo "==> Building and pushing multi-arch sandbox image (amd64 + arm64)..."
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    -t ${REPO}:${TAG} \
    -f docker/Dockerfile.sandbox \
    --push \
    .

echo "==> Done! Image pushed to ${REPO}:${TAG} (amd64 + arm64)"
