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

echo "==> Building sandbox image..."
docker build -t airstore-default-sandbox:${TAG} -f docker/Dockerfile.sandbox .

echo "==> Tagging image..."
docker tag airstore-default-sandbox:${TAG} ${REPO}:${TAG}

echo "==> Pushing to ${REPO}:${TAG}..."
docker push ${REPO}:${TAG}

echo "==> Done! Image pushed to ${REPO}:${TAG}"
