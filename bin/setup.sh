#!/usr/bin/env bash

# ----------------------------------------------
# This script sets up the development environment for Airstore.
# It installs kubectl, kustomize, stern, helm, okteto, and k3d.
# ----------------------------------------------

set +x
set -euo pipefail

os=$(uname -s | tr '[:upper:]' '[:lower:]')
if [ "$(uname -m)" = "arm64" ] || [ "$(uname -m)" = "aarch64" ]; then
  arch="arm64"
elif [ "$(uname -m)" = "x86_64" ]; then
  arch="amd64"
fi

k8s_version=$(curl -sSfL https://cdn.dl.k8s.io/release/stable-1.29.txt)
k3d_version="5.8.1"
kustomize_version="5.6.0"
stern_version="1.32.0"
helm_version="3.17.0"
okteto_version="3.3.1"

function err() {
  echo -e "\033[0;31m$1\033[0m"
}
function success() {
  echo -e "\033[0;32m$1\033[0m"
}
function check_status() {
  if [ $? -eq 0 ]; then
    success "Installed successfully"
  else
    err "Installation failed"
  fi
  echo ""
}

echo "=> Installing kubectl ${k8s_version}"
curl -sSfL "https://dl.k8s.io/release/${k8s_version}/bin/${os}/${arch}/kubectl" > /usr/local/bin/kubectl
chmod +x /usr/local/bin/kubectl
check_status

echo "=> Installing kustomize ${kustomize_version}"
curl -sSfL https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv${kustomize_version}/kustomize_v${kustomize_version}_${os}_${arch}.tar.gz | tar -xz -C /usr/local/bin kustomize
chmod +x /usr/local/bin/kustomize
check_status

echo "=> Installing stern ${stern_version}"
curl -sSfL "https://github.com/stern/stern/releases/download/v${stern_version}/stern_${stern_version}_${os}_${arch}.tar.gz" | tar -xz -C /usr/local/bin stern
chmod +x /usr/local/bin/stern
check_status

echo "=> Installing helm ${helm_version}"
curl -sSfL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | DESIRED_VERSION=v$helm_version bash
check_status

echo "=> Installing okteto ${okteto_version}"
curl -sSfL https://get.okteto.com | OKTETO_VERSION=${okteto_version} sh
check_status

echo "=> Installing k3d ${k3d_version}"
curl -sSfL https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG=v${k3d_version} bash
check_status

echo "=> Installing protobuf/gRPC tools"
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.27.1
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest
check_status

success "Setup complete! Run 'make k3d-up' to create a local cluster."
