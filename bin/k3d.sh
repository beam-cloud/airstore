#!/usr/bin/env bash

set -eu

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info() { echo -e "${GREEN}==>${NC} $1"; }
warn() { echo -e "${YELLOW}Warning:${NC} $1"; }
error() { echo -e "${RED}Error:${NC} $1"; }

k3d_up() {
  # Create required directories
  mkdir -p "$HOME/.airstore-k3d/storage"
  mkdir -p "$HOME/.airstore-k3d/registry"

  # Check if cluster already exists
  if k3d cluster list 2>/dev/null | grep -q "^airstore "; then
    info "Cluster 'airstore' already exists."
    
    # Make sure it's running
    if ! k3d cluster list | grep "^airstore " | grep -q "1/1"; then
      info "Starting existing cluster..."
      k3d cluster start airstore
    fi
    
    # Ensure kubeconfig is set
    k3d kubeconfig merge airstore --kubeconfig-switch-context
    
    # Ensure namespace exists
    kubectl create namespace airstore --dry-run=client -o yaml | kubectl apply -f -
    kubectl config set-context --current --namespace=airstore
    
    info "Using existing cluster 'airstore'"
    echo ""
    echo "Current context: $(kubectl config current-context)"
    exit 0
  fi

  # Check if registry already exists (possibly shared with other k3d clusters)
  if k3d registry list 2>/dev/null | grep -q "registry.localhost"; then
    info "Registry 'registry.localhost' already exists. Reusing it."
  fi

  # Check if port 1994 is in use
  if lsof -i :1994 >/dev/null 2>&1; then
    warn "Port 1994 is in use. You may need to stop the process using it."
    echo "Run: lsof -i :1994"
  fi

  # Check if port 5001 is in use but registry doesn't exist (something else using the port)
  if lsof -i :5001 >/dev/null 2>&1; then
    if ! k3d registry list 2>/dev/null | grep -q "registry.localhost"; then
      warn "Port 5001 is in use by something other than k3d registry."
      echo "Run: lsof -i :5001"
    fi
  fi

  info "Creating cluster 'airstore'..."
  
  # If registry already exists (from beta9), create cluster without registry
  if k3d registry list 2>/dev/null | grep -q "registry.localhost"; then
    # Create a temporary config without registry creation
    TMP_CONFIG=$(mktemp)
    grep -v "create:" hack/k3d.yaml | grep -v "name: registry.localhost" | \
      grep -v "hostPort:" | grep -v '\.airstore-k3d/registry' > "$TMP_CONFIG"
    
    # Add registry use instead of create
    cat >> "$TMP_CONFIG" << 'EOF'
registries:
  use:
    - k3d-registry.localhost:5000
EOF
    
    k3d cluster create --config "$TMP_CONFIG"
    rm "$TMP_CONFIG"
  else
    k3d cluster create --config hack/k3d.yaml
  fi
  
  # Create namespace
  kubectl create namespace airstore --dry-run=client -o yaml | kubectl apply -f -
  kubectl config set-context --current --namespace=airstore
  
  # Try to set okteto context, but don't fail if okteto isn't installed
  if command -v okteto &> /dev/null; then
    okteto context use k3d-airstore --namespace airstore 2>/dev/null || true
  fi
  
  echo ""
  info "Cluster 'airstore' is ready!"
  echo ""
  echo "Registry: push to localhost:5001, pull from registry.localhost:5000"
  echo "Current context: $(kubectl config current-context)"
}

k3d_down() {
  # Delete cluster
  if k3d cluster list 2>/dev/null | grep -q "^airstore "; then
    k3d cluster delete airstore
    info "Cluster 'airstore' deleted."
  else
    warn "Cluster 'airstore' not found."
  fi
  
  # Only delete registry if no other k3d clusters are using it
  remaining_clusters=$(k3d cluster list 2>/dev/null | grep -v "NAME" | wc -l | tr -d ' ')
  if [ "$remaining_clusters" = "0" ]; then
    if k3d registry list 2>/dev/null | grep -q "registry.localhost"; then
      info "No other clusters using registry, deleting 'registry.localhost'..."
      k3d registry delete registry.localhost 2>/dev/null || true
    fi
  else
    info "Other k3d clusters exist, keeping shared registry."
  fi
}

case "${1:-}" in
  up)   k3d_up ;;
  down) k3d_down ;;
  *)    echo "Usage: $0 {up|down}"; exit 1 ;;
esac
