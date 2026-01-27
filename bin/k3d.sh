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
    # Ensure registry container exists and is running
    if ! docker ps -a --format '{{.Names}}' | grep -q "^k3d-registry.localhost$"; then
      warn "Registry container missing; skipping recreate."
      warn "If cluster creation fails, run: k3d registry delete registry.localhost"
    elif ! docker ps --format '{{.Names}}' | grep -q "^k3d-registry.localhost$"; then
      info "Starting registry container..."
      docker start k3d-registry.localhost >/dev/null
    fi
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

  # Clean up any stale resources from previous failed attempts
  if docker network ls --format '{{.Name}}' | grep -q "^k3d-airstore$"; then
    info "Cleaning up stale network 'k3d-airstore'..."
    docker network rm k3d-airstore 2>/dev/null || true
  fi
  if docker volume ls --format '{{.Name}}' | grep -q "^k3d-airstore-images$"; then
    info "Cleaning up stale volume 'k3d-airstore-images'..."
    docker volume rm k3d-airstore-images 2>/dev/null || true
  fi
  
  # If registry already exists, create cluster without registry creation
  if k3d registry list 2>/dev/null | grep -q "registry.localhost"; then
    # Create k3s registries config to tell k3s where to find the registry
    # Find the actual registry container name (could be k3d-registry.localhost or registry.localhost)
    REGISTRY_CONTAINER=$(docker ps --filter "publish=5001" --format '{{.Names}}' | head -1)
    if [ -z "$REGISTRY_CONTAINER" ]; then
      REGISTRY_CONTAINER="k3d-registry.localhost"
    fi
    info "Using existing registry container: $REGISTRY_CONTAINER"
    
    REGISTRIES_CONFIG="$HOME/.airstore-k3d/registries.yaml"
    cat > "$REGISTRIES_CONFIG" << EOF
mirrors:
  "registry.localhost:5000":
    endpoint:
      - "http://${REGISTRY_CONTAINER}:5000"
EOF
    
    # Create a temporary config without the registries block
    TMP_CONFIG=$(mktemp)
    awk '
      /^registries:/ {skip=1; next}
      skip {
        if ($0 ~ /^[^[:space:]]/) {skip=0}
        else {next}
      }
      {print}
    ' hack/k3d.yaml > "$TMP_CONFIG"
    
    # Create cluster with registry config volume mounted
    k3d cluster create --config "$TMP_CONFIG" \
      --volume "$REGISTRIES_CONFIG:/etc/rancher/k3s/registries.yaml@all"
    rm "$TMP_CONFIG"
    
    # Manually connect the existing registry to the cluster network
    info "Connecting registry to cluster network..."
    if ! docker network inspect k3d-airstore --format '{{range .Containers}}{{.Name}} {{end}}' | grep -q "$REGISTRY_CONTAINER"; then
      docker network connect k3d-airstore "$REGISTRY_CONTAINER" 2>/dev/null || warn "Could not connect registry to network (may already be connected)"
    fi
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
