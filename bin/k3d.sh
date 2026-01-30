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

MAX_RETRIES="${K3D_MAX_RETRIES:-2}"

# Diagnostic function to capture container logs on failure
dump_diagnostics() {
  echo ""
  error "Cluster creation failed. Collecting diagnostics..."
  echo ""
  
  # Check Docker Desktop memory (macOS)
  if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "=== Docker Desktop Settings ==="
    if [ -f "$HOME/Library/Group Containers/group.com.docker/settings.json" ]; then
      mem_mib=$(python3 -c "import json; f=open('$HOME/Library/Group Containers/group.com.docker/settings.json'); d=json.load(f); print(d.get('memoryMiB', 'unknown'))" 2>/dev/null || echo "unknown")
      echo "Docker memory allocation: ${mem_mib} MiB"
      if [[ "$mem_mib" != "unknown" ]] && [[ "$mem_mib" -lt 4096 ]]; then
        warn "Docker has less than 4GB RAM allocated. k3s needs at least 2GB, recommend 4GB+."
        echo "  -> Open Docker Desktop -> Settings -> Resources -> Memory"
      fi
    else
      echo "Could not read Docker Desktop settings"
    fi
    echo ""
  fi
  
  # Try to get logs from the failed k3s container
  echo "=== k3s Container Logs (if available) ==="
  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q "k3d-airstore-server-0"; then
    docker logs k3d-airstore-server-0 2>&1 | tail -50 || echo "(no logs available)"
  else
    echo "(container not found)"
  fi
  echo ""
  
  # Check for port conflicts
  echo "=== Port Check ==="
  for port in 1994 1993 5001 6443; do
    if lsof -i :$port >/dev/null 2>&1; then
      echo "Port $port in use by:"
      lsof -i :$port | head -3
    else
      echo "Port $port: free"
    fi
  done
  echo ""
  
  # Docker info
  echo "=== Docker Info ==="
  docker info 2>&1 | grep -E "(Server Version|Total Memory|CPUs|Operating System)" || true
  echo ""
}

# Try to fix Docker state issues (helps with cgroups/restart loops)
reset_docker_state() {
  info "Resetting Docker state to fix potential cgroups issues..."
  
  # Kill any zombie k3d containers
  docker ps -a --format '{{.Names}}' 2>/dev/null | grep "k3d-airstore" | xargs -r docker rm -f 2>/dev/null || true
  
  # On macOS, try to restart Docker Desktop if containers keep restarting
  if [[ "$(uname -s)" == "Darwin" ]]; then
    # Check if Docker is responsive
    if ! docker info >/dev/null 2>&1; then
      warn "Docker not responsive, attempting restart..."
      osascript -e 'quit app "Docker"' 2>/dev/null || true
      sleep 3
      open -a Docker
      info "Waiting for Docker to start (up to 60s)..."
      for i in {1..60}; do
        if docker info >/dev/null 2>&1; then
          info "Docker is ready"
          break
        fi
        sleep 1
      done
    fi
  fi
  
  # Prune any dangling resources that might cause issues
  docker network prune -f 2>/dev/null || true
  docker volume prune -f 2>/dev/null || true
  
  sleep 2
}

# Cleanup stale resources from previous failed attempts
cleanup_stale_resources() {
  # First, try k3d's own cleanup (handles metadata that docker cleanup misses)
  # This fixes "cluster already exists" errors when cluster doesn't show in list
  if k3d cluster delete airstore 2>/dev/null; then
    info "Removed stale k3d cluster metadata"
  fi
  
  # Clean up any stale k3d-airstore containers
  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q "k3d-airstore"; then
    info "Cleaning up stale containers from previous attempt..."
    docker rm -f $(docker ps -a --format '{{.Names}}' | grep "k3d-airstore") 2>/dev/null || true
  fi
  
  if docker network ls --format '{{.Name}}' | grep -q "^k3d-airstore$"; then
    info "Cleaning up stale network 'k3d-airstore'..."
    docker network rm k3d-airstore 2>/dev/null || true
  fi
  
  if docker volume ls --format '{{.Name}}' | grep -q "^k3d-airstore-images$"; then
    info "Cleaning up stale volume 'k3d-airstore-images'..."
    docker volume rm k3d-airstore-images 2>/dev/null || true
  fi
}

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
    # Ensure registry container exists and is running
    if ! docker ps -a --format '{{.Names}}' | grep -q "^k3d-registry.localhost$"; then
      warn "Registry 'registry.localhost' exists in k3d but container is missing."
      info "Deleting stale registry and will recreate..."
      k3d registry delete registry.localhost 2>/dev/null || true
    elif ! docker ps --format '{{.Names}}' | grep -q "^k3d-registry.localhost$"; then
      info "Starting existing registry container..."
      docker start k3d-registry.localhost >/dev/null
    else
      info "Registry 'registry.localhost' already exists. Reusing it."
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
  cleanup_stale_resources

  # Retry loop for cluster creation
  attempt=0
  while [ $attempt -lt $MAX_RETRIES ]; do
    attempt=$((attempt + 1))
    
    if [ $attempt -gt 1 ]; then
      warn "Cluster creation attempt $attempt of $MAX_RETRIES..."
      reset_docker_state
      cleanup_stale_resources
    fi
    
    cluster_created=false
    
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
      # Use --no-rollback so we can capture logs on failure
      if k3d cluster create --config "$TMP_CONFIG" \
        --volume "$REGISTRIES_CONFIG:/etc/rancher/k3s/registries.yaml@all" \
        --no-rollback; then
        cluster_created=true
      fi
      rm "$TMP_CONFIG"
      
      if $cluster_created; then
        # Manually connect the existing registry to the cluster network
        info "Connecting registry to cluster network..."
        if ! docker network inspect k3d-airstore --format '{{range .Containers}}{{.Name}} {{end}}' | grep -q "$REGISTRY_CONTAINER"; then
          docker network connect k3d-airstore "$REGISTRY_CONTAINER" 2>/dev/null || warn "Could not connect registry to network (may already be connected)"
        fi
      fi
    else
      # Use --no-rollback so we can capture logs on failure
      if k3d cluster create --config hack/k3d.yaml --no-rollback; then
        cluster_created=true
      fi
    fi
    
    if $cluster_created; then
      break
    fi
    
    # Cluster creation failed
    dump_diagnostics
    info "Cleaning up failed cluster resources..."
    k3d cluster delete airstore 2>/dev/null || true
    
    if [ $attempt -ge $MAX_RETRIES ]; then
      error "Cluster creation failed after $MAX_RETRIES attempts."
      echo ""
      echo "Troubleshooting tips:"
      echo "  1. Try restarting Docker: docker restart or restart Docker Desktop"
      echo "  2. Ensure Docker has at least 4GB RAM allocated"
      echo "  3. Run: docker system prune -f"
      echo "  4. Set K3D_MAX_RETRIES=3 to retry more times"
      exit 1
    fi
    
    info "Retrying in 5 seconds..."
    sleep 5
  done
  
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
