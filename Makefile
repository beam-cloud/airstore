SHELL := /bin/bash
tag := latest
workerTag := latest

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    BUILD_ENV := CGO_ENABLED=1 CGO_CFLAGS="-I/usr/local/include/fuse" CGO_LDFLAGS="-L/usr/local/lib"
else
    BUILD_ENV := CGO_ENABLED=1
endif

# Go version requirement
GO_MIN_VERSION := 1.24
GO_CURRENT := $(shell go version 2>/dev/null | sed -E 's/.*go([0-9]+\.[0-9]+).*/\1/')

.PHONY: check-go
check-go:
	@if ! command -v go >/dev/null 2>&1; then \
		echo ""; \
		echo "ERROR: Go is not installed."; \
		echo ""; \
		echo "Install Go $(GO_MIN_VERSION)+ from https://go.dev/dl/"; \
		echo "See README.md for instructions."; \
		echo ""; \
		exit 1; \
	fi
	@if [ "$$(printf '%s\n' "$(GO_MIN_VERSION)" "$(GO_CURRENT)" | sort -V | head -n1)" != "$(GO_MIN_VERSION)" ]; then \
		echo ""; \
		echo "ERROR: Go $(GO_MIN_VERSION)+ required, but found $(GO_CURRENT)"; \
		echo ""; \
		echo "Install Go $(GO_MIN_VERSION)+ from https://go.dev/dl/"; \
		echo "See README.md for instructions."; \
		echo ""; \
		exit 1; \
	fi

# ============================================================================
# Quick Start
# ============================================================================
#
#   make setup     - Create k3d cluster and deploy everything
#   make test      - Run unit tests
#   make e2e       - Run end-to-end tests against cluster
#   make logs      - Watch cluster logs
#   make doctor    - Check your environment
#

# ============================================================================
# Setup
# ============================================================================

setup:
	@echo "==> Setting up Airstore development environment..."
	@make k3d-up
	@make gateway worker
	@kustomize build manifests/k3d | kubectl apply -f-
	@echo "==> Setup complete! Run 'make logs' to watch."

doctor:
	@echo "=== Airstore Environment Check ==="
	@echo ""
	@echo "Tools:"
	@printf "  go:        "; \
		if command -v go >/dev/null 2>&1; then \
			ver=$$(go version | sed -E 's/.*go([0-9]+\.[0-9]+).*/\1/'); \
			if [ "$$(printf '%s\n' "$(GO_MIN_VERSION)" "$$ver" | sort -V | head -n1)" = "$(GO_MIN_VERSION)" ]; then \
				echo "✓ ($$ver)"; \
			else \
				echo "✗ $$ver (need $(GO_MIN_VERSION)+)"; \
			fi; \
		else \
			echo "✗ not found (need $(GO_MIN_VERSION)+)"; \
		fi
	@printf "  kubectl:   "; which kubectl >/dev/null 2>&1 && echo "✓" || echo "✗ not found"
	@printf "  k3d:       "; which k3d >/dev/null 2>&1 && echo "✓" || echo "✗ not found"
	@printf "  docker:    "; which docker >/dev/null 2>&1 && echo "✓" || echo "✗ not found"
	@printf "  kustomize: "; which kustomize >/dev/null 2>&1 && echo "✓" || echo "✗ not found"
	@echo ""
	@echo "Cluster:"
	@k3d cluster list 2>/dev/null || echo "  (no clusters)"
	@echo ""
	@echo "Context: $$(kubectl config current-context 2>/dev/null || echo 'none')"

# ============================================================================
# Build
# ============================================================================

build: check-go shim
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/worker ./cmd/worker
	go build -o bin/cli ./cmd/cli

cli: check-go
	go build -o bin/cli ./cmd/cli

SHIM_DIR := pkg/filesystem/vnode/embed/shims
SHIM_SRC := ./cmd/tools/shim

shim: check-go
	@mkdir -p $(SHIM_DIR)
	@echo "Building shims..."
	@GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o $(SHIM_DIR)/darwin_amd64 $(SHIM_SRC)
	@GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -o $(SHIM_DIR)/darwin_arm64 $(SHIM_SRC)
	@GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o $(SHIM_DIR)/linux_amd64 $(SHIM_SRC)
	@GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o $(SHIM_DIR)/linux_arm64 $(SHIM_SRC)
	@# Ad-hoc sign macOS binaries to avoid Gatekeeper delays
	@if [ "$$(uname -s)" = "Darwin" ]; then \
		echo "Signing macOS shims..."; \
		codesign -s - --force $(SHIM_DIR)/darwin_amd64 2>/dev/null || true; \
		codesign -s - --force $(SHIM_DIR)/darwin_arm64 2>/dev/null || true; \
	fi
	@echo "Shims built: $$(ls -la $(SHIM_DIR))"

clean:
	rm -rf bin/ $(SHIM_DIR)

protocol:
	@bash bin/gen_proto.sh

baml:
	@echo "Generating BAML client..."
	@baml-cli generate --from pkg/sources/queries/baml_src
	@goimports -w pkg/sources/queries/baml_client/

fmt: check-go
	go fmt ./...

tidy: check-go
	go mod tidy

# ============================================================================
# Testing
# ============================================================================

# Unit tests (run locally, no cluster needed)
test: check-go
	$(BUILD_ENV) go test -v ./pkg/... -count=1

# End-to-end tests (run against k3d cluster)
e2e: e2e-check
	@bash ./e2e/run.sh

e2e-check:
	@kubectl config current-context | grep -q k3d-airstore || \
		(echo "ERROR: Not connected to k3d-airstore cluster. Run: make use" && exit 1)

# ============================================================================
# Cluster Management
# ============================================================================

k3d-up:
	bash bin/k3d.sh up

k3d-down:
	bash bin/k3d.sh down

k3d-rebuild:
	make k3d-down && sleep 2 && make setup

use:
	@kubectl config use-context k3d-airstore
	@kubectl config set-context --current --namespace=airstore
	@echo "Switched to k3d-airstore"

# ============================================================================
# Docker Images
# ============================================================================

gateway:
	docker build . --target final -f ./docker/Dockerfile.gateway -t localhost:5001/airstore-gateway:$(tag)
	docker push localhost:5001/airstore-gateway:$(tag)
	-kubectl rollout restart deployment/airstore-gateway -n airstore 2>/dev/null || true

worker:
	docker build . --target final -f ./docker/Dockerfile.worker -t localhost:5001/airstore-worker:$(workerTag)
	docker push localhost:5001/airstore-worker:$(workerTag)
	-kubectl delete pods -n airstore -l airstore.beam.cloud/role=worker --force --grace-period=0 2>/dev/null || true

# ============================================================================
# Deployment
# ============================================================================

deploy:
	kustomize build manifests/k3d | kubectl apply -f-

undeploy:
	kustomize build manifests/k3d | kubectl delete -f- --ignore-not-found

restart:
	kubectl rollout restart deployment -n airstore

# ============================================================================
# Development (local, no k8s)
# ============================================================================

dev-gateway: check-go
	go run ./cmd/gateway

dev-worker: check-go
	WORKER_ID=test-worker-1 GATEWAY_GRPC_ADDR=localhost:1993 go run ./cmd/worker

# Okteto hot-reload
start:
	cd hack && okteto up --file okteto.yaml

stop:
	cd hack && okteto down --file okteto.yaml

# ============================================================================
# Filesystem
# ============================================================================

MOUNT_POINT ?= /tmp/airstore

fs: cli build-shim
	./bin/cli mount $(MOUNT_POINT) --verbose

fs-unmount:
	@umount $(MOUNT_POINT) 2>/dev/null || diskutil unmount $(MOUNT_POINT) 2>/dev/null || fusermount -u $(MOUNT_POINT) 2>/dev/null || true

# ============================================================================
# Helpers
# ============================================================================

logs:
	stern -n airstore .

logs-gateway:
	kubectl logs -n airstore -l app=airstore-gateway -f

redis:
	docker run -d --name airstore-redis -p 6379:6379 redis:7-alpine

redis-stop:
	-docker stop airstore-redis && docker rm airstore-redis

clean-cluster:
	k3d cluster delete airstore 2>/dev/null || true

clean-all:
	@echo "Deleting all k3d clusters and registries..."
	@k3d cluster delete --all 2>/dev/null || true
	@k3d registry delete --all 2>/dev/null || true
	@docker network prune -f 2>/dev/null || true

.PHONY: check-go setup doctor build cli shim clean protocol baml fmt tidy \
        test e2e e2e-check \
        k3d-up k3d-down k3d-rebuild use \
        gateway worker deploy undeploy restart \
        dev-gateway dev-worker start stop \
        fs fs-unmount \
        logs logs-gateway redis redis-stop clean-cluster clean-all
