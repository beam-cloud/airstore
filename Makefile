SHELL := /bin/bash
tag := latest
workerTag := latest

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    BUILD_ENV := CGO_ENABLED=1 CGO_CFLAGS="-I/usr/local/include/fuse" CGO_LDFLAGS="-L/usr/local/lib"
else
    BUILD_ENV := CGO_ENABLED=1
endif

GO_MIN_VERSION := 1.24
GO_CURRENT := $(shell go version 2>/dev/null | sed -E 's/.*go([0-9]+\.[0-9]+).*/\1/')

.PHONY: check-go setup doctor build shim clean protocol baml fmt tidy \
        test e2e e2e-check \
        k3d-up k3d-down k3d-rebuild use \
        gateway worker sandbox \
        dev-gateway dev-worker start stop \
        cli cli-managed cli-managed-dev cli-release \
        fs fs-unmount \
        logs logs-gateway redis redis-stop clean-cluster clean-all

check-go:
	@if ! command -v go >/dev/null 2>&1; then \
		echo "ERROR: Go is not installed. Install $(GO_MIN_VERSION)+ from https://go.dev/dl/"; exit 1; \
	fi
	@if [ "$$(printf '%s\n' "$(GO_MIN_VERSION)" "$(GO_CURRENT)" | sort -V | head -n1)" != "$(GO_MIN_VERSION)" ]; then \
		echo "ERROR: Go $(GO_MIN_VERSION)+ required, found $(GO_CURRENT)"; exit 1; \
	fi

# ============================================================================
# Build
# ============================================================================

build: check-go shim
	$(BUILD_ENV) go build -o bin/gateway ./cmd/gateway
	$(BUILD_ENV) go build -o bin/worker ./cmd/worker
	$(BUILD_ENV) go build -o bin/airstore ./cmd/cli

SHIM_DIR := pkg/filesystem/vnode/embed/shims
SHIM_SRC := ./cmd/tools/shim

shim: check-go
	@mkdir -p $(SHIM_DIR)
	@echo "Building shims..."
	@GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o $(SHIM_DIR)/darwin_amd64 $(SHIM_SRC)
	@GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -o $(SHIM_DIR)/darwin_arm64 $(SHIM_SRC)
	@GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o $(SHIM_DIR)/linux_amd64 $(SHIM_SRC)
	@GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o $(SHIM_DIR)/linux_arm64 $(SHIM_SRC)
	@if [ "$$(uname -s)" = "Darwin" ]; then \
		codesign -s - --force $(SHIM_DIR)/darwin_amd64 2>/dev/null || true; \
		codesign -s - --force $(SHIM_DIR)/darwin_arm64 2>/dev/null || true; \
	fi

clean:
	rm -rf bin/ $(SHIM_DIR)

protocol:
	@bash bin/gen_proto.sh

baml:
	@echo "Generating BAML client..."
	@baml-cli generate --from pkg/sources/queries/baml_src
	@goimports -w pkg/sources/queries/baml_client/
	@baml-cli generate --from pkg/worker/agentsignal/baml_src
	@goimports -w pkg/worker/agentsignal/baml_client/
	@baml-cli generate --from pkg/views/baml_src
	@goimports -w pkg/views/baml_client/

fmt: check-go
	go fmt ./...

tidy: check-go
	go mod tidy

# ============================================================================
# Test
# ============================================================================

test: check-go
	$(BUILD_ENV) go test -v ./pkg/... -count=1

e2e: e2e-check
	@bash ./e2e/run.sh

e2e-check:
	@kubectl config current-context | grep -q k3d-airstore || \
		(echo "ERROR: Not connected to k3d-airstore cluster. Run: make use" && exit 1)

# ============================================================================
# Images
# ============================================================================

gateway:
	docker build . --target final -f ./docker/Dockerfile.gateway -t localhost:5001/airstore-gateway:$(tag)
	docker push localhost:5001/airstore-gateway:$(tag)
	-kubectl rollout restart deployment/airstore-gateway -n airstore 2>/dev/null || true

worker:
	docker build . --target final -f ./docker/Dockerfile.worker -t localhost:5001/airstore-worker:$(workerTag)
	docker push localhost:5001/airstore-worker:$(workerTag)
	-kubectl delete pods -n airstore -l airstore.beam.cloud/role=worker --force --grace-period=0 2>/dev/null || true
	-docker exec k3d-airstore-server-0 crictl rmi --prune 2>/dev/null || true
	-docker exec k3d-registry.localhost registry garbage-collect /etc/docker/registry/config.yml --delete-untagged -q 2>/dev/null || true

sandbox:
	./bin/build_sandbox.sh $(tag)

# ============================================================================
# CLI
# ============================================================================

cli: check-go
	$(BUILD_ENV) go build -o bin/airstore ./cmd/cli

cli-managed: check-go
	$(BUILD_ENV) go build -tags managed \
		-ldflags "-X github.com/beam-cloud/airstore/pkg/cli.Release=true" \
		-o bin/airstore ./cmd/cli

cli-managed-dev: check-go
	$(BUILD_ENV) go build -tags managed -o bin/airstore ./cmd/cli

VERSION ?= dev
cli-release: check-go shim
	$(BUILD_ENV) go build -tags managed \
		-ldflags "-s -w -X github.com/beam-cloud/airstore/pkg/cli.Version=$(VERSION) \
		          -X github.com/beam-cloud/airstore/pkg/cli.Release=true" \
		-o bin/airstore ./cmd/cli

# ============================================================================
# Cluster
# ============================================================================

setup:
	@make k3d-up
	@make gateway worker
	@kustomize build manifests/k3d | kubectl apply -f-

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
# Dev
# ============================================================================

dev-gateway: check-go
	go run ./cmd/gateway

dev-worker: check-go
	WORKER_ID=test-worker-1 GATEWAY_GRPC_ADDR=localhost:1993 go run ./cmd/worker

start:
	@mkdir -p logs
	cd hack && okteto up --file okteto.yaml 2>&1 | tee ../logs/gateway.log

stop:
	cd hack && okteto down --file okteto.yaml

doctor:
	@echo "=== Airstore Environment Check ==="
	@printf "  go:        "; command -v go >/dev/null 2>&1 && echo "✓ ($$(go version | sed -E 's/.*go([0-9]+\.[0-9]+).*/\1/'))" || echo "✗"
	@printf "  kubectl:   "; which kubectl >/dev/null 2>&1 && echo "✓" || echo "✗"
	@printf "  k3d:       "; which k3d >/dev/null 2>&1 && echo "✓" || echo "✗"
	@printf "  docker:    "; which docker >/dev/null 2>&1 && echo "✓" || echo "✗"
	@printf "  kustomize: "; which kustomize >/dev/null 2>&1 && echo "✓" || echo "✗"
	@echo ""
	@echo "Cluster:"; k3d cluster list 2>/dev/null || echo "  (none)"
	@echo "Context: $$(kubectl config current-context 2>/dev/null || echo 'none')"

# ============================================================================
# Filesystem
# ============================================================================

MOUNT_POINT ?= /tmp/airstore

fs: cli shim
	./bin/airstore mount $(MOUNT_POINT) --verbose

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
	@k3d cluster delete --all 2>/dev/null || true
	@k3d registry delete --all 2>/dev/null || true
	@docker network prune -f 2>/dev/null || true
