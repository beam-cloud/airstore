#!/bin/bash

set -e

if [ -z "$PROTOC_INCLUDE_PATH" ]; then
    PROTOC_INCLUDE_PATH="/usr/local/include"
fi

# Check if protoc-gen-go-grpc is installed
GRPC_PATH=$(which protoc-gen-go-grpc 2>/dev/null || true)
if [ -z "$GRPC_PATH" ]; then
    echo "protoc-gen-go-grpc is not installed"
    echo "Install with: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"
    exit 1
fi

# Check if protoc-gen-go is installed
GO_PATH=$(which protoc-gen-go 2>/dev/null || true)
if [ -z "$GO_PATH" ]; then
    echo "protoc-gen-go is not installed"
    echo "Install with: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"
    exit 1
fi

echo "Generating Go code for proto services..."

# Generate filesystem service
protoc \
    -I $PROTOC_INCLUDE_PATH \
    -I ./googleapis \
    -I ./proto \
    --go_out=./proto \
    --go_opt=paths=source_relative \
    --go-grpc_out=./proto \
    --go-grpc_opt=paths=source_relative \
    ./proto/filesystem.proto

# Generate worker service
protoc \
    -I $PROTOC_INCLUDE_PATH \
    -I ./googleapis \
    -I ./proto \
    --go_out=./proto \
    --go_opt=paths=source_relative \
    --go-grpc_out=./proto \
    --go-grpc_opt=paths=source_relative \
    ./proto/worker.proto

# Generate tools service
protoc \
    -I $PROTOC_INCLUDE_PATH \
    -I ./googleapis \
    -I ./proto \
    --go_out=./proto \
    --go_opt=paths=source_relative \
    --go-grpc_out=./proto \
    --go-grpc_opt=paths=source_relative \
    ./proto/tools.proto

# Generate context service
protoc \
    -I $PROTOC_INCLUDE_PATH \
    -I ./googleapis \
    -I ./proto \
    --go_out=./proto \
    --go_opt=paths=source_relative \
    --go-grpc_out=./proto \
    --go-grpc_opt=paths=source_relative \
    ./proto/context.proto

# Generate gateway service
protoc \
    -I $PROTOC_INCLUDE_PATH \
    -I ./googleapis \
    -I ./proto \
    --go_out=./proto \
    --go_opt=paths=source_relative \
    --go-grpc_out=./proto \
    --go-grpc_opt=paths=source_relative \
    ./proto/gateway.proto

echo "Proto generation complete!"

