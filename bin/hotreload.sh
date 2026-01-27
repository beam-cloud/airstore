#!/usr/bin/env bash

cd /workspace

# Use air from container if available, otherwise use synced binary
AIR_BIN="${AIR_BIN:-/usr/local/bin/air}"
if [ ! -x "$AIR_BIN" ]; then
  AIR_BIN="/workspace/bin/air.linux"
fi

exec $AIR_BIN \
  --build.cmd "$BUILD_COMMAND" \
  --build.bin "$BUILD_BINARY_PATH" \
  --build.exclude_dir "deploy,docs,docker,manifests,hack,bin,.git"
