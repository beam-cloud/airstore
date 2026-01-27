# End-to-End Tests

E2E tests run against the k3d cluster to verify the full system works.

## Quick Start

```bash
# Setup cluster (first time)
make setup

# Run all e2e tests
make e2e

# Run specific test
./e2e/run.sh setup    # Workspace, member, token creation
./e2e/run.sh task     # Container task execution
./e2e/run.sh fs       # FUSE filesystem mount
./e2e/run.sh tools    # Tool execution via filesystem
./e2e/run.sh context  # S3 context storage (LocalStack)
```

## Tests

| Test | Description |
|------|-------------|
| `setup` | Create workspace, add member, generate token |
| `task` | Submit and run a container task via worker |
| `fs` | Mount FUSE filesystem, verify directories |
| `tools` | Execute Wikipedia tool via filesystem |
| `context` | S3 read/write via LocalStack |

## Requirements

- k3d cluster running (`make setup`)
- Services deployed (gateway, worker, redis, postgres, localstack)
- Ports accessible: 1993 (gRPC), 1994 (HTTP), 4566 (S3)

## Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_GRPC` | `localhost:1993` | Gateway gRPC address |
| `GATEWAY_HTTP` | `localhost:1994` | Gateway HTTP address |
| `S3_ENDPOINT` | `http://localhost:4566` | LocalStack S3 endpoint |
| `S3_BUCKET` | `airstore-context` | Context storage bucket |
| `MOUNT_POINT` | `/tmp/airstore-e2e` | Filesystem mount location |

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   e2e/run   │────▶│   Gateway   │────▶│   Worker    │
│    tests    │     │  (k3d pod)  │     │  (k3d pod)  │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       │                   ▼                   │
       │            ┌─────────────┐            │
       │            │   Redis     │            │
       │            │   Postgres  │            │
       │            └─────────────┘            │
       │                                       │
       ▼                                       ▼
┌─────────────┐                       ┌─────────────┐
│  LocalStack │◀──────────────────────│  Filesystem │
│     S3      │                       │  (FUSE)     │
└─────────────┘                       └─────────────┘
```
