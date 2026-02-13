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
./e2e/run.sh sources      # Integration sources (/sources)
./e2e/run.sh compression  # Compression strategy comparison
```

## Tests

| Test | Description |
|------|-------------|
| `setup` | Create workspace, add member, generate token |
| `task` | Submit and run a container task via worker |
| `fs` | Mount FUSE filesystem, verify directories |
| `tools` | Execute Wikipedia tool via filesystem |
| `context` | S3 read/write via LocalStack |
| `sources` | Read-only integration filesystem (GitHub, Gmail, Notion, etc.) |
| `smart` | Smart query filesystem (mkdir creates Gmail queries, etc.) |
| `compression` | Read files raw vs strip vs passthrough, verify token reduction and cache |

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
| `GITHUB_TOKEN` | (none) | GitHub PAT for sources test (optional) |

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

---

## Integration Tests (CI)

The `integration_test.sh` script runs against the **production gateway** via
HTTP API in CI (GitHub Actions) and can also be run locally. It does not
require a k3d cluster or filesystem mount.

### Quick Start

```bash
# Run locally
AIRSTORE_WS_TOKEN=<token> bash e2e/integration_test.sh

# Generate charts from results
pip install matplotlib
python e2e/plot_results.py e2e/results.json e2e/plots/
```

### What it tests

| Phase | Description |
|-------|-------------|
| I/O Smoke | `list`, `read`, `stat` via HTTP API against `/sources/gmail/...` |
| Compression A/B | Reads each file raw vs `strip` via HTTP API, asserts no inflation and min avg reduction |
| Cache Consistency | Reads same file 3x with `strip`, asserts identical results |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AIRSTORE_WS_TOKEN` | (required) | Workspace auth token (resolves workspace automatically) |
| `AIRSTORE_GATEWAY_HTTP` | `https://api.airstore.ai` | HTTP API base URL |
| `AIRSTORE_QUERY_PATH` | `/sources/gmail/unread-emails` | Source path to test |
| `COMPRESSION_MIN_REDUCTION` | `10` | Min avg % reduction (fail below) |
| `RESULTS_JSON` | `e2e/results.json` | Output path for structured results |

### Output

- **`results.json`** — structured test results (uploaded as CI artifact)
- **`plots/`** — PNG charts: bytes comparison, reduction %, latency, summary donut
- **Job summary** — markdown table rendered in the GitHub Actions Summary tab

### CI Workflow

The `.github/workflows/integration.yml` workflow runs on PRs and pushes to `main`:

1. Runs `integration_test.sh` (HTTP API tests against production gateway)
2. Generates charts with `plot_results.py`
3. Writes markdown summary to the GitHub Actions Summary tab
4. Uploads results + plots as artifacts
