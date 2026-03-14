# Airstore

Backend orchestration and worker system for AI agents. Airstore exposes integrations (Gmail, GitHub, Linear, etc.) as a virtual filesystem so agents like Claude Code can read and work with data as files.

## Architecture

| Component | Role |
|-----------|------|
| **Gateway** | HTTP/gRPC API server, orchestration, SSE streaming, task lifecycle |
| **Worker** | Consumes tasks, runs agents in gVisor sandboxes, manages turns |
| **PostgreSQL** | Persistent state (tasks, runs, agents, workspaces) — database `airstore` |
| **Redis** | Real-time state, session leases, pub/sub, input wake channels |
| **S2** | Append-only task log storage |

## Key packages

| Package | Purpose |
|---------|---------|
| `pkg/orchestration` | Task lifecycle, input delivery, run dispatch, state machine |
| `pkg/worker` | Worker loop, sandbox management, Claude Code runner, turn sessions |
| `pkg/worker/agentsignal` | BAML classifiers — turn classification, output extraction, follow-up detection |
| `pkg/gateway` | Gateway server, S2, event bus, tool registry |
| `pkg/gateway/services` | gRPC services including worker recovery |
| `pkg/api/v1` | HTTP API endpoints, SSE streaming |
| `pkg/repository` | Postgres, Redis, task queue implementations |
| `pkg/types` | Domain types, error types, state enums |
| `pkg/filesystem` | Virtual filesystem, vnodes, MCP tool shims |
| `pkg/tools` | MCP tool definitions and builtins |
| `pkg/common` | Config, Redis client, S2 client, EventBus |

## Build and deploy

| Command | What it does |
|---------|-------------|
| `make worker` | Docker build → push → force-delete worker pods. **Required** after worker code changes. |
| `make gateway` | Docker build → push → rollout restart. Rarely needed — see below. |
| `make start` | `okteto up` — starts gateway hot-reload. |
| `make test` | Unit tests. |
| `make baml` | Regenerate BAML client after `.baml` file changes. |
| `make protocol` | Regenerate protobuf code. |
| `make shim` | Build MCP tool shims (multi-arch). |

### Gateway hot-reloads automatically

The gateway runs under Okteto with `air` for hot-reload. Code changes sync and rebuild automatically — do NOT run `make gateway` for normal edits. Just save the file.

### Worker requires explicit rebuild

Worker changes require `make worker` which builds the Docker image, pushes it, and force-deletes worker pods so they pull the new image.

## Code conventions

### Logging

Structured logging via zerolog:

```go
log.Info().Str("task_id", id).Int("exit_code", code).Msg("task finished")
addTaskExecutionContext(log.Warn().Err(err), task).Msg("something went wrong")
```

### Error handling

Wrap errors with context: `fmt.Errorf("acquire session lease: %w", err)`. Typed errors in `pkg/types` (e.g., `ErrWorkerNotFound`) for gRPC status mapping.

### BAML classifiers

Turn classification, output extraction, and follow-up detection use BAML (in `pkg/worker/agentsignal/baml_src/`). The classifier is the single source of truth for turn outcomes — do not add ad-hoc detection heuristics in Go code. Run `make baml` after editing `.baml` files.

### Task state machine

Task state transitions are validated by `isValidTransition` in `pkg/orchestration/task_lifecycle.go`. The `TaskLifecycle.Settle` function is the canonical path for run completion → task state changes. Do not bypass the state machine with direct DB updates.

### Interactive sessions

The turn loop lives in `pkg/worker/interactive_task.go`. Key flow:
1. `executeFirstTurn` — tries resume strategies, falls back gracefully
2. `checkNeedsInput` — BAML classification of agent output
3. `waitForFollowupInput` — subscribes to Redis wake channel, polls for claimed input
4. Loop back to step 1 with the user's message as the new prompt

### Output filtering

`isIntermediatePath` in `pkg/worker/analyzer_writer.go` filters outputs that aren't accessible via the UI. The rule is simple: absolute paths outside `/workspace/` are suppressed because presigned URLs won't resolve. The BAML `ExtractOutputs` classifier decides what's meaningful — path-based filtering only handles accessibility.

## Kubernetes

Everything runs in namespace `airstore`. Local dev uses k3d with registry on `localhost:5001`. Manifests are in `manifests/k3d/`.

## Module

```
github.com/beam-cloud/airstore
Go 1.24+
```
