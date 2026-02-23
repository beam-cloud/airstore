# Agent Orchestration Model (MVP)

This is the minimal contract for agents + runs in `airstore`.

## Core Model

- `agent_task`: accepted intent + routing + idempotency.
- `agent_run`: execution lifecycle for the accepted intent.
- `agent_run_attempt`: concrete execution attempt(s) for a run.
- `run_execution`: internal execution unit used by run attempts in the worker substrate.

## Runtime Flow

1. Validate and accept command payload.
2. Persist task as `accepted`.
3. Queue task in Redis and mark it `queued`.
4. Dispatcher creates or resolves a run target and creates a `run_attempt`.
5. Worker lifecycle updates attempts/runs and appends snapshots/events.
6. Retry scheduling can create additional attempts on the same run.

Clients should treat acceptance as async and poll `runs` for terminal state.

## Run + Retry Semantics

- A `run` is the execution lifecycle container.
- A `run_attempt` is one concrete execution attempt for that run.
- Retries stay on the same `run_id`; attempt numbers increment (`1..N`).
- Default retry policy is:
  - `retry.max_attempts = 2`
  - `retry.delay_ms = 0`
- Retry policy is part of `policy.retry` on task ingress and is persisted on run delivery metadata.

## Queue Modes

Supported queue modes are `queue`, `followup`, `steer`, and `interrupt`.

- `queue`: FIFO by task ID.
- `followup` / `steer` / `interrupt`: latest-task-wins per mode key.
- Replaced tasks are marked `dropped` with a reason.
- `run_input` tasks dispatch new attempts on the target run (`target_run_id`) instead of creating unrelated runs.
- `steer` is best-effort cooperative in-run injection:
  - The target run must be `running`.
  - A running attempt must exist with a live interactive run execution.
  - When eligible, input is injected into that run execution (same run, same attempt stream).
  - When not eligible, behavior falls back to followup-attempt dispatch (no input loss).
- `interrupt` cancels active attempt tasks and then continues with the latest run input.

## Execution Policy

`POST /tasks` accepts an optional `policy` object:

- `host`, `security`, `ask`
- `runtime_type`, `workspace_access`, `network_enabled`, `interactive`
- `resources` (`cpu`, `memory`, `gpu`)
- `retry` (`max_attempts`, `delay_ms`)

The policy is validated at ingress, persisted on run/attempt metadata, and bridged onto run executions for worker sandbox configuration.

## API Surface

Workspace-scoped endpoints under `/api/v1/workspaces/:workspace_id`:

- `POST /agents`, `GET /agents`, `GET /agents/:agent_id`
- `POST /tasks`, `GET /tasks/:task_id`
- `GET /runs`, `GET /runs/:run_id`
- `GET /runs/:run_id/attempts`, `GET /runs/:run_id/snapshots`, `GET /runs/:run_id/events`
- `POST /runs/:run_id/input`, `POST /runs/:run_id/cancel`

## Operational Caveats

- Agent/run ingress endpoints use strict JSON decoding (unknown fields or extra trailing payload are rejected).
- `session_id` and `idempotency_key` are server-generated when omitted for task ingress.
- Accepted responses always include `accepted`, `idempotent_hit`, and `task`; task ingress also returns `run_id` when available.
- Migration bridge logic includes idempotent schema guards so partially drifted databases can self-heal on startup.
- `ask` values other than `off` move attempts into `blocked` status; human approval workflows are not yet implemented.

## OpenClaw Gap Map

Implemented now:

- Agent profiles, tasks, runs, run attempts, snapshots, and run events.
- Queue reshape modes for `queue`, `followup`, `steer`, `interrupt`.
- Run-input routing onto target run attempts.
- Basic delivery metadata capture (`deliver`, routing hints, provenance, labels).

Not yet implemented:

- Full delivery-plan resolver with explicit plan objects/state transitions.
- `collect` queue mode.
- Chat-native send/event stream contract (`chat.send`, delta/final stream semantics).
- Subagent spawn registry, requester announcement routing, and completion lifecycle.
- HITL exec approval request/resolve transport and state machine.
