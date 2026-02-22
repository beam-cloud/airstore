# Agent Orchestration Model (MVP)

This document defines the backend orchestration contract introduced for agents and runs.

## Intent vs Execution Separation

- `agent_task_envelope` is the source of truth for accepted intent and routing metadata.
- `agent_run` is the source of truth for run lifecycle state.
- `agent_run_attempt` is the source of truth for concrete execution attempts.
- `task` remains the execution substrate only; legacy `/api/v1/tasks` semantics are unchanged.

## Accepted-First Semantics

Agent command ingress is accepted first and queued asynchronously:

1. Validate payload (`message`, `session_id`, `idempotency_key`, strict JSON fields).
2. Persist envelope with state `accepted`.
3. Queue envelope in Redis and transition envelope state to `queued`.
4. Return accepted response immediately.
5. Dispatcher later materializes `run` + `run_attempt` and bridges to execution `task`.

Because the acceptance step is decoupled from dispatch, client APIs should treat acceptance as asynchronous and poll `runs` for terminal state.

## Queue Reshaping

The queue router supports mode-based reshaping before dispatch:

- `queue`: FIFO enqueue by envelope ID.
- `followup` / `steer` / `interrupt`: mode-key queue with latest-envelope wins behavior.
- Replaced envelopes are marked `dropped` with a dropped reason.

## Run Lifecycle and Snapshots

- Dispatcher creates `agent_run` in `accepted` and appends the first snapshot.
- Worker callbacks (`SetTaskStarted`, `SetTaskResult`) update attempt + run lifecycle state.
- Snapshot events are appended independently of envelope state transitions.
- Run events are published to Redis (and S2 when configured).

## Execution Policy Mapping

Run policy maps deterministically to execution task + sandbox behavior:

- `host=sandbox` only (MVP constraint).
- `interactive=true` maps to interactive task mode.
- `runtime_type` maps to sandbox runtime (`gvisor`/`runc`).
- `workspace_access` maps to mount behavior (`none`, `ro`, `rw`).
- `network_enabled=false` disables sandbox networking.
- `timeout_ms` maps to per-task execution context timeout.

## API Surface (Workspace Scoped)

Under `/api/v1/workspaces/:workspace_id`:

- `POST /agents`, `GET /agents`, `GET /agents/:agent_id`
- `POST /tasks`, `GET /tasks/:envelope_id` (task envelope intent API)
- `GET /runs`, `GET /runs/:run_id`
- `GET /runs/:run_id/attempts`, `GET /runs/:run_id/snapshots`, `GET /runs/:run_id/events`
- `POST /runs/:run_id/input`, `POST /runs/:run_id/cancel`
