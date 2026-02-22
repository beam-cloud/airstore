# Agent Orchestration Model (MVP)

This is the minimal contract for agents + runs in `airstore`.

## Core Model

- `agent_task_envelope`: accepted intent + routing + idempotency.
- `agent_run`: execution lifecycle for the accepted intent.
- `agent_run_attempt`: concrete execution attempt(s) for a run.
- `task`: internal execution unit used by run attempts in the worker substrate.

## Runtime Flow

1. Validate and accept command payload.
2. Persist envelope as `accepted`.
3. Queue envelope in Redis and mark it `queued`.
4. Dispatcher creates `run` and first `run_attempt`.
5. Worker lifecycle updates attempts/runs and appends snapshots/events.

Clients should treat acceptance as async and poll `runs` for terminal state.

## Queue Modes

Supported queue modes are `queue`, `followup`, `steer`, and `interrupt`.

- `queue`: FIFO by envelope ID.
- `followup` / `steer` / `interrupt`: latest-envelope-wins per mode key.
- Replaced envelopes are marked `dropped` with a reason.

## Execution Policy

`POST /tasks` accepts an optional `policy` object:

- `host`, `security`, `ask`
- `runtime_type`, `workspace_access`, `network_enabled`, `interactive`
- `resources` (`cpu`, `memory`, `gpu`)

The policy is validated at ingress, persisted on run/attempt metadata, and bridged onto execution tasks for worker sandbox configuration.

## API Surface

Workspace-scoped endpoints under `/api/v1/workspaces/:workspace_id`:

- `POST /agents`, `GET /agents`, `GET /agents/:agent_id`
- `POST /tasks`, `GET /tasks/:envelope_id`
- `GET /runs`, `GET /runs/:run_id`
- `GET /runs/:run_id/attempts`, `GET /runs/:run_id/snapshots`, `GET /runs/:run_id/events`
- `POST /runs/:run_id/input`, `POST /runs/:run_id/cancel`

## Operational Caveats

- Agent/run ingress endpoints use strict JSON decoding (unknown fields or extra trailing payload are rejected).
- Accepted responses always include `accepted`, `idempotent_hit`, and `envelope`; task ingress also returns `run_id` when available.
- Migration bridge logic includes idempotent schema guards so partially drifted databases can self-heal on startup.
