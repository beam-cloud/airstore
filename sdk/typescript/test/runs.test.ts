import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import {
  createTestWorkspace,
  deleteTestWorkspace,
  getClient,
  uniqueName,
  waitForRunIdForTask,
} from './helpers.js';

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForRunSnapshots(
  workspaceId: string,
  runId: string,
  timeoutMs = 20_000,
): Promise<Array<{ run_id: string; seq: number }>> {
  const client = getClient();
  const deadline = Date.now() + timeoutMs;
  let snapshots = await client.runs.listSnapshots(workspaceId, runId);

  while (snapshots.length === 0 && Date.now() < deadline) {
    await sleep(800);
    snapshots = await client.runs.listSnapshots(workspaceId, runId);
  }

  return snapshots as Array<{ run_id: string; seq: number }>;
}

describe('Runs', () => {
  const client = getClient();
  let workspace: Workspace;
  let agentId: string;

  beforeAll(async () => {
    workspace = await createTestWorkspace('agent-runs');
    const agent = await client.agents.create(workspace.external_id, {
      agentKey: uniqueName('run-agent'),
      name: uniqueName('Run Agent'),
    });
    agentId = agent.id;
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('retrieves run state, snapshots, and events', async () => {
    const accepted = await client.tasks.create(workspace.external_id, {
      message: 'create run for runs.test.ts',
      sessionId: uniqueName('run-session'),
      agentId,
      idempotencyKey: uniqueName('run-idem'),
      timeoutMs: 60_000,
    });

    const runId =
      accepted.run_id ??
      accepted.task.target_run_id ??
      (await waitForRunIdForTask(workspace.external_id, accepted.task.id, { timeoutMs: 60_000 }));
    if (!runId) {
      const task = await client.tasks.retrieve(workspace.external_id, accepted.task.id);
      throw new Error(
        `run_id did not materialize for task ${accepted.task.id} (state=${task.state}, target_run_id=${task.target_run_id ?? 'none'}, dropped_reason=${task.dropped_reason ?? 'none'})`,
      );
    }

    const run = await client.runs.retrieve(workspace.external_id, runId);
    expect(run.id).toBe(runId);

    const snapshots = await waitForRunSnapshots(workspace.external_id, runId);
    expect(Array.isArray(snapshots)).toBe(true);
    expect(snapshots.length).toBeGreaterThan(0);
    expect(snapshots.every((snapshot) => snapshot.run_id === runId)).toBe(true);
    expect(snapshots.every((snapshot, idx) => idx === 0 || snapshot.seq > snapshots[idx - 1]!.seq)).toBe(true);

    const events = await client.runs.listEvents(workspace.external_id, runId);
    expect(Array.isArray(events)).toBe(true);
  }, 90_000);

  it('accepts run input without explicit idempotency key', async () => {
    const accepted = await client.tasks.create(workspace.external_id, {
      message: 'create run for run-input defaults',
      sessionId: uniqueName('run-input-session'),
      agentId,
      idempotencyKey: uniqueName('run-input-idem'),
      timeoutMs: 60_000,
    });

    const runId =
      accepted.run_id ??
      accepted.task.target_run_id ??
      (await waitForRunIdForTask(workspace.external_id, accepted.task.id, { timeoutMs: 60_000 }));
    if (!runId) {
      const task = await client.tasks.retrieve(workspace.external_id, accepted.task.id);
      throw new Error(
        `run_id did not materialize for task ${accepted.task.id} (state=${task.state}, target_run_id=${task.target_run_id ?? 'none'}, dropped_reason=${task.dropped_reason ?? 'none'})`,
      );
    }

    const inputAccepted = await client.runs.input(workspace.external_id, runId, {
      message: 'followup without explicit idempotency key',
      queueMode: 'followup',
    });

    expect(inputAccepted.accepted).toBe(true);
    expect(inputAccepted.task.idempotency_key.length).toBeGreaterThan(0);
    expect(inputAccepted.task.target_run_id ?? inputAccepted.run_id).toBe(runId);
  }, 90_000);
});
