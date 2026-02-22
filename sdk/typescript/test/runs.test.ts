import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import { createTestWorkspace, deleteTestWorkspace, getClient, uniqueName } from './helpers.js';

async function waitForRunId(
  workspaceId: string,
  envelopeId: string,
  timeoutMs = 30000,
): Promise<string | undefined> {
  const client = getClient();
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const envelope = await client.tasks.retrieve(workspaceId, envelopeId);
    if (envelope.target_run_id) return envelope.target_run_id;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  return undefined;
}

describe('Runs', () => {
  const client = getClient();
  let workspace: Workspace;
  let agentId: string;

  beforeAll(async () => {
    workspace = await createTestWorkspace('orchestration-runs');
    const agent = await client.agents.create(workspace.external_id, {
      agentKey: uniqueName('run-agent'),
      name: uniqueName('Run Agent'),
    });
    agentId = agent.id;
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('retrieves run state, attempts, snapshots, and events', async () => {
    const accepted = await client.tasks.create(workspace.external_id, {
      message: 'create run for runs.test.ts',
      sessionId: uniqueName('run-session'),
      agentId,
      idempotencyKey: uniqueName('run-idem'),
      timeoutMs: 60_000,
    });

    const runId =
      accepted.run_id ??
      (await waitForRunId(workspace.external_id, accepted.envelope.id));
    expect(runId).toBeDefined();
    if (!runId) return;

    const run = await client.runs.retrieve(workspace.external_id, runId);
    expect(run.id).toBe(runId);

    const attempts = await client.runs.listAttempts(workspace.external_id, runId);
    expect(Array.isArray(attempts)).toBe(true);
    expect(attempts.every((attempt) => attempt.run_id === runId)).toBe(true);

    const snapshots = await client.runs.listSnapshots(workspace.external_id, runId);
    expect(Array.isArray(snapshots)).toBe(true);
    expect(snapshots.length).toBeGreaterThan(0);
    expect(snapshots.every((snapshot) => snapshot.run_id === runId)).toBe(true);

    const events = await client.runs.listEvents(workspace.external_id, runId);
    expect(Array.isArray(events)).toBe(true);
  });
});
