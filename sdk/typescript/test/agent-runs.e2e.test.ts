import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import { createTestWorkspace, deleteTestWorkspace, getClient, uniqueName } from './helpers.js';

const TERMINAL = new Set(['ok', 'error', 'timeout', 'cancelled']);

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

describe('Agent/Runs E2E', () => {
  const client = getClient();
  let workspace: Workspace;
  let agentId: string;

  beforeAll(async () => {
    workspace = await createTestWorkspace('agent-runs-e2e');
    const agent = await client.agents.create(workspace.external_id, {
      agentKey: uniqueName('e2e-agent'),
      name: uniqueName('E2E Agent'),
      config: { model: 'claude-sonnet-4' },
    });
    agentId = agent.id;
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('runs envelope -> run -> attempts/snapshots with idempotent replay', async () => {
    const idempotencyKey = uniqueName('e2e-idem');
    const sessionId = uniqueName('e2e-session');

    const accepted = await client.tasks.create(workspace.external_id, {
      message: 'agent/runs e2e test prompt',
      sessionId,
      agentId,
      idempotencyKey,
      timeoutMs: 120_000,
    });
    expect(accepted.accepted).toBe(true);

    const runId =
      accepted.run_id ??
      (await waitForRunId(workspace.external_id, accepted.envelope.id));
    expect(runId).toBeDefined();
    if (!runId) return;

    const deadline = Date.now() + 90_000;
    let run = await client.runs.retrieve(workspace.external_id, runId);
    while (!TERMINAL.has(run.status) && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 1500));
      run = await client.runs.retrieve(workspace.external_id, runId);
    }
    expect(run.id).toBe(runId);
    expect(['accepted', 'running', 'ok', 'error', 'timeout', 'cancelled']).toContain(
      run.status,
    );

    const attempts = await client.runs.listAttempts(workspace.external_id, runId);
    const snapshots = await client.runs.listSnapshots(workspace.external_id, runId);
    expect(snapshots.length).toBeGreaterThan(0);
    expect(attempts.every((attempt) => attempt.run_id === runId)).toBe(true);
    expect(snapshots.every((snapshot) => snapshot.run_id === runId)).toBe(true);

    const replay = await client.tasks.create(workspace.external_id, {
      message: 'agent/runs e2e test prompt',
      sessionId,
      agentId,
      idempotencyKey,
      timeoutMs: 120_000,
    });
    expect(replay.idempotent_hit).toBe(true);
    expect(replay.envelope.id).toBe(accepted.envelope.id);
  });
});
