import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import {
  createTestWorkspace,
  deleteTestWorkspace,
  getClient,
  uniqueName,
  waitForRunIdForTask,
} from './helpers.js';

describe('Channels', () => {
  const client = getClient();
  let workspace: Workspace;
  let agentId: string;

  beforeAll(async () => {
    workspace = await createTestWorkspace('channels');
    const agent = await client.agents.create(workspace.external_id, {
      agentKey: uniqueName('channel-agent'),
      name: uniqueName('Channel Agent'),
    });
    agentId = agent.id;
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('sends direct messages to agents and runs', async () => {
    const initial = await client.channels.sendDirectAgentMessage(
      workspace.external_id,
      agentId,
      {
        message: 'seed direct channel task',
        sessionId: uniqueName('direct-session'),
        idempotencyKey: uniqueName('direct-idem'),
        timeoutMs: 60_000,
      },
    );
    expect(initial.accepted).toBe(true);
    expect(initial.task.id).toBeDefined();

    const runId =
      initial.run_id ??
      initial.task.target_run_id ??
      (await waitForRunIdForTask(workspace.external_id, initial.task.id, { timeoutMs: 60_000 }));
    if (!runId) {
      const task = await client.tasks.retrieve(workspace.external_id, initial.task.id);
      throw new Error(
        `run_id did not materialize for task ${initial.task.id} (state=${task.state}, target_run_id=${task.target_run_id ?? 'none'}, dropped_reason=${task.dropped_reason ?? 'none'})`,
      );
    }

    const followup = await client.channels.sendDirectRunMessage(
      workspace.external_id,
      runId,
      {
        message: 'follow-up from direct channels test',
        queueMode: 'followup',
      },
    );
    expect(followup.accepted).toBe(true);
    expect(followup.task.id).toBeDefined();
    expect(followup.task.target_run_id ?? followup.run_id).toBe(runId);
  }, 90_000);
});
