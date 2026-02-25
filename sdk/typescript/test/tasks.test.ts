import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import { createTestWorkspace, deleteTestWorkspace, getClient, uniqueName } from './helpers.js';

describe('Orchestration Tasks', () => {
  const client = getClient();
  let workspace: Workspace;
  let agentId: string;

  beforeAll(async () => {
    workspace = await createTestWorkspace('agent-tasks');
    const agent = await client.agents.create(workspace.external_id, {
      agentKey: uniqueName('task-agent'),
      name: uniqueName('Task Agent'),
      config: { model: 'claude-sonnet-4' },
    });
    agentId = agent.id;
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('accepts a task and supports idempotent replay', async () => {
    const idempotencyKey = uniqueName('idem');
    const sessionId = uniqueName('session');
    const accepted = await client.tasks.create(workspace.external_id, {
      message: 'hello from tasks test',
      sessionId,
      agentId,
      idempotencyKey,
      timeoutMs: 60_000,
    });

    expect(accepted.accepted).toBe(true);
    expect(accepted.task.id).toBeDefined();
    expect(accepted.task.idempotency_key).toBe(idempotencyKey);

    const fetched = await client.tasks.retrieve(
      workspace.external_id,
      accepted.task.id,
    );
    expect(fetched.id).toBe(accepted.task.id);
    expect(fetched.workspace_id).toBeDefined();

    const replay = await client.tasks.create(workspace.external_id, {
      message: 'hello from tasks test',
      sessionId,
      agentId,
      idempotencyKey,
      timeoutMs: 60_000,
    });

    expect(replay.idempotent_hit).toBe(true);
    expect(replay.task.id).toBe(accepted.task.id);
  });

  it('generates session and idempotency ids when omitted', async () => {
    const accepted = await client.tasks.create(workspace.external_id, {
      message: 'generate defaults for ids',
      agentId,
      timeoutMs: 60_000,
    });

    expect(accepted.accepted).toBe(true);
    expect(accepted.task.idempotency_key.length).toBeGreaterThan(0);

    const fetched = await client.tasks.retrieve(workspace.external_id, accepted.task.id);
    const sessionId = fetched.payload_json['session_id'];
    expect(typeof sessionId).toBe('string');
    expect((sessionId as string).length).toBeGreaterThan(0);
  });
});
