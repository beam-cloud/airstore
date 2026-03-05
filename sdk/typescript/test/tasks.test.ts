import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import { APIError } from '../src/errors.js';
import { createTestWorkspace, deleteTestWorkspace, getClient, uniqueName } from './helpers.js';

describe('Orchestration Tasks', () => {
  const client = getClient();
  let workspace: Workspace;
  let agentId: string;
  let firstTaskId: string;

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
    firstTaskId = accepted.task.id;

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

  it('archives a task', async () => {
    const ARCHIVABLE = new Set(['done', 'dropped', 'cancelled', 'idle']);

    // Reuse the task from the first test -- it has had time to settle
    // while the other tests ran.
    let taskId = firstTaskId;
    let task = await client.tasks.retrieve(workspace.external_id, taskId);

    if (!ARCHIVABLE.has(task.state)) {
      if (task.state === 'running') {
        try {
          await client.tasks.cancel(workspace.external_id, taskId);
        } catch (err) {
          if (!(err instanceof APIError)) throw err;
        }
      }
      await new Promise((r) => setTimeout(r, 2_000));
      task = await client.tasks.retrieve(workspace.external_id, taskId);
    }

    if (!ARCHIVABLE.has(task.state)) {
      const accepted = await client.tasks.create(workspace.external_id, {
        message: 'archive fallback',
        agentId,
        idempotencyKey: uniqueName('archive-idem'),
        timeoutMs: 5_000,
      });
      taskId = accepted.task.id;
      const deadline = Date.now() + 20_000;
      while (Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 3_000));
        task = await client.tasks.retrieve(workspace.external_id, taskId);
        if (ARCHIVABLE.has(task.state)) break;
        if (task.state === 'running') {
          try {
            await client.tasks.cancel(workspace.external_id, taskId);
          } catch (err) {
            if (!(err instanceof APIError)) throw err;
          }
        }
      }
    }

    expect(
      ARCHIVABLE.has(task.state),
      `expected task ${taskId} to reach an archivable state, but stuck in '${task.state}'`,
    ).toBe(true);

    const result = await client.tasks.archive(workspace.external_id, taskId);
    expect(result.status).toBe('archived');
  }, 60_000);
});
