import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NotFoundError } from '../src/errors.js';
import type { Workspace } from '../src/types/workspaces.js';
import { createTestWorkspace, deleteTestWorkspace, getClient, uniqueName } from './helpers.js';

describe('Agents', () => {
  const client = getClient();
  let workspace: Workspace;

  beforeAll(async () => {
    workspace = await createTestWorkspace('agents');
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('creates and retrieves an agent profile', async () => {
    const created = await client.agents.create(workspace.external_id, {
      agentKey: uniqueName('agent-key'),
      name: uniqueName('agent-name'),
      runner: 'claude_code',
      config: { model: 'claude-sonnet-4' },
    });

    expect(created.id).toBeDefined();
    expect(created.agent_key).toBeDefined();
    expect(created.name).toBeDefined();

    const fetched = await client.agents.retrieve(workspace.external_id, created.id);
    expect(fetched.id).toBe(created.id);
    expect(fetched.agent_key).toBe(created.agent_key);
    expect(fetched.config_json.runner).toBe('claude_code');
    expect(fetched.config_json.provider).toBe('claude');
  });

  it('lists workspace agent profiles', async () => {
    const list = await client.agents.list(workspace.external_id);
    expect(Array.isArray(list)).toBe(true);
  });

  it('updates an agent profile', async () => {
    const created = await client.agents.create(workspace.external_id, {
      agentKey: uniqueName('agent-update'),
      name: 'Original Name',
      runner: 'claude_code',
    });

    const updated = await client.agents.update(workspace.external_id, created.id, {
      name: 'Updated Name',
      config: { model: 'claude-sonnet-4-6' },
    });

    expect(updated.id).toBe(created.id);
    expect(updated.name).toBe('Updated Name');
    expect(updated.config_json.model).toBe('claude-sonnet-4-6');
  });

  it('deletes an agent profile', async () => {
    const created = await client.agents.create(workspace.external_id, {
      agentKey: uniqueName('agent-delete'),
      name: uniqueName('Delete Agent'),
      runner: 'claude_code',
    });

    await client.agents.delete(workspace.external_id, created.id);

    await expect(
      client.agents.retrieve(workspace.external_id, created.id),
    ).rejects.toThrow(NotFoundError);
  });
});
