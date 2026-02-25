import { afterAll, beforeAll, describe, expect, it } from 'vitest';
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
});
