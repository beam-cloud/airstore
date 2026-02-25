import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import { getClient, createTestWorkspace, deleteTestWorkspace, uniqueName } from './helpers.js';

describe('Tokens', () => {
  const client = getClient();
  let workspace: Workspace;

  beforeAll(async () => {
    workspace = await createTestWorkspace('tokens');
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('creates a token with email (auto-creates member)', async () => {
    const result = await client.tokens.create(workspace.external_id, {
      email: `test-${Date.now()}@sdk-test.local`,
      name: uniqueName('tok'),
    });

    expect(result.token).toBeDefined();
    expect(typeof result.token).toBe('string');
    expect(result.token.length).toBeGreaterThan(0);
    expect(result.info).toBeDefined();
    expect(result.info.external_id).toBeDefined();
    expect(result.info.name).toBeDefined();
  });

  it('lists tokens in a workspace', async () => {
    // Create a token first to ensure at least one exists
    await client.tokens.create(workspace.external_id, {
      email: `list-${Date.now()}@sdk-test.local`,
      name: uniqueName('tok-list'),
    });

    const tokens = await client.tokens.list(workspace.external_id);
    expect(Array.isArray(tokens)).toBe(true);
    expect(tokens.length).toBeGreaterThan(0);

    const first = tokens[0]!;
    expect(first.external_id).toBeDefined();
    expect(first.token_type).toBeDefined();
    expect(first.created_at).toBeDefined();
  });

  it('revokes a token', async () => {
    const created = await client.tokens.create(workspace.external_id, {
      email: `revoke-${Date.now()}@sdk-test.local`,
      name: uniqueName('tok-revoke'),
    });

    // Revoke should not throw
    await client.tokens.revoke(workspace.external_id, created.info.external_id);

    // After revocation, the token should no longer appear in the list
    const tokens = await client.tokens.list(workspace.external_id);
    const found = tokens.find((t) => t.external_id === created.info.external_id);
    expect(found).toBeUndefined();
  });
});
