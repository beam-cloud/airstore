import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import { getClient, createTestWorkspace, deleteTestWorkspace, uniqueName } from './helpers.js';

describe('Members', () => {
  const client = getClient();
  let workspace: Workspace;

  beforeAll(async () => {
    workspace = await createTestWorkspace('members');
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('creates a member', async () => {
    const email = `${uniqueName('member')}@sdk-test.local`;
    const member = await client.members.create(workspace.external_id, {
      email,
      name: 'Test Member',
      role: 'member',
    });

    expect(member.external_id).toBeDefined();
    expect(member.email).toBe(email);
    expect(member.name).toBe('Test Member');
    expect(member.role).toBe('member');
    expect(member.created_at).toBeDefined();
  });

  it('lists members and includes the created one', async () => {
    const email = `${uniqueName('member-list')}@sdk-test.local`;
    const created = await client.members.create(workspace.external_id, {
      email,
      name: 'List Test',
    });

    const members = await client.members.list(workspace.external_id);
    expect(Array.isArray(members)).toBe(true);
    expect(members.length).toBeGreaterThan(0);

    const found = members.find((m) => m.external_id === created.external_id);
    expect(found).toBeDefined();
    expect(found!.email).toBe(email);
  });

  it('deletes a member', async () => {
    const created = await client.members.create(workspace.external_id, {
      email: `${uniqueName('member-del')}@sdk-test.local`,
      name: 'Delete Test',
    });

    // Delete should not throw
    await client.members.del(workspace.external_id, created.external_id);

    // After deletion, the member should no longer appear in the list
    const members = await client.members.list(workspace.external_id);
    const found = members.find((m) => m.external_id === created.external_id);
    expect(found).toBeUndefined();
  });

  it('creates a member with admin role', async () => {
    const member = await client.members.create(workspace.external_id, {
      email: `${uniqueName('admin')}@sdk-test.local`,
      name: 'Admin Test',
      role: 'admin',
    });

    expect(member.role).toBe('admin');
  });
});
