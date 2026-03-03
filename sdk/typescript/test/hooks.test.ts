import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import type { Hook } from '../src/types/hooks.js';
import { createTestWorkspace, deleteTestWorkspace, getClient, uniqueName } from './helpers.js';

describe('Hooks', () => {
  const client = getClient();
  let workspace: Workspace;
  let createdHook: Hook;

  beforeAll(async () => {
    workspace = await createTestWorkspace('hooks');
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('creates a hook with default event types', async () => {
    createdHook = await client.hooks.create(workspace.external_id, {
      path: `/sources/test-${uniqueName('hook')}`,
      prompt: 'Review new files and summarize changes.',
    });

    expect(createdHook.external_id).toBeDefined();
    expect(createdHook.path).toBeDefined();
    expect(createdHook.prompt).toBe('Review new files and summarize changes.');
    expect(createdHook.active).toBe(true);
    expect(createdHook.event_types).toContain('fs.create');
  });

  it('creates a hook with custom event types', async () => {
    const hook = await client.hooks.create(workspace.external_id, {
      path: `/sources/test-${uniqueName('hook-events')}`,
      prompt: 'Handle file changes.',
      eventTypes: ['fs.create', 'fs.write', 'fs.delete'],
    });

    expect(hook.event_types).toEqual(
      expect.arrayContaining(['fs.create', 'fs.write', 'fs.delete']),
    );
    expect(hook.event_types).toHaveLength(3);

    await client.hooks.delete(workspace.external_id, hook.external_id);
  });

  it('lists hooks in a workspace', async () => {
    const list = await client.hooks.list(workspace.external_id);
    expect(Array.isArray(list)).toBe(true);
    expect(list.length).toBeGreaterThanOrEqual(1);
    expect(list.some((h) => h.external_id === createdHook.external_id)).toBe(true);
  });

  it('retrieves a hook by ID', async () => {
    const fetched = await client.hooks.retrieve(
      workspace.external_id,
      createdHook.external_id,
    );
    expect(fetched.external_id).toBe(createdHook.external_id);
    expect(fetched.prompt).toBe(createdHook.prompt);
  });

  it('updates a hook', async () => {
    const updated = await client.hooks.update(
      workspace.external_id,
      createdHook.external_id,
      {
        prompt: 'Updated: review and tag all new files.',
        eventTypes: ['fs.create', 'fs.write'],
      },
    );

    expect(updated.prompt).toBe('Updated: review and tag all new files.');
    expect(updated.event_types).toEqual(
      expect.arrayContaining(['fs.create', 'fs.write']),
    );
  });

  it('deactivates a hook', async () => {
    const updated = await client.hooks.update(
      workspace.external_id,
      createdHook.external_id,
      { active: false },
    );
    expect(updated.active).toBe(false);
  });

  it('deletes a hook', async () => {
    await client.hooks.delete(workspace.external_id, createdHook.external_id);

    const list = await client.hooks.list(workspace.external_id);
    expect(list.some((h) => h.external_id === createdHook.external_id)).toBe(false);
  });
});
