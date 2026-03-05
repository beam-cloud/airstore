import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import type { Hook } from '../src/types/hooks.js';
import { APIError } from '../src/errors.js';
import { createTestWorkspace, deleteTestWorkspace, getClient, uniqueName } from './helpers.js';

describe('Hooks', () => {
  const client = getClient();
  let workspace: Workspace;
  let hookBasePath: string;
  let createdHook: Hook | undefined;
  let skipAll = false;

  beforeAll(async () => {
    workspace = await createTestWorkspace('hooks');

    try {
      const view = await client.views.create(workspace.external_id, {
        integration: 'gmail',
        name: uniqueName('hook-view'),
        filter: { newer_than: '7d' },
        outputFormat: 'folder',
      });
      hookBasePath = view.path ?? `/sources/${view.external_id}`;
    } catch (err) {
      if (err instanceof APIError) {
        console.warn(`Skipping hooks tests: could not create source view (${err.status})`);
        skipAll = true;
        return;
      }
      throw err;
    }
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('creates a hook with default event types', async () => {
    if (skipAll) return;

    createdHook = await client.hooks.create(workspace.external_id, {
      path: hookBasePath,
      prompt: 'Review new files and summarize changes.',
    });

    expect(createdHook.external_id).toBeDefined();
    expect(createdHook.path).toBeDefined();
    expect(createdHook.prompt).toBe('Review new files and summarize changes.');
    expect(createdHook.active).toBe(true);
    expect(createdHook.event_types).toContain('fs.create');
  });

  it('creates a hook with custom event types', async () => {
    if (skipAll) return;

    let secondPath: string;
    try {
      const view2 = await client.views.create(workspace.external_id, {
        integration: 'gmail',
        name: uniqueName('hook-view2'),
        filter: { newer_than: '7d' },
        outputFormat: 'folder',
      });
      secondPath = view2.path ?? `/sources/${view2.external_id}`;
    } catch (err) {
      if (err instanceof APIError) {
        console.warn(`Skipping custom event types test: could not create second source view (${err.status})`);
        return;
      }
      throw err;
    }

    const hook = await client.hooks.create(workspace.external_id, {
      path: secondPath,
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
    if (skipAll || !createdHook) return;

    const list = await client.hooks.list(workspace.external_id);
    expect(Array.isArray(list)).toBe(true);
    expect(list.length).toBeGreaterThanOrEqual(1);
    expect(list.some((h) => h.external_id === createdHook!.external_id)).toBe(true);
  });

  it('retrieves a hook by ID', async () => {
    if (skipAll || !createdHook) return;

    const fetched = await client.hooks.retrieve(
      workspace.external_id,
      createdHook.external_id,
    );
    expect(fetched.external_id).toBe(createdHook.external_id);
    expect(fetched.prompt).toBe(createdHook.prompt);
  });

  it('updates a hook', async () => {
    if (skipAll || !createdHook) return;

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
    if (skipAll || !createdHook) return;

    const updated = await client.hooks.update(
      workspace.external_id,
      createdHook.external_id,
      { active: false },
    );
    expect(updated.active).toBe(false);
  });

  it('deletes a hook', async () => {
    if (skipAll || !createdHook) return;

    await client.hooks.delete(workspace.external_id, createdHook.external_id);

    const list = await client.hooks.list(workspace.external_id);
    expect(list.some((h) => h.external_id === createdHook!.external_id)).toBe(false);
  });
});
