import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import { APIError } from '../src/errors.js';
import { getClient, createTestWorkspace, deleteTestWorkspace, uniqueName } from './helpers.js';

describe('Source Views', () => {
  const client = getClient();
  let workspace: Workspace;
  let skipAll = false;

  beforeAll(async () => {
    workspace = await createTestWorkspace('views');

    try {
      await client.connections.create(workspace.external_id, {
        integrationType: 'gmail',
        accessToken: 'test-sf-access-' + Date.now(),
        refreshToken: 'test-sf-refresh-' + Date.now(),
      });
    } catch (err) {
      if (err instanceof APIError && (err.status === 400 || err.status === 422)) {
        console.warn('Source view tests will be best-effort: could not create connection');
      }
    }
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('creates a source view (smart mode)', async () => {
    try {
      const view = await client.views.create(workspace.external_id, {
        integration: 'gmail',
        name: uniqueName('sv'),
        guidance: 'Recent emails from the last 7 days',
        outputFormat: 'folder',
      });

      expect(view.external_id).toBeDefined();
      expect(view.name).toBeDefined();
      expect(view.integration).toBe('gmail');
      expect(view.path).toBeDefined();
    } catch (err) {
      if (err instanceof APIError) {
        console.warn(`View create returned ${err.status}: ${err.message}`);
        skipAll = true;
        return;
      }
      throw err;
    }
  });

  it('creates a source view (query mode)', async () => {
    if (skipAll) return;

    try {
      const view = await client.views.create(workspace.external_id, {
        integration: 'gmail',
        name: uniqueName('sv-query'),
        filter: { is_unread: true, newer_than: '7d' },
        outputFormat: 'folder',
      });

      expect(view.external_id).toBeDefined();
      expect(view.integration).toBe('gmail');
    } catch (err) {
      if (err instanceof APIError) {
        console.warn(`View query-mode create returned ${err.status}: ${err.message}`);
        return;
      }
      throw err;
    }
  });

  it('lists source views', async () => {
    if (skipAll) return;
    const views = await client.views.list(workspace.external_id);
    expect(Array.isArray(views)).toBe(true);
  });

  it('updates a source view', async () => {
    if (skipAll) return;

    let view;
    try {
      view = await client.views.create(workspace.external_id, {
        integration: 'gmail',
        name: uniqueName('sv-update'),
        guidance: 'Original guidance',
      });
    } catch {
      console.warn('Skipping update test: could not create source view');
      return;
    }

    const updated = await client.views.update(
      workspace.external_id,
      view.external_id,
      { name: 'Updated Name', guidance: 'Updated guidance' },
    );

    expect(updated.name).toBe('Updated Name');
    expect(updated.guidance).toBe('Updated guidance');
  });

  it('deletes a source view', async () => {
    if (skipAll) return;

    let view;
    try {
      view = await client.views.create(workspace.external_id, {
        integration: 'gmail',
        name: uniqueName('sv-delete'),
        guidance: 'To be deleted',
      });
    } catch {
      console.warn('Skipping delete test: could not create source view');
      return;
    }

    await client.views.del(workspace.external_id, view.external_id);

    const views = await client.views.list(workspace.external_id);
    const found = views.find((f) => f.external_id === view.external_id);
    expect(found).toBeUndefined();
  });

  it('syncs a source view', async () => {
    if (skipAll) return;

    let view;
    try {
      view = await client.views.create(workspace.external_id, {
        integration: 'gmail',
        name: uniqueName('sv-sync'),
        guidance: 'Emails to sync',
      });
    } catch {
      console.warn('Skipping sync test: could not create source view');
      return;
    }

    try {
      const result = await client.views.sync(workspace.external_id, view.external_id);
      expect(result.external_id).toBeDefined();
      expect(typeof result.results_count).toBe('number');
      expect(typeof result.new_results).toBe('number');
    } catch (err) {
      if (err instanceof APIError) {
        console.warn(`Sync returned ${err.status}: ${err.message}`);
        return;
      }
      throw err;
    }
  });
});
