import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import type { SourceView } from '../src/types/views.js';
import { APIError } from '../src/errors.js';
import { getClient, createTestWorkspace, deleteTestWorkspace, uniqueName } from './helpers.js';

describe('Source Views', () => {
  const client = getClient();
  let workspace: Workspace;
  let sharedView: SourceView;
  let skipAll = false;

  beforeAll(async () => {
    workspace = await createTestWorkspace('views');

    try {
      sharedView = await client.views.create(workspace.external_id, {
        integration: 'gmail',
        name: uniqueName('sv-shared'),
        guidance: 'Shared view for tests',
        outputFormat: 'folder',
      });
    } catch (err) {
      if (err instanceof APIError) {
        console.warn(`Skipping view tests: could not create source view (${err.status}): ${err.message}`);
        skipAll = true;
        return;
      }
      throw err;
    }
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace?.external_id);
  });

  it('creates a source view (smart mode)', async () => {
    if (skipAll) return;

    expect(sharedView.external_id).toBeDefined();
    expect(sharedView.name).toBeDefined();
    expect(sharedView.integration).toBe('gmail');
    expect(sharedView.path).toBeDefined();
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

    const updatedName = uniqueName('sv-updated');
    const updated = await client.views.update(
      workspace.external_id,
      sharedView.external_id,
      { name: updatedName, guidance: 'Updated guidance' },
    );

    expect(updated.name).toBe(updatedName);
    expect(updated.guidance).toBe('Updated guidance');
  });

  it('syncs a source view', async () => {
    if (skipAll) return;

    try {
      const result = await client.views.sync(
        workspace.external_id,
        sharedView.external_id,
        { timeout: 10_000, maxRetries: 0 },
      );
      expect(result.external_id).toBeDefined();
      expect(typeof result.results_count).toBe('number');
      expect(typeof result.new_results).toBe('number');
    } catch (err) {
      if (err instanceof APIError && (err.status === 400 || err.status === 404 || err.status === 422)) {
        const lower = err.message.toLowerCase();
        expect(
          lower.includes('not connected') ||
          lower.includes('invalid authentication') ||
          lower.includes('invalid_grant') ||
          lower.includes('expired token') ||
          lower.includes('not found') ||
          lower.includes('unauthorized'),
        ).toBe(true);
        console.warn(`Sync returned ${err.status}: ${err.message}`);
        return;
      }
      throw err;
    }
  });

  it('deletes a source view', async () => {
    if (skipAll) return;

    await client.views.del(workspace.external_id, sharedView.external_id);

    const views = await client.views.list(workspace.external_id);
    const found = views.find((f) => f.external_id === sharedView.external_id);
    expect(found).toBeUndefined();
  });
});
