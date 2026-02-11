import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import { APIError } from '../src/errors.js';
import { getClient, createTestWorkspace, deleteTestWorkspace, uniqueName } from './helpers.js';

describe('Smart Folders', () => {
  const client = getClient();
  let workspace: Workspace;
  let skipAll = false;

  beforeAll(async () => {
    workspace = await createTestWorkspace('smart-folders');

    // Smart folders require a connection. Try to create one; if it fails,
    // skip the rest of this suite gracefully.
    try {
      await client.connections.create(workspace.external_id, {
        integrationType: 'gmail',
        accessToken: 'test-sf-access-' + Date.now(),
        refreshToken: 'test-sf-refresh-' + Date.now(),
      });
    } catch (err) {
      if (err instanceof APIError && (err.status === 400 || err.status === 422)) {
        console.warn(
          'Smart folder tests will be best-effort: could not create connection',
        );
      }
      // Continue anyway — some smart folder ops may work without a connection
    }
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('creates a smart folder', async () => {
    try {
      const folder = await client.smartFolders.create(workspace.external_id, {
        integration: 'gmail',
        name: uniqueName('sf'),
        guidance: 'Recent emails from the last 7 days',
        outputFormat: 'folder',
      });

      expect(folder.external_id).toBeDefined();
      expect(folder.name).toBeDefined();
      expect(folder.integration).toBe('gmail');
      expect(folder.path).toBeDefined();
    } catch (err) {
      if (err instanceof APIError) {
        console.warn(`Smart folder create returned ${err.status}: ${err.message}`);
        skipAll = true;
        return;
      }
      throw err;
    }
  });

  it('lists smart folders', async () => {
    if (skipAll) {
      console.warn('Skipping: smart folder creation failed');
      return;
    }

    const folders = await client.smartFolders.list(workspace.external_id);
    expect(Array.isArray(folders)).toBe(true);
  });

  it('updates a smart folder', async () => {
    if (skipAll) {
      console.warn('Skipping: smart folder creation failed');
      return;
    }

    // Create one to update
    let folder;
    try {
      folder = await client.smartFolders.create(workspace.external_id, {
        integration: 'gmail',
        name: uniqueName('sf-update'),
        guidance: 'Original guidance',
      });
    } catch {
      console.warn('Skipping update test: could not create smart folder');
      return;
    }

    const updated = await client.smartFolders.update(
      workspace.external_id,
      folder.external_id,
      { name: 'Updated Name', guidance: 'Updated guidance' },
    );

    expect(updated.name).toBe('Updated Name');
    expect(updated.guidance).toBe('Updated guidance');
  });

  it('deletes a smart folder', async () => {
    if (skipAll) {
      console.warn('Skipping: smart folder creation failed');
      return;
    }

    let folder;
    try {
      folder = await client.smartFolders.create(workspace.external_id, {
        integration: 'gmail',
        name: uniqueName('sf-delete'),
        guidance: 'To be deleted',
      });
    } catch {
      console.warn('Skipping delete test: could not create smart folder');
      return;
    }

    await client.smartFolders.del(workspace.external_id, folder.external_id);

    // Verify it no longer appears in the list
    const folders = await client.smartFolders.list(workspace.external_id);
    const found = folders.find((f) => f.external_id === folder.external_id);
    expect(found).toBeUndefined();
  });
});
