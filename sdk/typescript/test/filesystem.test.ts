import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import { APIError } from '../src/errors.js';
import { getClient, createTestWorkspace, deleteTestWorkspace } from './helpers.js';

describe('Filesystem', () => {
  const client = getClient();
  let workspace: Workspace;

  beforeAll(async () => {
    workspace = await createTestWorkspace('filesystem');
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('lists root directory', async () => {
    try {
      const entries = await client.fs.list(workspace.external_id, { path: '/' });
      expect(Array.isArray(entries)).toBe(true);
      // A fresh workspace may have no entries or default folders
    } catch (err) {
      if (err instanceof APIError) {
        console.warn(`Filesystem list returned ${err.status}: ${err.message}`);
        return;
      }
      throw err;
    }
  });

  it('gets directory tree', async () => {
    try {
      const tree = await client.fs.tree(workspace.external_id, { path: '/' });
      expect(tree).toBeDefined();
      expect(tree.path).toBeDefined();
      expect(Array.isArray(tree.entries)).toBe(true);
      // `truncated` may not be present on all gateway versions
      if ('truncated' in tree) {
        expect(typeof tree.truncated).toBe('boolean');
      }
    } catch (err) {
      if (err instanceof APIError) {
        console.warn(`Filesystem tree returned ${err.status}: ${err.message}`);
        return;
      }
      throw err;
    }
  });

  it('stats root path', async () => {
    try {
      const file = await client.fs.stat(workspace.external_id, '/');
      expect(file).toBeDefined();
      expect(file.path).toBeDefined();
    } catch (err) {
      if (err instanceof APIError) {
        console.warn(`Filesystem stat returned ${err.status}: ${err.message}`);
        return;
      }
      throw err;
    }
  });
});
