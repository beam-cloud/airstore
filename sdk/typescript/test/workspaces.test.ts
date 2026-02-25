import { describe, it, expect, afterAll } from 'vitest';
import { NotFoundError } from '../src/errors.js';
import { getClient, uniqueName } from './helpers.js';

describe('Workspaces', () => {
  const client = getClient();
  const createdIds: string[] = [];

  afterAll(async () => {
    // Clean up all workspaces created during tests
    for (const id of createdIds) {
      try {
        await client.workspaces.del(id);
      } catch {
        // ignore
      }
    }
  });

  it('creates a workspace', async () => {
    const name = uniqueName('ws-test');
    const ws = await client.workspaces.create({ name });

    expect(ws.external_id).toBeDefined();
    expect(ws.name).toBe(name);
    expect(ws.created_at).toBeDefined();
    expect(ws.updated_at).toBeDefined();
    createdIds.push(ws.external_id);
  });

  it('lists workspaces and includes the created one', async () => {
    const name = uniqueName('ws-list');
    const ws = await client.workspaces.create({ name });
    createdIds.push(ws.external_id);

    const list = await client.workspaces.list();
    expect(Array.isArray(list)).toBe(true);

    const found = list.find((w) => w.external_id === ws.external_id);
    expect(found).toBeDefined();
    expect(found!.name).toBe(name);
  });

  it('retrieves a workspace by ID', async () => {
    const name = uniqueName('ws-retrieve');
    const ws = await client.workspaces.create({ name });
    createdIds.push(ws.external_id);

    const fetched = await client.workspaces.retrieve(ws.external_id);
    expect(fetched.external_id).toBe(ws.external_id);
    expect(fetched.name).toBe(name);
  });

  it('deletes a workspace', async () => {
    const ws = await client.workspaces.create({ name: uniqueName('ws-delete') });

    // Should not throw
    await client.workspaces.del(ws.external_id);

    // Retrieving deleted workspace should 404
    await expect(client.workspaces.retrieve(ws.external_id)).rejects.toThrow(
      NotFoundError,
    );
  });

  it('throws NotFoundError for non-existent workspace', async () => {
    await expect(
      client.workspaces.retrieve('00000000-0000-0000-0000-000000000000'),
    ).rejects.toThrow(NotFoundError);
  });

  it('attaches lastResponse metadata', async () => {
    const ws = await client.workspaces.create({ name: uniqueName('ws-meta') });
    createdIds.push(ws.external_id);

    const meta = (ws as any).lastResponse;
    expect(meta).toBeDefined();
    expect(meta.statusCode).toBe(201);
    expect(meta.headers).toBeDefined();
  });
});
