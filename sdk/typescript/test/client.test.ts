import { describe, it, expect } from 'vitest';
import { Airstore } from '../src/airstore.js';
import { AirstoreError, AuthenticationError } from '../src/errors.js';
import { getClient } from './helpers.js';

describe('Client', () => {
  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  it('throws when no API key is provided', () => {
    // Temporarily remove env var to test the guard
    const saved = process.env['AIRSTORE_API_KEY'];
    delete process.env['AIRSTORE_API_KEY'];

    try {
      expect(() => new Airstore({ apiKey: '' })).toThrow(AirstoreError);
    } finally {
      if (saved) process.env['AIRSTORE_API_KEY'] = saved;
    }
  });

  it('accepts an explicit API key', () => {
    const client = new Airstore({
      apiKey: 'test-key-123',
      baseURL: 'http://localhost:9999/api/v1',
    });
    expect(client.apiKey).toBe('test-key-123');
    expect(client.baseURL).toBe('http://localhost:9999/api/v1');
  });

  it('uses default timeout and maxRetries', () => {
    const client = new Airstore({ apiKey: 'test' });
    expect(client.timeout).toBe(60_000);
    expect(client.maxRetries).toBe(2);
  });

  it('allows overriding timeout and maxRetries', () => {
    const client = new Airstore({
      apiKey: 'test',
      timeout: 5_000,
      maxRetries: 0,
    });
    expect(client.timeout).toBe(5_000);
    expect(client.maxRetries).toBe(0);
  });

  it('strips trailing slashes from baseURL', () => {
    const client = new Airstore({
      apiKey: 'test',
      baseURL: 'http://localhost:1994/api/v1///',
    });
    expect(client.baseURL).toBe('http://localhost:1994/api/v1');
  });

  // -------------------------------------------------------------------------
  // Resource namespaces exist
  // -------------------------------------------------------------------------

  it('exposes all resource namespaces', () => {
    const client = new Airstore({ apiKey: 'test' });
    expect(client.workspaces).toBeDefined();
    expect(client.connections).toBeDefined();
    expect(client.smartFolders).toBeDefined();
    expect(client.tokens).toBeDefined();
    expect(client.members).toBeDefined();
    expect(client.oauth).toBeDefined();
    expect(client.fs).toBeDefined();
  });

  // -------------------------------------------------------------------------
  // Auth error with invalid token (requires live gateway)
  // -------------------------------------------------------------------------

  it('throws AuthenticationError for invalid API key', async () => {
    const badClient = new Airstore({
      apiKey: 'invalid-key-that-does-not-exist',
      baseURL: getClient().baseURL,
      maxRetries: 0,
    });

    await expect(badClient.workspaces.list()).rejects.toThrow(AuthenticationError);
  });

  // -------------------------------------------------------------------------
  // Health check (validates gateway is reachable)
  // -------------------------------------------------------------------------

  it('can reach the gateway health endpoint', async () => {
    const client = getClient();
    const resp = await client.rawRequest('GET', '/health');
    expect(resp.status).toBe(200);
  });
});
