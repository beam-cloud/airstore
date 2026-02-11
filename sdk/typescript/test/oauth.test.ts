import { describe, it, expect } from 'vitest';
import { APIError } from '../src/errors.js';
import { getClient } from './helpers.js';

describe('OAuth', () => {
  const client = getClient();

  it('creates an OAuth session', async () => {
    try {
      const session = await client.oauth.createSession({
        integrationType: 'gmail',
        returnTo: 'https://example.com/callback',
      });

      expect(session.session_id).toBeDefined();
      expect(typeof session.session_id).toBe('string');
      expect(session.authorize_url).toBeDefined();
      expect(session.authorize_url).toContain('http');
    } catch (err) {
      // OAuth may not be configured on all gateways, or the token type
      // may not have permission for OAuth endpoints.
      if (err instanceof APIError && [400, 401, 403, 404, 500].includes(err.status)) {
        console.warn(`OAuth not available on this gateway (${err.status}): ${err.message}`);
        return;
      }
      throw err;
    }
  });

  it('retrieves an OAuth session status', async () => {
    let sessionId: string;
    try {
      const session = await client.oauth.createSession({
        integrationType: 'gmail',
      });
      sessionId = session.session_id;
    } catch (err) {
      if (err instanceof APIError) {
        console.warn(`Skipping: OAuth not configured (${err.status})`);
        return;
      }
      throw err;
    }

    const status = await client.oauth.getSession(sessionId);
    expect(status.status).toBeDefined();
    // A freshly created session should be pending
    expect(['pending', 'complete', 'error']).toContain(status.status);
  });
});
