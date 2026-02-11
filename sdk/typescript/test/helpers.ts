/**
 * Shared test utilities for Airstore SDK integration tests.
 *
 * Configuration via environment variables:
 *   AIRSTORE_API_KEY  – Required. Admin or org token for API access.
 *   AIRSTORE_BASE_URL – Optional. Defaults to http://localhost:1994/api/v1
 */
import { Airstore } from '../src/airstore.js';
import type { Workspace } from '../src/types/workspaces.js';

// ---------------------------------------------------------------------------
// Client singleton
// ---------------------------------------------------------------------------

let _client: Airstore | undefined;

/**
 * Get a shared Airstore client configured from environment variables.
 * Throws immediately if AIRSTORE_API_KEY is not set.
 */
export function getClient(): Airstore {
  if (_client) return _client;

  const apiKey = process.env['AIRSTORE_API_KEY'];
  if (!apiKey) {
    throw new Error(
      'AIRSTORE_API_KEY env var is required to run integration tests.\n' +
        'Example: AIRSTORE_API_KEY=<token> npm test',
    );
  }

  _client = new Airstore({
    apiKey,
    baseURL: process.env['AIRSTORE_BASE_URL'] || 'http://localhost:1994/api/v1',
    maxRetries: 1,
    timeout: 15_000,
  });

  return _client;
}

// ---------------------------------------------------------------------------
// Workspace lifecycle helpers
// ---------------------------------------------------------------------------

/**
 * Create a temporary workspace for testing. Returns the workspace object.
 * Name is prefixed with `sdk-test-` and includes a timestamp for uniqueness.
 */
export async function createTestWorkspace(
  suffix?: string,
): Promise<Workspace> {
  const client = getClient();
  const name = `sdk-test-${suffix ?? 'default'}-${Date.now()}`;
  return client.workspaces.create({ name });
}

/**
 * Delete a workspace, swallowing errors (best-effort cleanup).
 */
export async function deleteTestWorkspace(id: string): Promise<void> {
  try {
    await getClient().workspaces.del(id);
  } catch {
    // Swallow — workspace may already be deleted or test may have failed before creation.
  }
}

/**
 * Generate a unique name for test resources to avoid collisions.
 */
export function uniqueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}
