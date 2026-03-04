/**
 * Shared test utilities for Airstore SDK integration tests.
 *
 * Configuration via environment variables:
 *   AIRSTORE_API_KEY  – Required. Workspace/org/admin token for API access.
 *   AIRSTORE_BASE_URL – Optional. Defaults to http://localhost:1994/api/v1
 */
import { Airstore } from '../src/airstore.js';
import { APIError } from '../src/errors.js';
import type { Workspace } from '../src/types/workspaces.js';

declare const process: {
  env: Record<string, string | undefined>;
};

// ---------------------------------------------------------------------------
// Client singleton
// ---------------------------------------------------------------------------

let _client: Airstore | undefined;
const _ephemeralWorkspaceIds = new Set<string>();

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
 * Requires token permissions to create workspaces.
 */
export async function createTestWorkspace(
  suffix?: string,
): Promise<Workspace> {
  const client = getClient();
  const explicitWorkspaceId = process.env['AIRSTORE_WORKSPACE_ID'];
  if (explicitWorkspaceId) {
    return client.workspaces.retrieve(explicitWorkspaceId);
  }

  const name = `sdk-test-${suffix ?? 'default'}-${Date.now()}`;
  try {
    const workspace = await client.workspaces.create({ name });
    _ephemeralWorkspaceIds.add(workspace.external_id);
    return workspace;
  } catch (err) {
    // Workspace-member tokens can't create workspaces; fall back to the token's workspace.
    if (err instanceof APIError && err.status === 403) {
      const whoami = await client.request<{ workspace_id?: string; workspace_name?: string }>(
        'GET',
        '/auth/whoami',
      );
      if (whoami.workspace_id) {
        const now = new Date().toISOString();
        return {
          external_id: whoami.workspace_id,
          name: whoami.workspace_name ?? 'workspace',
          created_at: now,
          updated_at: now,
        };
      }
    }
    throw err;
  }
}

/**
 * Delete a workspace, swallowing errors (best-effort cleanup).
 */
export async function deleteTestWorkspace(id: string): Promise<void> {
  if (!_ephemeralWorkspaceIds.has(id)) {
    return;
  }

  try {
    await getClient().workspaces.del(id);
  } catch {
    // Swallow — workspace may already be deleted or test may have failed before creation.
  } finally {
    _ephemeralWorkspaceIds.delete(id);
  }
}

/**
 * Generate a unique name for test resources to avoid collisions.
 */
export function uniqueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export interface WaitForRunIdOptions {
  timeoutMs?: number;
  intervalMs?: number;
}

/**
 * Resolve a run id for a task, accounting for eventual consistency.
 * We first read task.target_run_id and then fall back to scanning runs by origin_task_id.
 */
export async function waitForRunIdForTask(
  workspaceId: string,
  taskId: string,
  options?: WaitForRunIdOptions,
): Promise<string | undefined> {
  const client = getClient();
  const timeoutMs = options?.timeoutMs ?? 60_000;
  const intervalMs = options?.intervalMs ?? 400;
  const deadline = Date.now() + timeoutMs;
  let pollCount = 0;
  let currentIntervalMs = intervalMs;

  while (Date.now() < deadline) {
    pollCount += 1;
    const task = await client.tasks.retrieve(workspaceId, taskId);
    if (task.target_run_id) return task.target_run_id;
    const sessionId = typeof task.payload_json['session_id'] === 'string'
      ? task.payload_json['session_id']
      : undefined;
    const taskAgentId = typeof task.agent_id === 'string' ? task.agent_id : undefined;

    if (task.state === 'dropped' || task.state === 'cancelled') {
      const reason = task.dropped_reason ? ` (${task.dropped_reason})` : '';
      throw new Error(
        `task ${taskId} reached terminal state ${task.state} before run materialized${reason}`,
      );
    }

    // Fallback for eventual-consistency windows where run exists before task.target_run_id updates.
    // Keep this infrequent to avoid turning run lookup into a heavy list loop.
    if (pollCount % 5 === 0) {
      const runsResponse = await client.runs.list(workspaceId);
      const match = runsResponse.runs.find((run) => {
        if (run.origin_task_id === taskId) return true;
        if (!sessionId || run.session_id !== sessionId) return false;
        if (!taskAgentId) return true;
        return run.agent_id === taskAgentId;
      });
      if (match) return match.id;
    }

    await new Promise((resolve) => setTimeout(resolve, currentIntervalMs));
    if (currentIntervalMs < 2_000) {
      currentIntervalMs = Math.min(2_000, currentIntervalMs + 200);
    }
  }

  return undefined;
}
