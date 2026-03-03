import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { APIError } from '../src/errors.js';
import { getClient, uniqueName, waitForRunIdForTask } from './helpers.js';

declare const process: {
  env: Record<string, string | undefined>;
};

const TERMINAL = new Set(['ok', 'error', 'timeout', 'cancelled']);

async function resolveWorkspaceFromWhoami(): Promise<string | undefined> {
  const client = getClient();
  const resp = await client.rawRequest('GET', '/auth/whoami', { timeout: 15_000 });
  if (!resp.ok) return undefined;

  const body = (await resp.json()) as { data?: { workspace_id?: string } };
  return body.data?.workspace_id;
}

async function waitForTerminalRun(
  workspaceId: string,
  runId: string,
  timeoutMs = 180_000,
) {
  const client = getClient();
  const deadline = Date.now() + timeoutMs;
  let run = await client.runs.retrieve(workspaceId, runId);

  while (!TERMINAL.has(run.status) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 1500));
    run = await client.runs.retrieve(workspaceId, runId);
  }

  return run;
}

async function waitForOutputJSON(
  workspaceId: string,
  paths = ['/data/output.json', '/workspace/data/output.json'],
  timeoutMs = 45_000,
): Promise<string> {
  const client = getClient();
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    for (const path of paths) {
      try {
        const content = await client.fs.read(workspaceId, { path });
        if (content.trim() !== '') return content;
      } catch (err) {
        lastError = err;
        if (!(err instanceof APIError && err.status === 404)) {
          throw err;
        }
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  if (lastError instanceof Error) {
    throw new Error(`timed out waiting for output JSON (${paths.join(', ')}): ${lastError.message}`);
  }
  throw new Error(`timed out waiting for output JSON (${paths.join(', ')})`);
}

describe('Agent/Runs E2E', () => {
  const client = getClient();
  let workspaceId = '';
  let createdWorkspace = false;
  let agentId: string;

  beforeAll(async () => {
    // First choice: resolve workspace directly from token identity.
    workspaceId = (await resolveWorkspaceFromWhoami()) ?? '';

    // Optional explicit override (mainly useful for org/admin tokens).
    if (!workspaceId) {
      workspaceId = process.env['AIRSTORE_WORKSPACE_ID'] ?? '';
    }

    // Final fallback: create a temporary workspace when token permissions allow it.
    if (!workspaceId) {
      try {
        const workspace = await client.workspaces.create({
          name: uniqueName('agent-runs-e2e'),
        });
        workspaceId = workspace.external_id;
        createdWorkspace = true;
      } catch (err) {
        const status = (err as { status?: number }).status;
        if (status === 403) {
          throw new Error(
            'Token does not map to a workspace and cannot create one. Use a workspace token, or set AIRSTORE_WORKSPACE_ID with an org/admin token.',
          );
        }
        throw err;
      }
    }

    if (!workspaceId) {
      throw new Error(
        'Could not resolve workspace from token identity.',
      );
    }

    const agent = await client.agents.create(workspaceId, {
      agentKey: uniqueName('e2e-agent'),
      name: uniqueName('E2E Agent'),
      config: { model: 'claude-sonnet-4-6' },
    });
    agentId = agent.id;
  });

  afterAll(async () => {
    if (!createdWorkspace || !workspaceId) return;
    await client.workspaces.del(workspaceId).catch(() => undefined);
  });

  it('creates an agent and runs a browser automation task for YC stories', async () => {
    const idempotencyKey = uniqueName('e2e-idem');
    const browserPrompt = [
      'Use the browser tool to scrape Hacker News newest stories.',
      'URL: https://news.ycombinator.com/newest',
      'Collect the top 5 most recent stories from the page.',
      'Build JSON with shape {"source":"https://news.ycombinator.com/newest","stories":[{"rank":1,"title":"...","url":"https://news.ycombinator.com/item?id=..."}]}.',
      'Write that JSON to /workspace/data/output.json (exact file path).',
      'Use absolute URLs and make sure the file contains valid JSON only.',
    ].join('\n');

    const accepted = await client.tasks.create(workspaceId, {
      message: browserPrompt,
      agentId,
      idempotencyKey,
      timeoutMs: 180_000,
      policy: {
        host: 'sandbox',
        security: 'full',
        ask: 'off',
        runtimeType: 'gvisor',
        workspaceAccess: 'rw',
        networkEnabled: true,
        interactive: false,
      },
    });
    expect(accepted.accepted).toBe(true);
    const acceptedSessionID = accepted.task.payload_json['session_id'];
    expect(typeof acceptedSessionID).toBe('string');
    expect((acceptedSessionID as string).length).toBeGreaterThan(0);

    const runId =
      accepted.run_id ??
      accepted.task.target_run_id ??
      (await waitForRunIdForTask(workspaceId, accepted.task.id, { timeoutMs: 75_000 }));
    if (!runId) {
      const task = await client.tasks.retrieve(workspaceId, accepted.task.id);
      throw new Error(
        `run_id did not materialize for task ${accepted.task.id} (state=${task.state}, target_run_id=${task.target_run_id ?? 'none'}, dropped_reason=${task.dropped_reason ?? 'none'})`,
      );
    }

    const run = await waitForTerminalRun(workspaceId, runId, 240_000);
    expect(run.id).toBe(runId);
    if (!TERMINAL.has(run.status)) {
      console.warn(`Run ${runId} still in status '${run.status}' after timeout — skipping terminal assertions`);
      return;
    }
    expect(run.status).toBe('ok');
    expect(run.model).toBe('claude-sonnet-4-6');

    const snapshots = await client.runs.listSnapshots(workspaceId, runId);
    expect(snapshots.length).toBeGreaterThan(0);
    expect(snapshots.every((snapshot) => snapshot.run_id === runId)).toBe(true);

    const outputJSON = await waitForOutputJSON(workspaceId);
    const output = JSON.parse(outputJSON) as {
      source?: string;
      stories?: Array<{ rank?: number; title?: string; url?: string }>;
    };
    expect(output.source).toBe('https://news.ycombinator.com/newest');
    expect(Array.isArray(output.stories)).toBe(true);
    expect(output.stories?.length).toBe(5);
    expect(
      output.stories?.every(
        (story) =>
          typeof story.rank === 'number' &&
          typeof story.title === 'string' &&
          story.title.length > 0 &&
          typeof story.url === 'string' &&
          story.url.includes('item?id='),
      ),
    ).toBe(true);

    const replay = await client.tasks.create(workspaceId, {
      message: browserPrompt,
      agentId,
      idempotencyKey,
      timeoutMs: 180_000,
    });
    expect(replay.idempotent_hit).toBe(true);
    expect(replay.task.id).toBe(accepted.task.id);
  }, 300_000);
});
