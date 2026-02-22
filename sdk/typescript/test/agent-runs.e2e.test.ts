import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { APIError } from '../src/errors.js';
import { getClient, uniqueName } from './helpers.js';

const TERMINAL = new Set(['ok', 'error', 'timeout', 'cancelled']);

async function waitForRunId(
  workspaceId: string,
  envelopeId: string,
  timeoutMs = 30000,
): Promise<string | undefined> {
  const client = getClient();
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const envelope = await client.tasks.retrieve(workspaceId, envelopeId);
    if (envelope.target_run_id) return envelope.target_run_id;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  return undefined;
}

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

async function readTaskLogs(taskId: string, timeoutMs = 30_000): Promise<string> {
  const client = getClient();
  const resp = await client.rawRequest('GET', `/tasks/${taskId}/logs/stream`, {
    timeout: timeoutMs,
  });

  if (!resp.ok) {
    throw new Error(`log stream request failed: ${resp.status} ${resp.statusText}`);
  }

  const ssePayload = await resp.text();
  const logChunks: string[] = [];

  for (const line of ssePayload.split('\n')) {
    if (!line.startsWith('data: ')) continue;
    const raw = line.slice(6);
    try {
      const event = JSON.parse(raw) as { type?: string; data?: string };
      if (event.type === 'log' && typeof event.data === 'string') {
        logChunks.push(event.data);
      }
    } catch {
      // Ignore malformed SSE rows; we only care about log payloads.
    }
  }

  return logChunks.join('\n');
}

async function waitForOutputJSON(
  workspaceId: string,
  path = '/memory/output.json',
  timeoutMs = 45_000,
): Promise<string> {
  const client = getClient();
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      const content = await client.fs.read(workspaceId, { path });
      if (content.trim() !== '') return content;
    } catch (err) {
      lastError = err;
      if (!(err instanceof APIError && err.status === 404)) {
        throw err;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  if (lastError instanceof Error) {
    throw new Error(`timed out waiting for ${path}: ${lastError.message}`);
  }
  throw new Error(`timed out waiting for ${path}`);
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
      config: { model: 'claude-sonnet-4' },
    });
    agentId = agent.id;
  });

  afterAll(async () => {
    if (!createdWorkspace || !workspaceId) return;
    await client.workspaces.del(workspaceId).catch(() => undefined);
  });

  it('creates an agent and runs a browser automation task for YC stories', async () => {
    const idempotencyKey = uniqueName('e2e-idem');
    const sessionId = uniqueName('e2e-session');
    const browserPrompt = [
      'Use the browser tool to scrape Hacker News newest stories.',
      'URL: https://news.ycombinator.com/newest',
      'Collect the top 5 most recent stories from the page.',
      'Build JSON with shape {"source":"https://news.ycombinator.com/newest","stories":[{"rank":1,"title":"...","url":"https://news.ycombinator.com/item?id=..."}]}.',
      'Write that JSON to /workspace/memory/output.json (exact file path).',
      'Use absolute URLs and make sure the file contains valid JSON only.',
    ].join('\n');

    const accepted = await client.tasks.create(workspaceId, {
      message: browserPrompt,
      sessionId,
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

    const runId =
      accepted.run_id ??
      (await waitForRunId(workspaceId, accepted.envelope.id));
    expect(runId).toBeDefined();
    if (!runId) return;

    const run = await waitForTerminalRun(workspaceId, runId, 180_000);
    expect(run.id).toBe(runId);
    expect(run.status).toBe('ok');
    expect(run.model).toBe('claude-sonnet-4');

    const attempts = await client.runs.listAttempts(workspaceId, runId);
    const snapshots = await client.runs.listSnapshots(workspaceId, runId);
    expect(snapshots.length).toBeGreaterThan(0);
    expect(attempts.length).toBeGreaterThan(0);
    expect(attempts.every((attempt) => attempt.run_id === runId)).toBe(true);
    expect(snapshots.every((snapshot) => snapshot.run_id === runId)).toBe(true);

    const execTaskId = attempts[attempts.length - 1]?.execution_task_external_id;
    expect(execTaskId).toBeDefined();
    if (!execTaskId) return;

    const logs = await readTaskLogs(execTaskId);
    expect(logs.toLowerCase()).toContain('news.ycombinator.com');

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
      sessionId,
      agentId,
      idempotencyKey,
      timeoutMs: 180_000,
    });
    expect(replay.idempotent_hit).toBe(true);
    expect(replay.envelope.id).toBe(accepted.envelope.id);
  }, 300_000);
});
