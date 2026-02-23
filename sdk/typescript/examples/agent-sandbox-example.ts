#!/usr/bin/env npx tsx
import { Airstore } from '../src/airstore.js';

declare const process: {
  env: Record<string, string | undefined>;
  exit(code?: number): never;
};

const TERMINAL_RUN_STATUSES = new Set(['ok', 'error', 'timeout', 'cancelled']);

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function resolveWorkspaceFromWhoami(baseURL: string, apiKey: string): Promise<string | undefined> {
  const normalizedBase = baseURL.replace(/\/+$/, '');
  const res = await fetch(`${normalizedBase}/auth/whoami`, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      Accept: 'application/json',
    },
  });
  if (!res.ok) return undefined;
  const body = (await res.json()) as {
    data?: { workspace_id?: string };
  };
  return body.data?.workspace_id;
}

async function main() {
  const apiKey = process.env['AIRSTORE_API_KEY'];
  if (!apiKey) {
    throw new Error('AIRSTORE_API_KEY is required');
  }

  const baseURL = process.env['AIRSTORE_BASE_URL'] || 'http://localhost:1994/api/v1';
  const client = new Airstore({
    apiKey,
    baseURL,
    timeout: 30_000,
    maxRetries: 1,
  });

  const workspaceName = `sdk-agent-sandbox-example-${Date.now()}`;
  let workspaceId: string | undefined;
  let createdWorkspace = false;

  try {
    console.log('[1/7] resolve workspace');
    workspaceId = process.env['AIRSTORE_WORKSPACE_ID'];
    if (!workspaceId) {
      try {
        const workspace = await client.workspaces.create({ name: workspaceName });
        workspaceId = workspace.external_id;
        createdWorkspace = true;
      } catch (err) {
        const status = (err as { status?: number }).status;
        if (status !== 403) throw err;
        workspaceId = await resolveWorkspaceFromWhoami(baseURL, apiKey);
      }
    }
    assert(
      workspaceId,
      'workspace could not be resolved; set AIRSTORE_WORKSPACE_ID or use an admin/org token',
    );

    console.log('[2/7] create agent profile');
    const agent = await client.agents.create(workspaceId, {
      agentKey: `sandbox-agent-${Date.now()}`,
      name: 'Sandbox Verification Agent',
      config: { model: 'claude-sonnet-4', purpose: 'sandbox-verification' },
    });
    assert(agent.id, 'expected agent id');

    console.log('[3/7] submit task');
    const sessionId = `sandbox-session-${Date.now()}`;
    const accepted = await client.tasks.create(workspaceId, {
      message: 'Run a minimal sandbox task and report status',
      sessionId,
      agentId: agent.id,
      idempotencyKey: `sandbox-idem-${Date.now()}`,
      timeoutMs: 120_000,
    });
    assert(accepted.accepted, 'expected accepted response');

    console.log('[4/7] wait for run materialization');
    let runId = accepted.run_id ?? accepted.task.target_run_id;
    const taskId = accepted.task.id;
    const taskDeadline = Date.now() + 30_000;
    while (!runId && Date.now() < taskDeadline) {
      await sleep(1000);
      const task = await client.tasks.retrieve(workspaceId, taskId);
      runId = task.target_run_id;
    }
    assert(runId, 'run was not materialized from task within timeout');

    console.log('[5/7] wait for run execution binding');
    const attemptDeadline = Date.now() + 45_000;
    let attempts = await client.runs.listAttempts(workspaceId, runId);
    while (
      Date.now() < attemptDeadline &&
      !attempts.some((attempt) => Boolean(attempt.execution_id))
    ) {
      await sleep(1500);
      attempts = await client.runs.listAttempts(workspaceId, runId);
    }
    const boundAttempt = attempts.find((attempt) => Boolean(attempt.execution_id));
    assert(boundAttempt, 'run attempt did not bind to run execution');

    console.log('[6/7] wait for terminal run state');
    const runDeadline = Date.now() + 180_000;
    let run = await client.runs.retrieve(workspaceId, runId);
    while (!TERMINAL_RUN_STATUSES.has(run.status) && Date.now() < runDeadline) {
      await sleep(1500);
      run = await client.runs.retrieve(workspaceId, runId);
    }
    assert(
      TERMINAL_RUN_STATUSES.has(run.status),
      `run did not reach terminal status, current=${run.status}`,
    );

    console.log('[7/7] verify snapshots + events');
    const snapshots = await client.runs.listSnapshots(workspaceId, runId);
    const events = await client.runs.listEvents(workspaceId, runId);

    assert(snapshots.length > 0, 'expected at least one run snapshot');
    assert(
      snapshots.every((snapshot, idx, all) => idx === 0 || snapshot.seq > all[idx - 1]!.seq),
      'snapshot sequence is not strictly increasing',
    );
    assert(
      snapshots.every((snapshot) => snapshot.run_id === runId),
      'snapshot run_id mismatch',
    );

    console.log('agent sandbox plumbing verified');
    console.log(
      `workspace=${workspaceId} run=${runId} status=${run.status} attempts=${attempts.length} execution=${boundAttempt.execution_id} snapshots=${snapshots.length} events=${events.length}`,
    );
  } finally {
    if (createdWorkspace && workspaceId && process.env['KEEP_EXAMPLE_WORKSPACE'] !== '1') {
      await client.workspaces.del(workspaceId).catch(() => undefined);
    }
  }
}

main().catch((err) => {
  console.error('agent sandbox example failed:', err);
  process.exit(1);
});
