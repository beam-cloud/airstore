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

async function main() {
  const apiKey = process.env['AIRSTORE_API_KEY'];
  if (!apiKey) {
    throw new Error('AIRSTORE_API_KEY is required');
  }

  const client = new Airstore({
    apiKey,
    baseURL: process.env['AIRSTORE_BASE_URL'] || 'http://localhost:1994/api/v1',
    timeout: 30_000,
    maxRetries: 1,
  });

  const workspaceName = `sdk-agent-runs-smoke-${Date.now()}`;
  let workspaceId: string | undefined;

  try {
    console.log('[1/8] create workspace');
    const workspace = await client.workspaces.create({ name: workspaceName });
    workspaceId = workspace.external_id;

    console.log('[2/8] create agent profile');
    const agent = await client.agents.create(workspaceId, {
      agentKey: `agent-${Date.now()}`,
      name: 'Agent/Runs Smoke Agent',
      config: { model: 'claude-sonnet-4', purpose: 'smoke' },
    });
    assert(agent.id, 'expected agent id');

    const idempotencyKey = `idem-${Date.now()}`;
    const sessionId = `session-${Date.now()}`;

    console.log('[3/8] submit task');
    const accepted = await client.tasks.create(workspaceId, {
      message: 'Say hello from agent/runs smoke test',
      sessionId,
      agentId: agent.id,
      idempotencyKey,
      timeoutMs: 120_000,
    });
    assert(accepted.accepted, 'expected accepted response');
    assert(accepted.task.id, 'expected task id');

    console.log('[4/8] resolve run id from task');
    let runId = accepted.run_id;
    const taskId = accepted.task.id;
    const taskDeadline = Date.now() + 30_000;
    while (!runId && Date.now() < taskDeadline) {
      await sleep(1000);
      const task = await client.tasks.retrieve(workspaceId, taskId);
      runId = task.target_run_id;
    }
    assert(runId, 'run was not materialized from task within timeout');

    console.log('[5/8] poll run status');
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

    console.log('[6/8] fetch snapshots and events');
    const snapshots = await client.runs.listSnapshots(workspaceId, runId);
    const events = await client.runs.listEvents(workspaceId, runId);

    assert(snapshots.length > 0, 'expected at least one snapshot');
    assert(
      snapshots.every((snapshot) => snapshot.run_id === runId),
      'snapshot run_id mismatch',
    );
    assert(
      snapshots.every((snapshot, idx, all) => idx === 0 || snapshot.seq > all[idx - 1]!.seq),
      'snapshot sequence is not strictly increasing',
    );
    console.log(
      `run=${runId} status=${run.status} snapshots=${snapshots.length} events=${events.length}`,
    );

    console.log('[7/8] replay idempotency key');
    const replay = await client.tasks.create(workspaceId, {
      message: 'Say hello from agent/runs smoke test',
      sessionId,
      agentId: agent.id,
      idempotencyKey,
      timeoutMs: 120_000,
    });
    assert(replay.idempotent_hit, 'expected idempotent replay hit');
    assert(
      replay.task.id === accepted.task.id,
      'idempotent replay returned different task id',
    );

    console.log('[8/8] done');
    console.log('agent/runs smoke passed');
  } finally {
    if (workspaceId && process.env['KEEP_SMOKE_WORKSPACE'] !== '1') {
      await client.workspaces.del(workspaceId).catch(() => undefined);
    }
  }
}

main().catch((err) => {
  console.error('agent/runs smoke failed:', err);
  process.exit(1);
});
