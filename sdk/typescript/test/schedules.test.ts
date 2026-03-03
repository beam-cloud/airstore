import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Workspace } from '../src/types/workspaces.js';
import type { Schedule } from '../src/types/tasks.js';
import { createTestWorkspace, deleteTestWorkspace, getClient, uniqueName } from './helpers.js';

describe('Task Schedules', () => {
  const client = getClient();
  let workspace: Workspace;
  let agentId: string;
  let createdSchedule: Schedule;

  beforeAll(async () => {
    workspace = await createTestWorkspace('schedules');

    const agent = await client.agents.create(workspace.external_id, {
      agentKey: uniqueName('sched-agent'),
      name: uniqueName('schedule-test-agent'),
      runner: 'claude_code',
      config: { model: 'claude-sonnet-4' },
    });
    agentId = agent.id;
  });

  afterAll(async () => {
    await deleteTestWorkspace(workspace.external_id);
  });

  it('creates a schedule', async () => {
    createdSchedule = await client.tasks.createSchedule(workspace.external_id, {
      agentId,
      cronExpr: '0 9 * * *',
      prompt: 'Daily morning report: summarize overnight changes.',
    });

    expect(createdSchedule.external_id).toBeDefined();
    expect(createdSchedule.agent_id).toBe(agentId);
    expect(createdSchedule.cron_expr).toBe('0 9 * * *');
    expect(createdSchedule.prompt).toBe('Daily morning report: summarize overnight changes.');
    expect(createdSchedule.active).toBe(true);
    expect(createdSchedule.next_run_at).toBeDefined();
  });

  it('lists schedules', async () => {
    const list = await client.tasks.listSchedules(workspace.external_id);
    expect(Array.isArray(list)).toBe(true);
    expect(list.length).toBeGreaterThanOrEqual(1);
    expect(list.some((s) => s.external_id === createdSchedule.external_id)).toBe(true);
  });

  it('retrieves a schedule by ID', async () => {
    const fetched = await client.tasks.retrieveSchedule(
      workspace.external_id,
      createdSchedule.external_id,
    );
    expect(fetched.external_id).toBe(createdSchedule.external_id);
    expect(fetched.cron_expr).toBe('0 9 * * *');
  });

  it('updates a schedule prompt and cron', async () => {
    const updated = await client.tasks.updateSchedule(
      workspace.external_id,
      createdSchedule.external_id,
      {
        prompt: 'Updated: weekly summary of changes.',
        cronExpr: '0 9 * * 1',
      },
    );

    expect(updated.prompt).toBe('Updated: weekly summary of changes.');
    expect(updated.cron_expr).toBe('0 9 * * 1');
    expect(updated.next_run_at).toBeDefined();
  });

  it('pauses a schedule', async () => {
    const updated = await client.tasks.updateSchedule(
      workspace.external_id,
      createdSchedule.external_id,
      { active: false },
    );
    expect(updated.active).toBe(false);
  });

  it('resumes a schedule', async () => {
    const updated = await client.tasks.updateSchedule(
      workspace.external_id,
      createdSchedule.external_id,
      { active: true },
    );
    expect(updated.active).toBe(true);
  });

  it('deletes a schedule', async () => {
    await client.tasks.deleteSchedule(
      workspace.external_id,
      createdSchedule.external_id,
    );

    const list = await client.tasks.listSchedules(workspace.external_id);
    expect(list.some((s) => s.external_id === createdSchedule.external_id)).toBe(false);
  });
});
