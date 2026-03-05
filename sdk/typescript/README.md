# @airstore/sdk

Official TypeScript SDK for the [Airstore](https://airstore.ai) API. Create workspaces, connect data sources, run agents, and observe everything — all from your backend.

## Installation

```bash
npm install @airstore/sdk
```

## Quick Start

```typescript
import Airstore from '@airstore/sdk';

const airstore = new Airstore({ apiKey: process.env.AIRSTORE_API_KEY });

// Create a workspace
const ws = await airstore.workspaces.create({ name: 'user-123' });

// Create an agent
const agent = await airstore.agents.create(ws.external_id, {
  agentKey: 'support-agent',
  name: 'Support Agent',
  runner: 'claude_code',
  config: { model: 'claude-sonnet-4-6' },
});

// Submit a task
const { task, run_id } = await airstore.tasks.create(ws.external_id, {
  message: 'Summarize recent support tickets',
  agentId: agent.id,
});

// Poll for logs and events
const batch = await airstore.tasks.streamEvents(ws.external_id, task.id);
console.log(batch.logs);
```

## Configuration

```typescript
const airstore = new Airstore({
  apiKey: 'org_...',                // or set AIRSTORE_API_KEY env var
  baseURL: 'https://api.airstore.ai/api/v1', // or AIRSTORE_BASE_URL
  timeout: 30_000,                  // request timeout in ms (default: 60000)
  maxRetries: 3,                    // retries for 429/5xx (default: 2)
});
```

## API Reference

### Agents

```typescript
// Create
const agent = await airstore.agents.create(wsId, {
  agentKey: 'my-agent',
  name: 'My Agent',
  runner: 'claude_code',
  config: { model: 'claude-sonnet-4-6', system_prompt: 'You are helpful.' },
});

// List
const agents = await airstore.agents.list(wsId);

// Retrieve
const a = await airstore.agents.retrieve(wsId, agentId);

// Update
await airstore.agents.update(wsId, agentId, { name: 'Renamed Agent' });

// Delete (also removes bound hooks)
await airstore.agents.delete(wsId, agentId);
```

### Tasks

```typescript
// Create a task (returns accepted response with task + run_id)
const { task, run_id } = await airstore.tasks.create(wsId, {
  message: 'Do something',
  agentId: agent.id,
  idempotencyKey: 'unique-key',     // optional, server generates if omitted
  sessionId: 'session-abc',         // optional, groups related tasks
  timeoutMs: 120_000,               // optional
  policy: {                         // optional execution policy
    host: 'sandbox',
    security: 'allowlist',
    runtimeType: 'gvisor',
    workspaceAccess: 'rw',
  },
});

// List with filters
const page = await airstore.tasks.list(wsId, {
  state: ['running', 'idle'],
  agentId: agent.id,
  limit: 20,
});

// Retrieve
const t = await airstore.tasks.retrieve(wsId, taskId);

// Cancel a running task
await airstore.tasks.cancel(wsId, taskId);

// Archive a completed/idle task
await airstore.tasks.archive(wsId, taskId);
```

### Log Streaming & Events

```typescript
// Fetch logs for a task (cursor-based for incremental reads)
const logs = await airstore.tasks.listLogs(wsId, taskId);

// Composite event stream: task/run state + logs + run events in one call
let logCursor = 0;
let eventCursor = 0;

const batch = await airstore.tasks.streamEvents(wsId, taskId, {
  logCursor,
  runEventCursor: eventCursor,
});

// Use next cursors for subsequent polls
logCursor = batch.next_log_cursor;
eventCursor = batch.next_run_event_cursor;
```

### Runs

```typescript
// List runs
const runs = await airstore.runs.list(wsId, { status: 'running' });

// Retrieve a run
const run = await airstore.runs.retrieve(wsId, runId);

// List snapshots (intermediate state)
const snapshots = await airstore.runs.listSnapshots(wsId, runId);

// List execution events
const events = await airstore.runs.listEvents(wsId, runId);

// Cancel
await airstore.runs.cancel(wsId, runId);
```

### Channels (Follow-Up Messages)

```typescript
// Send a new task to an agent via direct channel
await airstore.channels.sendDirectAgentMessage(wsId, agentId, {
  message: 'What about the billing tickets?',
});

// Send follow-up input to an active run
await airstore.channels.sendDirectRunMessage(wsId, runId, {
  message: 'Focus on the last 24 hours',
  taskId: task.id,
  queueMode: 'steer', // 'queue' | 'followup' | 'steer' | 'interrupt'
});
```

### Scheduled Tasks (Cron)

```typescript
// Create a schedule
const schedule = await airstore.tasks.createSchedule(wsId, {
  agentId: agent.id,
  cronExpr: '0 9 * * 1-5',    // every weekday at 9am
  timezone: 'America/New_York',
  prompt: 'Summarize overnight support tickets',
});

// List schedules
const schedules = await airstore.tasks.listSchedules(wsId);

// Update (pause, change cron, etc.)
await airstore.tasks.updateSchedule(wsId, schedule.external_id, {
  active: false,
});

// Delete
await airstore.tasks.deleteSchedule(wsId, schedule.external_id);
```

### Hooks (File-System Triggers)

```typescript
// Create a hook on a source view folder
const hook = await airstore.hooks.create(wsId, {
  path: '/sources/gmail/Recent Emails',
  prompt: 'Triage this email and draft a reply',
  eventTypes: ['create'],
  agentName: 'Email Triager',
});

// List / Retrieve / Update / Delete
const hooks = await airstore.hooks.list(wsId);
await airstore.hooks.update(wsId, hookId, { active: false });
await airstore.hooks.delete(wsId, hookId);
```

### Workspaces

```typescript
const ws = await airstore.workspaces.create({ name: 'my-workspace' });
const workspaces = await airstore.workspaces.list();
const ws2 = await airstore.workspaces.retrieve(wsId);
await airstore.workspaces.del(wsId);
```

### Connections

```typescript
// OAuth tokens
await airstore.connections.create(wsId, {
  integrationType: 'gmail',
  accessToken: '...',
  refreshToken: '...',
});

// API key
await airstore.connections.create(wsId, {
  integrationType: 'github',
  apiKey: 'ghp_...',
});

const connections = await airstore.connections.list(wsId);
await airstore.connections.del(wsId, connectionId);
```

### Source Views

```typescript
// Smart mode (LLM-inferred)
const view = await airstore.views.create(wsId, {
  integration: 'gmail',
  name: 'Important Emails',
  guidance: 'Emails marked as important from the last month',
});

// Query mode (structured filter)
const view2 = await airstore.views.create(wsId, {
  integration: 'github',
  name: 'Open PRs',
  filter: { repo: 'acme/api', type: 'prs', state: 'open' },
});

// Sync, update, delete
await airstore.views.sync(wsId, viewId);
await airstore.views.update(wsId, viewId, { guidance: 'Updated' });
await airstore.views.del(wsId, viewId);
```

### Tokens, Members, OAuth

```typescript
// Tokens (for CLI mounting)
const token = await airstore.tokens.create(wsId, {
  email: 'agent@internal', name: 'vm-mount',
});

// Members
const member = await airstore.members.create(wsId, {
  email: 'user@example.com', name: 'Jane', role: 'member',
});

// Interactive OAuth
const session = await airstore.oauth.createSession({
  integrationType: 'gmail',
  returnTo: 'https://myapp.com/callback',
});
const completed = await airstore.oauth.poll(session.session_id);
```

### Filesystem

```typescript
const entries = await airstore.fs.list(wsId, { path: '/' });
const content = await airstore.fs.read(wsId, { path: '/sources/gmail/email.txt' });
const tree = await airstore.fs.tree(wsId, { path: '/', maxKeys: 100 });
const meta = await airstore.fs.stat(wsId, '/sources/gmail/email.txt');
```

## Error Handling

```typescript
import { NotFoundError, RateLimitError, AuthenticationError } from '@airstore/sdk';

try {
  await airstore.workspaces.retrieve('ws_nonexistent');
} catch (err) {
  if (err instanceof NotFoundError) console.log('Not found');
  if (err instanceof RateLimitError) console.log('Rate limited');
  if (err instanceof AuthenticationError) console.log('Bad API key');
}
```

| Class | Status | Description |
|---|---|---|
| `AirstoreError` | — | Base error for all SDK errors |
| `APIError` | varies | Base for HTTP errors |
| `AuthenticationError` | 401 | Invalid or missing API key |
| `PermissionDeniedError` | 403 | Token lacks permission |
| `NotFoundError` | 404 | Resource not found |
| `ConflictError` | 409 | Conflicting operation |
| `UnprocessableEntityError` | 422 | Validation failed |
| `RateLimitError` | 429 | Rate limit exceeded |
| `InternalServerError` | 5xx | Server error (retried automatically) |
| `APIConnectionError` | — | Network failure |
| `APIConnectionTimeoutError` | — | Request timed out |

## Per-Request Options

Every method accepts an optional last argument for per-request overrides:

```typescript
await airstore.workspaces.list({
  timeout: 10_000,
  maxRetries: 5,
  signal: controller.signal,
  headers: { 'X-Trace-Id': 'abc' },
});
```

## Response Metadata

Every response includes a non-enumerable `lastResponse` property:

```typescript
const ws = await airstore.workspaces.create({ name: 'test' });
ws.lastResponse.statusCode;  // 200
ws.lastResponse.requestId;   // 'req_abc123'
```

## Raw Requests

For endpoints not yet covered by the SDK:

```typescript
const response = await airstore.rawRequest('POST', '/some/new/endpoint', {
  body: { key: 'value' },
});
```

## Requirements

- Node.js 18+ (uses native `fetch`)
- TypeScript 5.0+
- Zero runtime dependencies

## License

Apache-2.0
