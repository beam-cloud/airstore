# @airstore/sdk

Official TypeScript SDK for the [Airstore](https://airstore.ai) API. Provision workspaces, manage connections, configure smart folders, and generate mount tokens — all from your backend.

## Installation

```bash
npm install @airstore/sdk
```

## Quick Start

```typescript
import Airstore from '@airstore/sdk';

const airstore = new Airstore({
  apiKey: 'org_...', // or set AIRSTORE_API_KEY env var
});

const workspace = await airstore.workspaces.create({ name: 'user-123' });
console.log(workspace.external_id);
```

## Full Provisioning Flow

This is the typical flow when a new user signs up on your platform:

```typescript
import Airstore from '@airstore/sdk';

const airstore = new Airstore({ apiKey: process.env.AIRSTORE_API_KEY });

async function provisionUser(
  userId: string,
  gmailTokens: { accessToken: string; refreshToken: string },
) {
  // 1. Create a workspace
  const ws = await airstore.workspaces.create({ name: `user-${userId}` });

  // 2. Add a member (so we can create a scoped token)
  const member = await airstore.members.create(ws.external_id, {
    email: `${userId}@internal`,
    name: userId,
    role: 'member',
  });

  // 3. Connect Gmail with existing OAuth tokens
  await airstore.connections.create(ws.external_id, {
    integrationType: 'gmail',
    accessToken: gmailTokens.accessToken,
    refreshToken: gmailTokens.refreshToken,
  });

  // 4. Set up smart folders for what the agent should see
  await airstore.smartFolders.create(ws.external_id, {
    integration: 'gmail',
    name: 'Recent Emails',
    guidance: 'Last 7 days of emails from the inbox',
    outputFormat: 'folder',
  });

  // 5. Generate a mount token for the user's VM
  const token = await airstore.tokens.create(ws.external_id, {
    email: `${userId}@internal`,
    name: 'vm-mount',
  });

  // 6. Pass this to the VM:
  //    airstore start --token <token.token>
  return {
    workspaceId: ws.external_id,
    mountToken: token.token,
  };
}
```

## Configuration

```typescript
const airstore = new Airstore({
  // Required — org token or cluster admin token
  apiKey: 'org_...',

  // Override the base URL (default: https://api.airstore.ai/api/v1)
  baseURL: 'https://api.airstore.ai/api/v1',

  // Request timeout in ms (default: 60000)
  timeout: 30_000,

  // Max retries for 429/5xx errors (default: 2)
  maxRetries: 3,

  // Default headers for every request
  defaultHeaders: { 'X-Custom-Header': 'value' },
});
```

### Environment Variables

| Variable | Description |
|---|---|
| `AIRSTORE_API_KEY` | Default API key if not passed to constructor |
| `AIRSTORE_BASE_URL` | Default base URL if not passed to constructor |

## API Reference

### Workspaces

```typescript
// Create
const ws = await airstore.workspaces.create({ name: 'my-workspace' });

// List all (org tokens only see their tenant's workspaces)
const workspaces = await airstore.workspaces.list();

// Retrieve by ID
const ws = await airstore.workspaces.retrieve('ws_abc123');

// Delete
await airstore.workspaces.del('ws_abc123');
```

### Connections

```typescript
// Create with existing OAuth tokens
const conn = await airstore.connections.create('ws_abc123', {
  integrationType: 'gmail',
  accessToken: '...',
  refreshToken: '...',
});

// Create with API key
const conn = await airstore.connections.create('ws_abc123', {
  integrationType: 'github',
  apiKey: 'ghp_...',
});

// List
const connections = await airstore.connections.list('ws_abc123');

// Delete
await airstore.connections.del('ws_abc123', 'conn_abc123');
```

### Smart Folders

```typescript
// Create
const folder = await airstore.smartFolders.create('ws_abc123', {
  integration: 'gmail',
  name: 'Important Emails',
  guidance: 'Emails marked as important from the last month',
  outputFormat: 'folder', // or 'file'
});

// List all
const folders = await airstore.smartFolders.list('ws_abc123');

// Retrieve by path
const folder = await airstore.smartFolders.retrieve('ws_abc123', '/Sources/gmail/Important Emails');

// Update
const updated = await airstore.smartFolders.update('ws_abc123', 'query_abc', {
  guidance: 'Updated guidance text',
});

// Delete
await airstore.smartFolders.del('ws_abc123', 'query_abc');
```

### Tokens

```typescript
// Create a workspace-scoped token (for CLI mounting)
const token = await airstore.tokens.create('ws_abc123', {
  email: 'agent@internal',
  name: 'vm-mount',
  expiresIn: 86400, // optional, seconds
});
console.log(token.token); // raw value — only shown once

// List tokens (values are never returned)
const tokens = await airstore.tokens.list('ws_abc123');

// Revoke
await airstore.tokens.revoke('ws_abc123', 'tok_abc123');
```

### Members

```typescript
// Add a member
const member = await airstore.members.create('ws_abc123', {
  email: 'user@example.com',
  name: 'Jane Doe',
  role: 'member', // 'admin' | 'member' | 'viewer'
});

// List
const members = await airstore.members.list('ws_abc123');

// Remove
await airstore.members.del('ws_abc123', 'mem_abc123');
```

### OAuth Sessions

For interactive connection setup where users authorize via browser redirect:

```typescript
// Create an OAuth session
const session = await airstore.oauth.createSession({
  integrationType: 'gmail',
  returnTo: 'https://myapp.com/callback',
});
console.log(session.authorize_url); // redirect user here

// Check status
const status = await airstore.oauth.getSession(session.session_id);

// Or poll until completion (default: 5 min timeout, 2s interval)
const completed = await airstore.oauth.poll(session.session_id, {
  timeout: 120_000,
  interval: 3_000,
});
console.log(completed.connection_id);
```

### Filesystem

Read-only access to the virtual filesystem:

```typescript
// List directory contents
const entries = await airstore.fs.list('ws_abc123', { path: '/' });

// Read file contents
const content = await airstore.fs.read('ws_abc123', {
  path: '/Sources/gmail/inbox/email.txt',
});

// Get directory tree
const tree = await airstore.fs.tree('ws_abc123', {
  path: '/',
  maxKeys: 100,
});

// Stat a file
const meta = await airstore.fs.stat('ws_abc123', '/Sources/gmail/inbox/email.txt');
```

## Per-Request Options

Every method accepts an optional last argument for per-request overrides:

```typescript
const ws = await airstore.workspaces.list({
  timeout: 10_000,
  maxRetries: 5,
  signal: controller.signal,
  headers: { 'X-Trace-Id': 'abc' },
});
```

## Error Handling

The SDK throws typed errors for easy programmatic handling:

```typescript
import {
  AuthenticationError,
  NotFoundError,
  RateLimitError,
} from '@airstore/sdk';

try {
  await airstore.workspaces.retrieve('ws_nonexistent');
} catch (err) {
  if (err instanceof NotFoundError) {
    console.log('Workspace not found');
  } else if (err instanceof AuthenticationError) {
    console.log('Invalid API key');
  } else if (err instanceof RateLimitError) {
    console.log('Rate limited, retry after:', err.headers.get('retry-after'));
  }
}
```

### Error Hierarchy

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

## Response Metadata

Every response object includes a non-enumerable `lastResponse` property:

```typescript
const ws = await airstore.workspaces.create({ name: 'test' });
console.log(ws.lastResponse.statusCode);  // 200
console.log(ws.lastResponse.requestId);   // 'req_abc123'
console.log(ws.lastResponse.headers);     // Headers object
```

## Automatic Retries

The SDK automatically retries on transient errors (408, 409, 429, 500, 502, 503, 504) with exponential backoff and jitter. The `Retry-After` header is respected when present.

## Raw Requests

For endpoints not yet covered by the SDK, use the escape hatch:

```typescript
const response = await airstore.rawRequest('POST', '/some/new/endpoint', {
  body: { key: 'value' },
  timeout: 5_000,
});
const data = await response.json();
```

## Requirements

- Node.js 18+ (uses native `fetch`)
- TypeScript 5.0+ (for type-only imports)
- Zero runtime dependencies

## License

Apache-2.0
