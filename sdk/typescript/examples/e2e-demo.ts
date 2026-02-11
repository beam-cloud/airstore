#!/usr/bin/env npx tsx
/**
 * End-to-end demo: provision a workspace, connect Gmail via OAuth,
 * create a smart folder, browse the virtual filesystem, and read a file.
 *
 * Usage:
 *   AIRSTORE_API_KEY=<org-token> npx tsx examples/e2e-demo.ts
 *   AIRSTORE_API_KEY=<org-token> AIRSTORE_BASE_URL=http://localhost:1994/api/v1 npx tsx examples/e2e-demo.ts
 *
 * The script will print an OAuth URL — open it in your browser to
 * authorize Gmail, then come back and watch the rest happen automatically.
 */
import * as readline from 'node:readline';
import { Airstore } from '@airstore/sdk'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BOLD = '\x1b[1m';
const DIM = '\x1b[2m';
const GREEN = '\x1b[32m';
const CYAN = '\x1b[36m';
const RESET = '\x1b[0m';

function step(n: number, msg: string) {
  console.log(`\n${BOLD}${CYAN}[Step ${n}]${RESET} ${BOLD}${msg}${RESET}`);
}

function info(msg: string, detail?: unknown) {
  const extra = detail !== undefined
    ? ` ${DIM}${typeof detail === 'string' ? detail : JSON.stringify(detail)}${RESET}`
    : '';
  console.log(`  → ${msg}${extra}`);
}

function ok(msg: string) {
  console.log(`  ${GREEN}✓${RESET} ${msg}`);
}

function waitForEnter(prompt: string): Promise<void> {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    rl.question(`\n  ${BOLD}${prompt}${RESET} `, () => {
      rl.close();
      resolve();
    });
  });
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const client = new Airstore({
    baseURL: process.env['AIRSTORE_BASE_URL'] || 'https://api.airstore.ai/api/v1',
    maxRetries: 2,
    timeout: 30_000,
  });

  let workspaceId: string | undefined;

  try {
    // ----------------------------------------------------------------
    // 1. Create workspace
    // ----------------------------------------------------------------
    const wsName = `demo-${Date.now()}`;
    step(1, 'Create workspace');
    const ws = await client.workspaces.create({ name: wsName });
    workspaceId = ws.external_id;
    ok(`Created "${ws.name}" (${workspaceId})`);

    // ----------------------------------------------------------------
    // 2. Create a mount token
    // ----------------------------------------------------------------
    step(2, 'Create mount token');
    const mountToken = await client.tokens.create(workspaceId, {
      name: 'vm-mount',
    });
    ok(`Token: ${mountToken.token.slice(0, 16)}...`);
    info('Use this to mount on a VM:', `airstore mount ~/airstore --token ${mountToken.token.slice(0, 16)}...`);

    // ----------------------------------------------------------------
    // 3. Connect Gmail via OAuth
    // ----------------------------------------------------------------
    step(3, 'Connect Gmail via OAuth');
    const session = await client.oauth.createSession({
      integrationType: 'gmail',
      workspaceId,
    });
    console.log(`\n  ${BOLD}Open this URL in your browser to connect Gmail:${RESET}`);
    console.log(`  ${CYAN}${session.authorize_url}${RESET}\n`);

    await waitForEnter('Press Enter after you\'ve completed the OAuth flow...');

    // ----------------------------------------------------------------
    // 4. Poll for OAuth completion
    // ----------------------------------------------------------------
    step(4, 'Wait for OAuth to complete');
    info('Polling session', session.session_id);
    const completed = await client.oauth.poll(session.session_id, {
      timeout: 120_000,
      interval: 2_000,
    });
    ok(`Gmail connected! Connection ID: ${completed.connection_id}`);

    // ----------------------------------------------------------------
    // 5. Verify connection shows up
    // ----------------------------------------------------------------
    step(5, 'Verify connections');
    const connections = await client.connections.list(workspaceId);
    const gmail = connections.find((c) => c.integration_type === 'gmail');
    if (gmail) {
      ok(`Found Gmail connection: ${gmail.external_id}`);
    } else {
      info(`${connections.length} connection(s) found, Gmail may still be syncing`);
    }

    // ----------------------------------------------------------------
    // 6. Create smart folder for unread emails
    // ----------------------------------------------------------------
    step(6, 'Create smart folder: "Unread Emails"');
    const folder = await client.smartFolders.create(workspaceId, {
      integration: 'gmail',
      name: 'Unread Emails',
      guidance: 'Show only unread emails from the inbox',
      outputFormat: 'folder',
    });
    ok(`Smart folder created at ${folder.path}`);
    info('Folder ID', folder.external_id);

    // ----------------------------------------------------------------
    // 7. Browse the virtual filesystem
    // ----------------------------------------------------------------
    step(7, 'Browse virtual filesystem');

    // 7a. List root
    info('Listing /');
    const root = await client.fs.list(workspaceId, { path: '/' });
    for (const entry of root) {
      console.log(`    ${entry.type === 'directory' ? '📁' : '📄'} ${entry.name}`);
    }

    // 7b. List the smart folder path
    info(`Listing ${folder.path}`);

    // Give the smart folder a moment to populate
    await new Promise((r) => setTimeout(r, 3000));

    const emails = await client.fs.list(workspaceId, { path: folder.path });
    if (emails.length === 0) {
      info('Smart folder is empty (may still be syncing). Trying Sources/gmail/ instead...');
      const gmailDir = await client.fs.list(workspaceId, { path: '/Sources/gmail/' });
      for (const entry of gmailDir.slice(0, 10)) {
        console.log(`    ${entry.type === 'directory' ? '📁' : '📄'} ${entry.name}`);
      }
    } else {
      for (const entry of emails.slice(0, 10)) {
        console.log(`    ${entry.type === 'directory' ? '📁' : '📄'} ${entry.name}`);
      }
    }

    // ----------------------------------------------------------------
    // 8. Read a file
    // ----------------------------------------------------------------
    step(8, 'Read a file');

    // Find the first readable file from the smart folder or gmail source
    const filesToTry = emails.length > 0 ? emails : await client.fs.list(workspaceId, { path: '/Sources/gmail/' });
    const firstFile = filesToTry.find((e) => e.type !== 'directory');

    if (firstFile) {
      const filePath = firstFile.path || `${folder.path}/${firstFile.name}`;
      info(`Reading ${filePath}`);
      try {
        const content = await client.fs.read(workspaceId, { path: filePath });
        const preview = content.slice(0, 500);
        console.log(`\n${DIM}--- file content (first 500 chars) ---${RESET}`);
        console.log(preview);
        console.log(`${DIM}--- end ---${RESET}`);
        ok('File read successfully');
      } catch (err) {
        info(`Could not read file: ${err instanceof Error ? err.message : err}`);
      }
    } else {
      info('No files found to read yet (Gmail may still be syncing)');
    }

    // ----------------------------------------------------------------
    // 9. List smart folders
    // ----------------------------------------------------------------
    step(9, 'List smart folders');
    const folders = await client.smartFolders.list(workspaceId);
    for (const f of folders) {
      console.log(`    📂 ${f.name} → ${f.path} (${f.integration})`);
    }
    ok(`${folders.length} smart folder(s)`);

    // ----------------------------------------------------------------
    // Done!
    // ----------------------------------------------------------------
    console.log(`\n${BOLD}${GREEN}🎉 Demo complete!${RESET}`);
    console.log(`\n${DIM}Workspace "${ws.name}" (${workspaceId}) is still active.`);
    console.log(`To clean up: await client.workspaces.del("${workspaceId}")${RESET}\n`);

  } catch (err) {
    console.error(`\n❌ Failed:`, err);
    process.exitCode = 1;

    // Clean up on failure
    if (workspaceId) {
      try {
        await client.workspaces.del(workspaceId);
        info(`Cleaned up workspace ${workspaceId}`);
      } catch {
        // ignore
      }
    }
  }
}

main();
