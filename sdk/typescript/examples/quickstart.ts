import Airstore from '@airstore/sdk'

// Initialize — reads AIRSTORE_API_KEY from env automatically
const airstore = new Airstore()

// Create a workspace
const ws = await airstore.workspaces.create({ name: 'my-workspace' })

// Generate a mount token for headless VMs
const { token } = await airstore.tokens.create(ws.external_id, { name: 'vm-mount' })
console.log(`Mount with: airstore mount ~/airstore --token ${token}`)

// Connect Gmail via OAuth (open the URL in your browser)
const session = await airstore.oauth.createSession({ integrationType: 'gmail', workspaceId: ws.external_id })
console.log(`Authorize Gmail: ${session.authorize_url}`)

// Wait for the user to complete OAuth
const completed = await airstore.oauth.poll(session.session_id)
console.log(`Connected! Connection: ${completed.connection_id}`)

// Create a source view (smart mode — LLM-inferred query)
const folder = await airstore.views.create(ws.external_id, {
  integration: 'gmail', name: 'Unread Emails', guidance: 'unread inbox emails', outputFormat: 'folder',
})

// Or create a source view with a structured filter (query mode — auto-detected)
await airstore.views.create(ws.external_id, {
  integration: 'gmail', name: 'From Boss',
  filter: { from: 'boss@company.com', is_unread: true },
})

// GitHub query mode with content_type for diff output
await airstore.views.create(ws.external_id, {
  integration: 'github', name: 'Open PRs',
  filter: { repo: 'acme/api', type: 'prs', state: 'open', content_type: 'diff' },
})

// Browse the virtual filesystem
const files = await airstore.fs.list(ws.external_id, { path: folder.path })
files.forEach((f) => console.log(`${f.is_folder ? '📁' : '📄'} ${f.name}`))

// Manually sync a view (refresh metadata from the source)
const syncResult = await airstore.views.sync(ws.external_id, folder.external_id)
console.log(`Synced: ${syncResult.results_count} total, ${syncResult.new_results} new`)

// Read a file
const first = files.find((f) => !f.is_folder)
if (first) console.log(await airstore.fs.read(ws.external_id, { path: first.path! }))
