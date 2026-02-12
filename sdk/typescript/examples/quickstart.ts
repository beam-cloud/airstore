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

// Create a smart folder
const folder = await airstore.smartFolders.create(ws.external_id, {
  integration: 'gmail', name: 'Unread Emails', guidance: 'unread inbox emails', outputFormat: 'folder',
})

// Browse the virtual filesystem
const files = await airstore.fs.list(ws.external_id, { path: folder.path })
files.forEach((f) => console.log(`${f.type === 'directory' ? '📁' : '📄'} ${f.name}`))

// Read a file
const first = files.find((f) => f.type !== 'directory')
if (first) console.log(await airstore.fs.read(ws.external_id, { path: first.path! }))
