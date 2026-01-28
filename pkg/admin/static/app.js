// Airstore Admin UI - Single Page Application
(function() {
    const app = document.getElementById('app');
    let currentUser = null;
    let workspaces = [];
    let selectedWorkspace = null;
    let workspaceData = { members: [], tokens: [], connections: [] };

    // API helpers
    async function api(method, path, body) {
        const opts = {
            method,
            headers: { 'Accept': 'application/json' }
        };
        if (body) {
            opts.headers['Content-Type'] = 'application/json';
            opts.body = JSON.stringify(body);
        }
        const res = await fetch('/admin/api' + path, opts);
        if (res.status === 401) {
            window.location.href = '/admin/login';
            return null;
        }
        return res.json();
    }

    // Render functions
    function render() {
        app.innerHTML = `
            <nav class="bg-white shadow-sm border-b">
                <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                    <div class="flex justify-between h-16">
                        <div class="flex items-center">
                            <h1 class="text-xl font-bold text-gray-900">Airstore Admin</h1>
                        </div>
                        <div class="flex items-center gap-4">
                            ${currentUser ? `
                                <span class="text-sm text-gray-600">${currentUser.email}</span>
                                <a href="/admin/logout" class="text-sm text-red-600 hover:text-red-800">Logout</a>
                            ` : ''}
                        </div>
                    </div>
                </div>
            </nav>
            <main class="max-w-7xl mx-auto py-6 px-4 sm:px-6 lg:px-8">
                <div class="grid grid-cols-1 lg:grid-cols-4 gap-6">
                    <div class="lg:col-span-1">
                        ${renderWorkspaceList()}
                    </div>
                    <div class="lg:col-span-3">
                        ${selectedWorkspace ? renderWorkspaceDetail() : renderWelcome()}
                    </div>
                </div>
            </main>
        `;
        attachEventListeners();
    }

    function renderWorkspaceList() {
        return `
            <div class="bg-white rounded-lg shadow p-4">
                <div class="flex justify-between items-center mb-4">
                    <h2 class="font-semibold text-gray-900">Workspaces</h2>
                    <button onclick="createWorkspace()" class="text-sm bg-blue-600 text-white px-3 py-1 rounded hover:bg-blue-700">
                        + New
                    </button>
                </div>
                <div class="space-y-2">
                    ${workspaces.length === 0 ? '<p class="text-gray-500 text-sm">No workspaces yet</p>' : ''}
                    ${workspaces.map(ws => `
                        <button onclick="selectWorkspace('${ws.external_id}')" 
                                class="w-full text-left px-3 py-2 rounded ${selectedWorkspace?.external_id === ws.external_id ? 'bg-blue-100 text-blue-700' : 'hover:bg-gray-100'}">
                            <div class="font-medium">${ws.name}</div>
                            <div class="text-xs text-gray-500">${ws.external_id.slice(0, 8)}...</div>
                        </button>
                    `).join('')}
                </div>
            </div>
        `;
    }

    function renderWelcome() {
        return `
            <div class="bg-white rounded-lg shadow p-8 text-center">
                <h2 class="text-xl font-semibold text-gray-900 mb-2">Welcome to Airstore Admin</h2>
                <p class="text-gray-600 mb-4">Select a workspace or create a new one to get started.</p>
            </div>
        `;
    }

    function renderWorkspaceDetail() {
        return `
            <div class="space-y-6">
                <!-- Workspace Header -->
                <div class="bg-white rounded-lg shadow p-4">
                    <div class="flex justify-between items-start">
                        <div>
                            <h2 class="text-xl font-semibold text-gray-900">${selectedWorkspace.name}</h2>
                            <p class="text-sm text-gray-500 font-mono">${selectedWorkspace.external_id}</p>
                        </div>
                        <button onclick="deleteWorkspace('${selectedWorkspace.external_id}')" 
                                class="text-sm text-red-600 hover:text-red-800">Delete</button>
                    </div>
                </div>

                <!-- Tokens Section -->
                <div class="bg-white rounded-lg shadow p-4">
                    <div class="flex justify-between items-center mb-4">
                        <h3 class="font-semibold text-gray-900">API Tokens</h3>
                        <button onclick="createToken()" class="text-sm bg-green-600 text-white px-3 py-1 rounded hover:bg-green-700">
                            + Generate Token
                        </button>
                    </div>
                    ${workspaceData.tokens.length === 0 ? 
                        '<p class="text-gray-500 text-sm">No tokens. Create a member first, then generate a token.</p>' : 
                        `<div class="space-y-2">
                            ${workspaceData.tokens.map(t => `
                                <div class="flex justify-between items-center p-2 bg-gray-50 rounded">
                                    <div>
                                        <span class="font-medium">${t.name}</span>
                                        <span class="text-xs text-gray-500 ml-2">${t.external_id.slice(0, 8)}...</span>
                                    </div>
                                </div>
                            `).join('')}
                        </div>`
                    }
                    <div id="new-token-display" class="hidden mt-4 p-3 bg-green-50 border border-green-200 rounded">
                        <p class="text-sm text-green-800 font-medium mb-2">New token created! Copy it now (won't be shown again):</p>
                        <div class="flex gap-2">
                            <input id="token-value" type="text" readonly class="flex-1 font-mono text-sm bg-white border rounded px-2 py-1">
                            <button onclick="copyToken()" class="text-sm bg-green-600 text-white px-3 py-1 rounded hover:bg-green-700">Copy</button>
                        </div>
                    </div>
                </div>

                <!-- Members Section -->
                <div class="bg-white rounded-lg shadow p-4">
                    <div class="flex justify-between items-center mb-4">
                        <h3 class="font-semibold text-gray-900">Members</h3>
                        <button onclick="createMember()" class="text-sm bg-blue-600 text-white px-3 py-1 rounded hover:bg-blue-700">
                            + Add Member
                        </button>
                    </div>
                    ${workspaceData.members.length === 0 ? 
                        '<p class="text-gray-500 text-sm">No members yet</p>' : 
                        `<div class="space-y-2">
                            ${workspaceData.members.map(m => `
                                <div class="flex justify-between items-center p-2 bg-gray-50 rounded">
                                    <div>
                                        <span class="font-medium">${m.name || m.email}</span>
                                        <span class="text-xs text-gray-500 ml-2">${m.email}</span>
                                        <span class="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded ml-2">${m.role}</span>
                                    </div>
                                    <span class="text-xs text-gray-400 font-mono">${m.external_id.slice(0, 8)}...</span>
                                </div>
                            `).join('')}
                        </div>`
                    }
                </div>

                <!-- Connections Section -->
                <div class="bg-white rounded-lg shadow p-4">
                    <div class="flex justify-between items-center mb-4">
                        <h3 class="font-semibold text-gray-900">Integration Connections</h3>
                        <button onclick="createConnection()" class="text-sm bg-purple-600 text-white px-3 py-1 rounded hover:bg-purple-700">
                            + Connect
                        </button>
                    </div>
                    ${workspaceData.connections.length === 0 ? 
                        '<p class="text-gray-500 text-sm">No integrations connected</p>' : 
                        `<div class="space-y-2">
                            ${workspaceData.connections.map(c => `
                                <div class="flex justify-between items-center p-2 bg-gray-50 rounded">
                                    <div>
                                        <span class="font-medium capitalize">${c.integration_type}</span>
                                        <span class="text-xs ${c.is_shared ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'} px-2 py-0.5 rounded ml-2">
                                            ${c.is_shared ? 'shared' : 'personal'}
                                        </span>
                                    </div>
                                    <button onclick="deleteConnection('${c.external_id}')" class="text-xs text-red-600 hover:text-red-800">Remove</button>
                                </div>
                            `).join('')}
                        </div>`
                    }
                </div>
            </div>
        `;
    }

    // Event handlers
    window.selectWorkspace = async function(id) {
        selectedWorkspace = workspaces.find(w => w.external_id === id);
        await loadWorkspaceData(id);
        render();
    };

    window.createWorkspace = async function() {
        const name = prompt('Workspace name:');
        if (!name) return;
        const ws = await api('POST', '/workspaces', { name });
        if (ws) {
            await loadWorkspaces();
            selectWorkspace(ws.external_id);
        }
    };

    window.deleteWorkspace = async function(id) {
        if (!confirm('Delete this workspace?')) return;
        await api('DELETE', '/workspaces/' + id);
        selectedWorkspace = null;
        await loadWorkspaces();
        render();
    };

    window.createMember = async function() {
        const email = prompt('Member email:');
        if (!email) return;
        const name = prompt('Member name (optional):') || '';
        await api('POST', '/workspaces/' + selectedWorkspace.external_id + '/members', { email, name, role: 'admin' });
        await loadWorkspaceData(selectedWorkspace.external_id);
        render();
    };

    window.createToken = async function() {
        if (workspaceData.members.length === 0) {
            alert('Create a member first before generating a token');
            return;
        }
        const memberId = workspaceData.members[0].external_id;
        const name = prompt('Token name:', 'CLI Token');
        if (!name) return;
        const result = await api('POST', '/workspaces/' + selectedWorkspace.external_id + '/tokens', { member_id: memberId, name });
        if (result && result.token) {
            await loadWorkspaceData(selectedWorkspace.external_id);
            render();
            // Show the token
            document.getElementById('new-token-display').classList.remove('hidden');
            document.getElementById('token-value').value = result.token;
        }
    };

    window.copyToken = function() {
        const input = document.getElementById('token-value');
        input.select();
        document.execCommand('copy');
        alert('Token copied to clipboard!');
    };

    window.createConnection = async function() {
        const integrations = ['github', 'gmail', 'notion', 'gdrive'];
        const type = prompt('Integration type (' + integrations.join(', ') + '):');
        if (!type || !integrations.includes(type)) {
            alert('Invalid integration type');
            return;
        }
        const token = prompt('Access token or API key:');
        if (!token) return;
        await api('POST', '/workspaces/' + selectedWorkspace.external_id + '/connections', { 
            integration_type: type, 
            access_token: token 
        });
        await loadWorkspaceData(selectedWorkspace.external_id);
        render();
    };

    window.deleteConnection = async function(connId) {
        if (!confirm('Remove this connection?')) return;
        await api('DELETE', '/workspaces/' + selectedWorkspace.external_id + '/connections/' + connId);
        await loadWorkspaceData(selectedWorkspace.external_id);
        render();
    };

    function attachEventListeners() {
        // Any additional event listeners
    }

    // Data loading
    async function loadUser() {
        currentUser = await api('GET', '/user');
    }

    async function loadWorkspaces() {
        workspaces = await api('GET', '/workspaces') || [];
    }

    async function loadWorkspaceData(id) {
        const [members, tokens, connections] = await Promise.all([
            api('GET', '/workspaces/' + id + '/members'),
            api('GET', '/workspaces/' + id + '/tokens'),
            api('GET', '/workspaces/' + id + '/connections')
        ]);
        workspaceData = {
            members: members || [],
            tokens: tokens || [],
            connections: connections || []
        };
    }

    // Initialize
    async function init() {
        await loadUser();
        await loadWorkspaces();
        render();
    }

    init();
})();
