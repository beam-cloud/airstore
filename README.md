# Airstore

An agent focused virtual filesystem that exposes MCP servers and other things as executables.

## macOS Setup

### 1. Install FUSE-T

```bash
brew install fuse-t
```

### 2. Install Node.js (for npx)

```bash
brew install node
```

### 3. Build the CLI

```bash
make build
```

### 4. Configure MCP Servers

Edit `config.local.yaml` to add your MCP servers:

```yaml
mode: local
debugMode: true
prettyLogs: true

gateway:
  grpc:
    port: 1993
  http:
    host: 127.0.0.1
    port: 1994

tools:
  mcp:
    # Filesystem access
    filesystem:
      command: npx
      args: ["-y", "@modelcontextprotocol/server-filesystem", "/tmp", "/Users"]

    # Memory/knowledge graph
    memory:
      command: npx
      args: ["-y", "@modelcontextprotocol/server-memory"]

    # GitHub (set GITHUB_TOKEN env var)
    github:
      command: npx
      args: ["-y", "@modelcontextprotocol/server-github"]
      env:
        GITHUB_TOKEN: "${GITHUB_TOKEN}"
```

### 5. Mount

```bash
./bin/cli mount /tmp/airstore --config config.local.yaml
```

### 6. Use

Each registered tool (and any registered MCP servers) become virtual executables in `<MOUNT_PATH>/tools/`:

```bash
# List available tools
ls /tmp/airstore/tools/

# Use filesystem server
/tmp/airstore/tools/filesystem list_directory /tmp

# Use memory server  
/tmp/airstore/tools/memory create_entities '[{"name": "test", "type": "note"}]'

# Get help for any tool
/tmp/airstore/tools/filesystem --help
```

Press `Ctrl+C` to unmount.
