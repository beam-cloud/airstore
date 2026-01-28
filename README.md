# 💨 Airstore

Airstore is a virtual filesystem for AI agents. 

## Why Airstore?

Agents perform best when they have access to a computer. But your tools, integrations, and context are scattered across external APIs and MCP servers.

Airstore is a virtual filesystem that puts everything your agent needs (tools, context, and events) in a single folder on your computer. That folder is virtualized, so you can access it from anywhere.

We envision a world where your entire company can be represented as a POSIX filesystem that any agent can interact with.

### Features

* Mount a complete agent environment as a local folder
* Access MCP tools and integrations as executable binaries
* Share context across agents and machines
* Trigger background agents on filesystem events (coming soon)

## How it works

Airstore is a custom filesystem where each tool is a virtual file. It lets you chain tool calls using pure bash, without the hassle of dealing with APIs or MCP servers. For example:

```
luke@Lukes-MacBook-Pro tools % ls -lart /tmp/airstore/tools/
total 4295007838
drwxr-xr-x  2 luke  staff          0 Jan 26 20:19 ..
drwxr-xr-x  2 luke  staff          0 Jan 26 20:19 .
-rwxr-xr-x  1 luke  staff   10378242 Jan 26 20:19 github
-rwxr-xr-x  1 luke  staff   10378242 Jan 26 20:19 wikipedia
luke@Lukes-MacBook-Pro tools %
```

```
luke@Lukes-MacBook-Pro tools % /tmp/airstore/tools/wikipedia search "something"
{
  "results": [
    {
      "title": "Something",
      "page_id": 8041208,
      "excerpt": "up something in Wiktionary, the free dictionary. Something may refer to: Something (concept) \u0026quot;Something\u0026quot;, an English indefinite pronoun Something (Chairlift)"
    },
    {
      "title": "Something Something",
      "page_id": 7077513,
      "excerpt": "formerly known as Something Something... Unakkum Enakkum Something Something (2012 film), a 2012 Odia-language film Something Something 2 (2014), a sequel"
    }
  ]
}
```

```
luke@Lukes-MacBook-Pro ~ % /tmp/airstore/tools/wikipedia search "albert" | grep -i 'einstein'
    "title": "Albert Einstein",
    "excerpt": "Albert Einstein (14 March 1879 – 18 April 1955) was a German-born theoretical physicist best known for developing the theory of relativity. Einstein also"
    "title": "Einstein family",
    "excerpt": "The Einstein family is the family of physicist Albert Einstein (1879-1955). Einstein\u0026#039;s fourth-great-grandfather, Jakob Weil, was his oldest recorded relative"
```

## macOS Setup

### 0. Install Go 1.24

The easiest way is the official Go installer:

- Download and run the macOS pkg from https://go.dev/dl/
- Restart your terminal and confirm:

```bash
go version
```

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
