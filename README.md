# Airstore

**The filesystem for AI agents**

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/beam-cloud/airstore)](https://github.com/beam-cloud/airstore/stargazers)

[Website](https://airstore.ai) · [Docs](https://docs.airstore.ai) · [Demo Video](https://youtu.be/rWJo4wJe4wY) · [Discord](https://discord.gg/6y5UUsXpqt)

---

## What is Airstore?

Airstore adds any source of data into a virtual filesystem. Connect Gmail, GitHub, Linear — then describe what you need in plain English. Results appear as files that Claude Code can read.

⭐ If this is useful, [give us a star](https://github.com/beam-cloud/airstore)

## Why files?

Claude Code already knows how to read files, search directories, and work with file contents. By turning your integrations into files, Claude can use the same tools it uses for code, for your data.

Source views also let you scope exactly what Claude can access. Instead of granting access to your entire inbox, create a view with just "invoices from last week" — or use structured filters for precise control.

## Features

- **Source views** - Natural language or structured queries that materialize as folders of files
- **Integrations** - Connect GitHub, Gmail, Google Drive, Linear, Notion, Slack, Web
- **Tools** - MCP servers exposed as executable binaries
- **Team workspaces** - Share integrations and source views across your team
- **Local mode** - Run entirely on your own infrastructure

## Quick Start

### 1. Sign up

Go to [app.airstore.ai](https://app.airstore.ai) and create an account.

### 2. Connect an Integration

In the dashboard, go to **Settings → Integrations** and connect a service (GitHub, Gmail, Drive, and more.).

### 3. Create a source view

Click **New Folder** and describe what you want in natural language, or use structured filters:

- "Open PRs in acme/api that need review"
- "Invoices I received in email last week"
- "High priority issues in the current sprint"

### 4. Install the CLI and Login

```bash
curl -fsSL https://get.airstore.ai | sh
```

```
airstore login
```

### 5. Mount the filesystem

```bash
airstore mount ~/airstore 
```

Your source views are now available as local directories:

```bash
ls ~/airstore/gmail/invoices/
# stripe-invoice-jan-28.eml
# aws-invoice-jan-25.eml
# digitalocean-jan-22.eml
```

```bash
cat ~/airstore/gmail/invoices/stripe-invoice-jan-28.eml

From: billing@stripe.com
Subject: Your invoice from Stripe
Date: Jan 28, 2025

Amount: $249.00
Status: Paid
```

### 6. Use with Claude Code

```bash
cd ~/airstore
claude
```

Ask Claude to work with your data:

- "Summarize these invoices and tell me the total"
- "Which invoices are unpaid?"
- "Extract vendor names and amounts into a CSV"

## How it works

```
~/airstore/
├── linear/
│   └── design-issues/     # Source view
├── github/
│   └── open-prs/          # Source view
├── gmail/
│   └── invoices/          # Source view
└── tools/
    ├── github             # MCP tool executable
    ├── linear             # MCP tool executable
    └── gmail              # MCP tool executable
```

**Source views** contain data from your integrations, materialized as files. They sync automatically in the background, or manually on demand.

**Tools** are MCP servers exposed as executables that let you take actions:

```bash
~/airstore/tools/github create-issue --repo=acme/api --title="Bug fix needed"
```

Tools output JSON and can be piped together with standard Unix tools:

```bash
~/airstore/tools/wikipedia search "albert" | grep -i 'einstein'
```

## Installation

The install script handles everything:

```bash
curl -fsSL https://get.airstore.ai | sh
```

This automatically:
- Downloads the correct binary for your platform (macOS/Linux, Intel/ARM)
- Installs FUSE dependencies
- Configures permissions

Verify the installation:

```bash
airstore --version
```

### System requirements

| Platform | Status |
|----------|--------|
| macOS (Apple Silicon) | Supported |
| macOS (Intel) | Supported |
| Linux (x86_64) | Supported |
| Linux (ARM64) | Supported |
| Windows | Coming soon |

---

<details>
<summary><strong>Local Mode (Self-Hosted)</strong></summary>

For development or self-hosted deployments, you can run Airstore entirely on your own infrastructure with your own MCP servers.

### Prerequisites

**FUSE** (required):

```bash
# macOS
brew install fuse-t

# Ubuntu/Debian
sudo apt install fuse3
```

**Node.js** (for npx-based MCP servers):

```bash
# macOS
brew install node

# Ubuntu/Debian
sudo apt install nodejs npm
```

### Configuration

Create a `config.local.yaml` file:

```yaml
mode: local

gateway:
  grpc:
    port: 1993
  http:
    host: 127.0.0.1
    port: 1994

tools:
  mcp:
    filesystem:
      command: npx
      args: ["-y", "@modelcontextprotocol/server-filesystem", "/tmp", "/Users"]

    memory:
      command: npx
      args: ["-y", "@modelcontextprotocol/server-memory"]

    wikipedia:
      command: npx
      args: ["-y", "@modelcontextprotocol/server-wikipedia"]

    github:
      command: npx
      args: ["-y", "@modelcontextprotocol/server-github"]
      env:
        GITHUB_TOKEN: "${GITHUB_TOKEN}"
```

### Mount locally

```bash
airstore mount ~/airstore --config config.local.yaml
```

Your tools are now available:

```bash
ls ~/airstore/tools/
# filesystem  memory  wikipedia  github

# Search Wikipedia
~/airstore/tools/wikipedia search "artificial intelligence"

# List files
~/airstore/tools/filesystem list_directory /tmp

# Get help for any tool
~/airstore/tools/filesystem --help
```

### Building from source

```bash
git clone https://github.com/beam-cloud/airstore
cd airstore
make build
./bin/cli mount ~/airstore --config config.local.yaml
```

</details>

---

## Documentation

Full documentation at [docs.airstore.ai](https://docs.airstore.ai):

- [Quickstart](https://docs.airstore.ai/quickstart) - Get running in 5 minutes
- [Source Views](https://docs.airstore.ai/concepts/source-views) - Create dynamic data views
- [Tools](https://docs.airstore.ai/concepts/tools) - Run MCP tools as CLI commands
- [CLI Reference](https://docs.airstore.ai/reference/cli) - Full command documentation

## Contributing

PRs welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

Join our [Discord](https://discord.gg/6y5UUsXpqt) to discuss ideas and get help.

## License

AGPL 3.0 License - see [LICENSE](LICENSE) for details.
