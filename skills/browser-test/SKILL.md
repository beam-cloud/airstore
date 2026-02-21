---
name: browser-test
description: End-to-end browser automation test using Kernel cloud browsers
metadata:
  author: beam-cloud
  airstore:
    writes:
      - /Memory/browser-test/
---

# Browser Automation Test

You have a browser tool at `/workspace/tools/browser` that controls a headless
cloud browser via Kernel. It wraps the `agent-browser` CLI.

## Your Task

Perform the following browser automation sequence and save structured results.

### Step 1 — Open Hacker News

```bash
/workspace/tools/browser open https://news.ycombinator.com
```

### Step 2 — Snapshot the page

Take an interactive snapshot to understand the page structure:

```bash
/workspace/tools/browser snapshot --interactive
```

### Step 3 — Extract top stories

Use `eval` to extract the top 5 stories as JSON:

```bash
/workspace/tools/browser eval 'JSON.stringify(
  Array.from(document.querySelectorAll(".titleline > a")).slice(0,5).map((a,i) => ({
    rank: i+1,
    title: a.textContent,
    url: a.href
  }))
)'
```

### Step 4 — Click the first story

From the snapshot, click the first story link using its accessibility ref
(e.g. `@e3` or whichever ref corresponds to the first `.titleline > a`).

```bash
/workspace/tools/browser click <ref>
```

### Step 5 — Capture the destination

Get the current page title and URL:

```bash
/workspace/tools/browser get title
/workspace/tools/browser get url
```

### Step 6 — Go back and verify

```bash
/workspace/tools/browser back
/workspace/tools/browser get title
```

### Step 7 — Save results

Write a JSON file to `/workspace/Memory/browser-test/results.json` containing:

```json
{
  "timestamp": "<ISO 8601>",
  "stories": [ ... the 5 extracted stories ... ],
  "visited": {
    "title": "<title of the story page>",
    "url": "<url of the story page>"
  },
  "status": "success"
}
```

Create the directory first: `mkdir -p /workspace/Memory/browser-test`

### Step 8 — Clean up

```bash
/workspace/tools/browser close
```

## Important Notes

- The browser tool is a local CLI wrapper around `agent-browser`.
  Run it directly with Bash — it is NOT an MCP tool.
- Each command prints its result to stdout. Parse the output as needed.
- If a command fails, retry once before reporting failure.
- The browser session persists across commands (stateful daemon).
