---
name: browser-test
description: End-to-end browser automation test using Kernel cloud browsers. Use when the user asks to test or verify browser automation, run a browser smoke test, or validate that the Kernel cloud browser tool is working correctly.
metadata:
  author: beam-cloud
  airstore-writes: "/memory/browser-test/"
---

# Browser Automation Test

You have a browser tool at `/workspace/tools/browser` that controls a headless
cloud browser via Kernel. Run it directly with Bash — it is NOT an MCP tool.

## Goal

1. Open https://news.ycombinator.com
2. Take an interactive snapshot (`snapshot --interactive`) to see element refs
3. Use `eval` to extract the top 5 story titles and URLs as JSON
4. Click the first story link using its ref from the snapshot
5. Get the destination page title and URL
6. Navigate back and confirm the title is "Hacker News"
7. Close the browser session

## Save Results

Write structured JSON to `/workspace/memory/browser-test/results.json`:

```json
{
  "timestamp": "<ISO 8601>",
  "stories": [{"rank": 1, "title": "...", "url": "..."}, ...],
  "visited": {"title": "...", "url": "..."},
  "status": "success"
}
```

Create the directory first: `mkdir -p /workspace/memory/browser-test`

## Notes

- If a command fails, retry once before reporting failure.
- The browser session persists across commands (stateful daemon).
