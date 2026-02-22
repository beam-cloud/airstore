---
name: browser-automation
description: Automate web browsers using agent-browser CLI with Kernel cloud sessions. Use when a task requires navigating websites, filling forms, clicking elements, or extracting page content.
metadata:
  airstore:
    needs:
      - browser
---

# Browser Automation

You have `agent-browser` installed. It connects to Kernel cloud browsers -- no local Chromium needed.

## Environment

- `KERNEL_API_KEY` is set in the environment.
- `AGENT_BROWSER_PROVIDER=kernel` is the default provider.

## Core Workflow

Always follow this pattern: **snapshot before you act**.

```bash
# 1. Open a page
agent-browser open https://example.com

# 2. Snapshot to see interactive elements with refs
agent-browser snapshot -i

# 3. Use refs from the snapshot to interact
agent-browser click @e2
agent-browser fill @e3 "user@example.com"

# 4. Re-snapshot after the page changes
agent-browser snapshot -i
```

## Commands Reference

### Navigation
```
agent-browser open <url>
agent-browser back
agent-browser forward
agent-browser reload
```

### Observe (always do this before interacting)
```
agent-browser snapshot -i              # Interactive elements only (recommended)
agent-browser snapshot -i -c           # Compact mode (less noise)
agent-browser screenshot               # Visual capture
agent-browser screenshot --annotate    # Numbered labels on elements
```

### Interact (use @refs from snapshot)
```
agent-browser click @e1                # Click element
agent-browser fill @e2 "text"          # Clear field and type
agent-browser type @e3 "text"          # Append text
agent-browser press Enter              # Press key
agent-browser hover @e4                # Hover element
agent-browser select @e5 "option"      # Select dropdown
```

### Wait
```
agent-browser wait 2000               # Wait ms
agent-browser wait "#element"          # Wait for element visible
agent-browser wait --text "Success"    # Wait for text to appear
agent-browser wait --load networkidle  # Wait for network idle
```

### Extract Data
```
agent-browser get text @e1             # Element text
agent-browser get value @e2            # Input value
agent-browser get title                # Page title
agent-browser get url                  # Current URL
agent-browser eval "document.title"    # Run JavaScript
```

### Session Management
```
agent-browser close                    # End session
```

## Rules

1. **Always snapshot before clicking or typing.** Refs change when the page updates.
2. **Use `--json` when you need to parse output programmatically.** Example: `agent-browser snapshot -i --json`
3. **Re-snapshot after any action that changes the page** (click, fill, navigation).
4. **Handle errors.** If a click fails, re-snapshot and find the correct ref. Retry up to 3 times.
5. **Always close the session when done**, even if an error occurs.
6. **Use `fill` instead of `type`** for form fields -- `fill` clears existing content first.
7. **Use `-i` (interactive) flag on snapshots** to reduce output size and focus on actionable elements.

## Error Recovery

If an action fails:
1. Run `agent-browser snapshot -i` to get the current page state.
2. Check if the page changed (navigation, modal, loading state).
3. Find the correct ref for your target element.
4. Retry the action with the updated ref.
5. If still failing after 3 attempts, take a screenshot and report the issue.

## Kernel-Specific Options

For stealth mode or persistent profiles, set environment variables before running:

```bash
export KERNEL_STEALTH=true              # Avoid bot detection
export KERNEL_PROFILE_NAME=my-profile   # Persist cookies/logins across sessions
export KERNEL_TIMEOUT_SECONDS=600       # Longer session timeout
```
