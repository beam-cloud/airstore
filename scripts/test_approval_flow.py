#!/usr/bin/env python3
"""
Test script to verify that tasks requiring approval/input stay in the
'waiting' state and do not prematurely transition to 'done'.

Usage:
    python3 scripts/test_approval_flow.py \
        --token <api-token> \
        --base-url http://localhost:1994/api/v1 \
        [--wait-duration 180]
"""

import argparse
import json
import sys
import time
import uuid
import urllib.request
import urllib.error

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def ts():
    return time.strftime("%H:%M:%S")


def info(msg):
    print(f"{CYAN}[{ts()}]{RESET} {msg}")


def warn(msg):
    print(f"{YELLOW}[{ts()}] WARN:{RESET} {msg}")


def fail(msg):
    print(f"{RED}{BOLD}[{ts()}] FAIL:{RESET}{RED} {msg}{RESET}")


def ok(msg):
    print(f"{GREEN}{BOLD}[{ts()}] PASS:{RESET}{GREEN} {msg}{RESET}")


def state_color(state):
    colors = {
        "queued": YELLOW,
        "running": CYAN,
        "waiting": BOLD + YELLOW,
        "done": GREEN,
        "error": RED,
        "cancelled": RED,
        "dropped": RED,
        "sleeping": YELLOW,
    }
    return colors.get(state, "")


# ---------------------------------------------------------------------------
# HTTP helpers (stdlib only -- no external deps)
# ---------------------------------------------------------------------------

def api_request(base_url, path, token, method="GET", body=None):
    url = f"{base_url}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode(errors="replace")
        fail(f"{method} {path} -> {e.code}: {body_text[:500]}")
        sys.exit(1)


def stream_sse(base_url, path, token, timeout=300):
    """Yield parsed SSE data dicts from a streaming endpoint."""
    url = f"{base_url}{path}"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "text/event-stream")
    resp = urllib.request.urlopen(req, timeout=timeout)
    buf = b""
    event_name = None
    while True:
        chunk = resp.read(1)
        if not chunk:
            break
        buf += chunk
        if buf.endswith(b"\n\n") or buf.endswith(b"\r\n\r\n"):
            lines = buf.decode(errors="replace").strip().splitlines()
            buf = b""
            data_parts = []
            for line in lines:
                if line.startswith("event:"):
                    event_name = line[len("event:"):].strip()
                elif line.startswith("data:"):
                    data_parts.append(line[len("data:"):].strip())
            if data_parts:
                raw = "\n".join(data_parts)
                try:
                    yield event_name, json.loads(raw)
                except json.JSONDecodeError:
                    pass
            event_name = None


# ---------------------------------------------------------------------------
# Main test
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Test approval flow persistence")
    parser.add_argument("--token", required=True, help="API bearer token")
    parser.add_argument("--base-url", default="http://localhost:1994/api/v1")
    parser.add_argument("--wait-duration", type=int, default=180,
                        help="Seconds to wait while task should remain in 'waiting' (default 180)")
    parser.add_argument("--workspace-id", help="Workspace ID (auto-discovered if omitted)")
    parser.add_argument("--agent-id", help="Agent ID (auto-discovered if omitted)")
    args = parser.parse_args()

    base = args.base_url.rstrip("/")
    token = args.token

    # -- Discover workspace --------------------------------------------------
    workspace_id = args.workspace_id
    if not workspace_id:
        info("Discovering workspace via whoami...")
        resp = api_request(base, "/auth/whoami", token)
        whoami = resp.get("data", {})
        workspace_id = whoami.get("workspace_id")
        if not workspace_id:
            fail("Could not determine workspace_id from token")
            sys.exit(1)
        info(f"Using workspace: {workspace_id} ({whoami.get('workspace_name', '?')})")

    # -- Discover agent ------------------------------------------------------
    agent_id = args.agent_id
    if not agent_id:
        info("Discovering agents...")
        resp = api_request(base, f"/workspaces/{workspace_id}/agents", token)
        agents = resp.get("data", [])
        if not agents:
            fail("No agents found in workspace")
            sys.exit(1)
        agent_id = agents[0]["id"]
        info(f"Using agent: {agent_id} ({agents[0].get('name', '?')})")

    # -- Create task with approval-triggering prompt -------------------------
    session_id = str(uuid.uuid4())
    idempotency_key = str(uuid.uuid4())
    prompt = (
        "Draft an email to test@example.com with the subject 'Weekly Report' and body "
        "'Here are the weekly metrics...'. Show me the draft and ask for my approval "
        "before sending. Do NOT send it until I explicitly approve."
    )

    info(f"Creating task (session={session_id[:8]}...)...")
    create_resp = api_request(
        base,
        f"/workspaces/{workspace_id}/tasks",
        token,
        method="POST",
        body={
            "message": prompt,
            "agent_id": agent_id,
            "session_id": session_id,
            "idempotency_key": idempotency_key,
        },
    )
    task_data = create_resp.get("data", {})
    task = task_data.get("task", {})
    task_id = task.get("id")
    if not task_id:
        fail(f"Task creation did not return task id: {json.dumps(create_resp, indent=2)}")
        sys.exit(1)

    initial_state = task.get("state", "?")
    info(f"Task created: {task_id}  state={state_color(initial_state)}{initial_state}{RESET}")

    # -- Poll task state (SSE may not be available for all setups) ------------
    info(f"Polling task state every 5s for up to {args.wait_duration}s...")
    info("Looking for: running -> waiting (should persist) vs waiting -> done (the bug)")
    print()

    transitions = []
    prev_state = initial_state
    saw_waiting = False
    waiting_at = None
    bug_detected = False
    start = time.time()

    while time.time() - start < args.wait_duration:
        time.sleep(5)
        resp = api_request(base, f"/workspaces/{workspace_id}/tasks/{task_id}", token)
        t = resp.get("data", {})
        state = t.get("state", "?")
        input_kind = t.get("input_kind", "")
        waiting_summary = t.get("waiting_summary", "")
        blocker_id = t.get("current_blocker_id", "")
        elapsed = time.time() - start

        if state != prev_state:
            sc = state_color(state)
            extra = ""
            if input_kind:
                extra += f"  input_kind={input_kind}"
            if waiting_summary:
                extra += f"  summary={waiting_summary[:60]}"
            if blocker_id:
                extra += f"  blocker={blocker_id[:12]}"
            print(f"  {BOLD}{prev_state}{RESET} -> {sc}{state}{RESET}  "
                  f"(+{elapsed:.0f}s){extra}")
            transitions.append((elapsed, prev_state, state))
            prev_state = state

        if state == "waiting":
            if not saw_waiting:
                saw_waiting = True
                waiting_at = time.time()
                info("Task entered 'waiting' -- monitoring for premature exit...")

        if saw_waiting and state in ("done", "error", "cancelled", "dropped"):
            waited_for = time.time() - (waiting_at or start)
            bug_detected = True
            fail(f"Task left 'waiting' -> '{state}' after {waited_for:.0f}s WITHOUT user input!")
            fail("This confirms the bug: run settlement overrides the waiting state.")
            break

        if state in ("done", "error", "cancelled", "dropped") and not saw_waiting:
            warn(f"Task reached terminal state '{state}' without ever entering 'waiting'.")
            warn("The prompt may not have triggered an approval flow. Try a different agent/prompt.")
            break

    print()

    # -- Summary -------------------------------------------------------------
    if bug_detected:
        fail("BUG CONFIRMED: approval/waiting state does not persist.")
        print()
        print(f"  {BOLD}Transitions:{RESET}")
        for elapsed, f, t in transitions:
            print(f"    +{elapsed:5.0f}s  {f} -> {t}")
        print()
        print(f"  {BOLD}Root cause:{RESET} When needsInput=true, the worker does not set")
        print(f"  WaitingForInput=true on RunExecutionResult.PostRun.")
        print(f"  Settlement calls Settle(WaitingForInput=false) which overrides")
        print(f"  the live 'waiting' state to 'done'.")
        print()
        print(f"  {BOLD}Fix:{RESET} In pkg/worker/interactive_task.go ~line 512:")
        print(f"  When needsInput is true, build postRun with WaitingForInput: true")
        sys.exit(1)

    elif saw_waiting and prev_state == "waiting":
        ok(f"Task stayed in 'waiting' for {args.wait_duration}s -- approval flow is working correctly.")
        sys.exit(0)

    else:
        warn(f"Test inconclusive. Final state: {prev_state}")
        warn("Transitions observed:")
        for elapsed, f, t in transitions:
            print(f"    +{elapsed:5.0f}s  {f} -> {t}")
        sys.exit(2)


if __name__ == "__main__":
    main()
