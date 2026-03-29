#!/usr/bin/env python3
"""
E2E test for source watch / follow-up wake cycle.

Uses the test action in the Laundromat Site Finder view to:
1. Create a task that sends a test outreach email
2. Monitor the task until it reaches 'sleeping' state
3. Verify filesystem_queries + filesystem_hooks + task_source_watches are created
4. (Manual step) Reply to the email thread
5. Verify the task wakes up and processes the reply
"""

import json
import os
import subprocess
import sys
import time
import requests

API_BASE = "http://localhost:1994/api/v1"
TOKEN = "e8c8f312c3a7959d56b8186521add2374bc1cd7c896a2a21ba1a12a402e9f72f"
WORKSPACE_EXT_ID = "737af74d-902f-4464-9eb2-19cbd4dd0247"
VIEW_ID = "3cd21c07-70f2-47db-a7c7-ebf82a9baf06"
OUTREACH_AGENT_ID = "16101cc1-b294-4cd1-af52-5a1dd4b68f35"
WORKSPACE_INT_ID = 347

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def api(method, path, body=None):
    url = f"{API_BASE}/workspaces/{WORKSPACE_EXT_ID}{path}"
    resp = requests.request(method, url, headers=HEADERS, json=body, timeout=30)
    return resp.json()


def psql(query):
    result = subprocess.run(
        ["kubectl", "exec", "-n", "airstore", "postgres-6fd8bf6449-6znmn", "--",
         "psql", "-U", "airstore", "-d", "airstore", "-t", "-A", "-c", query],
        capture_output=True, text=True, timeout=15,
    )
    return result.stdout.strip()


def cancel_sleeping_tasks():
    print("\n=== Cancelling old sleeping tasks ===")
    rows = psql(f"SELECT id FROM agent_task WHERE workspace_id = {WORKSPACE_INT_ID} AND state = 'sleeping';")
    if not rows:
        print("  No sleeping tasks to cancel.")
        return
    for task_id in rows.strip().split("\n"):
        task_id = task_id.strip()
        if task_id:
            resp = api("POST", f"/tasks/{task_id}/cancel")
            print(f"  Cancelled {task_id}: {resp.get('success', resp)}")


def create_test_task():
    print("\n=== Creating test task via test action ===")
    prompt = (
        "Send a test outreach email to Test Broker at luke@beam.cloud "
        "inquiring about laundromat use approval. "
        "Property address: 999 Source Watch Test Blvd, Brooklyn NY 11201. "
        "Additional context: This is an automated test of the source watch wake mechanism. "
        "Use the standard laundromat inquiry template, sign as Eli, "
        "and create as a Gmail draft for approval before sending."
    )
    body = {
        "message": prompt,
        "agent_id": OUTREACH_AGENT_ID,
        "source_view_id": VIEW_ID,
    }
    resp = api("POST", "/tasks", body)
    if not resp.get("success") and not resp.get("data", {}).get("accepted"):
        print(f"  FAIL: {json.dumps(resp, indent=2)}")
        sys.exit(1)
    task = resp["data"]["task"]
    task_id = task["id"]
    print(f"  Created task: {task_id}")
    print(f"  State: {task['state']}")
    return task_id


def wait_for_state(task_id, target_state, timeout_sec=300, poll_sec=5):
    print(f"\n=== Waiting for task {task_id[:8]} to reach '{target_state}' (timeout {timeout_sec}s) ===")
    start = time.time()
    last_state = None
    while time.time() - start < timeout_sec:
        resp = api("GET", f"/tasks/{task_id}")
        task = resp.get("data", resp)
        state = task.get("state", "unknown")
        if state != last_state:
            elapsed = int(time.time() - start)
            wake_reason = task.get("wake_reason", "")
            print(f"  [{elapsed:3d}s] state={state}" + (f" wake_reason={wake_reason[:80]}" if wake_reason else ""))
            last_state = state
        if state == target_state:
            return task
        if state in ("error", "cancelled", "completed"):
            print(f"  Task reached terminal state '{state}' instead of '{target_state}'")
            return task
        time.sleep(poll_sec)
    print(f"  TIMEOUT after {timeout_sec}s — last state: {last_state}")
    return None


def check_source_watches(task_id):
    print(f"\n=== Checking source watches for task {task_id[:8]} ===")

    # Check filesystem_queries
    queries = psql(
        f"SELECT id, name, path, integration FROM filesystem_queries "
        f"WHERE workspace_id = {WORKSPACE_INT_ID} AND owner_task_id = '{task_id}' AND system_managed = true;"
    )
    query_count = len([l for l in queries.split("\n") if l.strip()]) if queries else 0
    print(f"  filesystem_queries (system_managed): {query_count}")
    if queries:
        for line in queries.strip().split("\n"):
            print(f"    {line}")

    # Check filesystem_hooks
    hooks = psql(
        f"SELECT id, path, delivery_mode, target_task_id, system_managed, active "
        f"FROM filesystem_hooks "
        f"WHERE workspace_id = {WORKSPACE_INT_ID} AND system_managed = true AND target_task_id = '{task_id}';"
    )
    hook_count = len([l for l in hooks.split("\n") if l.strip()]) if hooks else 0
    print(f"  filesystem_hooks (system_managed, targeting task): {hook_count}")
    if hooks:
        for line in hooks.strip().split("\n"):
            print(f"    {line}")

    # Check task_source_watches correlation index
    watches = psql(
        f"SELECT id, integration, correlation_key, reason "
        f"FROM task_source_watches WHERE task_id = '{task_id}';"
    )
    watch_count = len([l for l in watches.split("\n") if l.strip()]) if watches else 0
    print(f"  task_source_watches (correlation index): {watch_count}")
    if watches:
        for line in watches.strip().split("\n"):
            print(f"    {line}")

    # Verdict
    print()
    if query_count > 0 and hook_count > 0 and watch_count > 0:
        print("  PASS: All three watch mechanisms are in place")
        return True
    else:
        missing = []
        if query_count == 0:
            missing.append("filesystem_queries")
        if hook_count == 0:
            missing.append("filesystem_hooks")
        if watch_count == 0:
            missing.append("task_source_watches")
        print(f"  FAIL: Missing: {', '.join(missing)}")
        return False


def check_all_hooks():
    """Debug: show ALL hooks in the workspace."""
    print(f"\n=== All hooks in workspace {WORKSPACE_INT_ID} ===")
    hooks = psql(
        f"SELECT id, path, delivery_mode, target_task_id, system_managed, active "
        f"FROM filesystem_hooks WHERE workspace_id = {WORKSPACE_INT_ID};"
    )
    if hooks:
        for line in hooks.strip().split("\n"):
            print(f"  {line}")
    else:
        print("  (none)")


def main():
    print("=" * 60)
    print("Source Watch E2E Test")
    print("=" * 60)

    # Step 0: Cancel old sleeping tasks
    cancel_sleeping_tasks()

    # Step 1: Create task
    task_id = create_test_task()

    # Step 2: Wait for sleeping
    task = wait_for_state(task_id, "sleeping", timeout_sec=300, poll_sec=5)
    if task is None or task.get("state") != "sleeping":
        print("\nFAIL: Task did not reach sleeping state.")
        check_all_hooks()
        sys.exit(1)

    # Step 3: Check source watches
    all_ok = check_source_watches(task_id)
    check_all_hooks()

    if not all_ok:
        print("\nFAIL: Source watches are incomplete — hooks are missing.")
        print("Check gateway logs: tail -f logs/gateway.log | grep -i 'hook\\|watch\\|error'")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("PHASE 1 COMPLETE: Task is sleeping with source watches armed.")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Reply to the email thread that was sent")
    print("  2. Re-run this script with: python3 hack/test_source_watches.py --check-wake <task_id>")

    return task_id


def check_wake(task_id):
    print(f"\n=== Checking wake for task {task_id[:8]} ===")
    resp = api("GET", f"/tasks/{task_id}")
    task = resp.get("data", resp)
    state = task.get("state", "unknown")
    print(f"  Current state: {state}")
    print(f"  Wake count: {task.get('wake_count', 0)}")
    print(f"  Wake reason: {task.get('wake_reason', 'none')}")

    if state == "sleeping":
        print("\n  Task is still sleeping. Waiting for wake...")
        task = wait_for_state(task_id, "running", timeout_sec=120, poll_sec=3)
        if task and task.get("state") in ("running", "dispatched", "queued"):
            print("  Task woke up!")
            task = wait_for_state(task_id, "sleeping", timeout_sec=300, poll_sec=5)
    elif state in ("running", "dispatched", "queued"):
        print("  Task is already running (was woken)!")
        task = wait_for_state(task_id, "sleeping", timeout_sec=300, poll_sec=5)

    if task:
        check_source_watches(task_id)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--check-wake":
        if len(sys.argv) < 3:
            print("Usage: python3 hack/test_source_watches.py --check-wake <task_id>")
            sys.exit(1)
        check_wake(sys.argv[2])
    else:
        main()
