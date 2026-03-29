#!/usr/bin/env python3
"""
End-to-end test for deterministic view row insertion through the real worker.

Creates a task that runs through the full agent pipeline:
  Task creation → Worker picks up → Agent runs → AnalyzerWriter extracts outputs
  → attachViewRowProjections computes cells → gRPC CreateTaskOutput with
  _view_row_projections → Gateway upserts ViewRows to MongoDB → Rows appear
  in resolved view data

Usage:
    python hack/test_deterministic_view.py

Requires: requests
"""

import json
import sys
import time
import uuid

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_BASE = "http://localhost:1994/api/v1"
TOKEN = "1ff209f7be651d691e6ad5d704317ccee09d6652ff97be5191a5b52b6b587093"
WORKSPACE_EXT_ID = "737af74d-902f-4464-9eb2-19cbd4dd0247"

VIEW_ID = "ae0b7d0e-c52e-4ad5-88bb-a038657df98b"
AGENT_ID = "16101cc1-b294-4cd1-af52-5a1dd4b68f35"

SHEET_1 = "sheet-1"
COMPONENT_1 = "c1"
SHEET_2 = "sheet-2"
COMPONENT_2 = "c2"

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

TASK_TIMEOUT_S = 300
POLL_INTERVAL_S = 5

passed = 0
failed = 0
errors = []

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def base_url(path: str) -> str:
    return f"{API_BASE}/workspaces/{WORKSPACE_EXT_ID}{path}"


def check(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  \033[32m✓\033[0m {name}")
    else:
        failed += 1
        msg = f"{name}: {detail}" if detail else name
        errors.append(msg)
        print(f"  \033[31m✗\033[0m {msg}")


def api_get(path: str, params: dict | None = None) -> dict:
    r = requests.get(base_url(path), headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def api_post(path: str, body: dict | None = None) -> dict:
    r = requests.post(base_url(path), headers=HEADERS, json=body or {}, timeout=60)
    r.raise_for_status()
    return r.json()


def get_view_data(sheet: str, component: str) -> dict:
    return api_get(f"/views/{VIEW_ID}/data", params={"sheet": sheet, "component": component})["data"]


def get_task_outputs(task_id: str) -> list:
    resp = api_get(f"/tasks/{task_id}/outputs")
    return resp.get("data", {}).get("outputs", [])


def get_task(task_id: str) -> dict:
    return api_get(f"/tasks/{task_id}")["data"]


TERMINAL_STATES = {"done", "error", "cancelled", "timed_out", "failed"}


def wait_for_task(task_id: str, timeout_s: int = TASK_TIMEOUT_S) -> dict:
    """Poll until the task reaches a terminal state or produces outputs."""
    start = time.time()
    last_state = ""
    last_output_count = 0

    while time.time() - start < timeout_s:
        task = get_task(task_id)
        state = task.get("state", "unknown")
        outputs = get_task_outputs(task_id)
        output_count = len(outputs)

        if state != last_state or output_count != last_output_count:
            elapsed = time.time() - start
            print(f"    [{elapsed:5.0f}s] state={state}, outputs={output_count}")
            last_state = state
            last_output_count = output_count

        if state in TERMINAL_STATES:
            return task

        time.sleep(POLL_INTERVAL_S)

    print(f"    \033[33mTimeout after {timeout_s}s (state={last_state})\033[0m")
    return get_task(task_id)


# ---------------------------------------------------------------------------
# Test 1: Snapshot before state
# ---------------------------------------------------------------------------

def snapshot_view_data() -> dict:
    """Capture current view state across both sheets."""
    d1 = get_view_data(SHEET_1, COMPONENT_1)
    d2 = get_view_data(SHEET_2, COMPONENT_2)
    return {
        "sheet1_total": d1["total"],
        "sheet1_rows": d1["rows"],
        "sheet1_columns": d1["columns"],
        "sheet2_total": d2["total"],
        "sheet2_rows": d2["rows"],
        "sheet2_columns": d2["columns"],
    }


# ---------------------------------------------------------------------------
# Test 2: Create a real task through the worker pipeline
# ---------------------------------------------------------------------------

def create_task() -> dict:
    print("\n--- Creating task through the worker pipeline ---")

    session_id = f"e2e-{uuid.uuid4().hex[:8]}"
    idempotency_key = f"e2e-det-{uuid.uuid4().hex}"

    body = {
        "message": (
            "Draft and send a brief outreach email to test-e2e@example.com "
            "asking about a laundromat property at 123 Test Street, Testville, CA 90210. "
            "The property owner is Test Owner at Test Realty. "
            "Keep it very short — one paragraph."
        ),
        "agent_id": AGENT_ID,
        "session_id": session_id,
        "idempotency_key": idempotency_key,
        "source_view_id": VIEW_ID,
    }

    resp = api_post("/tasks", body=body)
    check("Task creation accepted", resp.get("success") or resp.get("data", {}).get("accepted"))
    task = resp["data"]["task"]
    task_id = task["id"]
    print(f"    Task ID: {task_id}")
    print(f"    State: {task['state']}")
    return task


# ---------------------------------------------------------------------------
# Test 3: Wait for worker to process the task
# ---------------------------------------------------------------------------

def test_task_execution(task_id: str) -> dict:
    print("\n--- Waiting for worker to process task ---")

    task = wait_for_task(task_id)
    state = task.get("state", "unknown")

    check(
        "Task reached terminal state",
        state in TERMINAL_STATES,
        f"state={state}",
    )
    check(
        "Task completed successfully",
        state == "done" or state == "waiting",
        f"state={state}",
    )
    return task


# ---------------------------------------------------------------------------
# Test 4: Verify outputs were created through the worker pipeline
# ---------------------------------------------------------------------------

def test_outputs_created(task_id: str) -> list:
    print("\n--- Verifying task outputs ---")

    outputs = get_task_outputs(task_id)
    check("Task produced outputs", len(outputs) > 0, f"count={len(outputs)}")

    output_types = set(o["output_type"] for o in outputs)
    print(f"    Output types: {output_types}")
    print(f"    Output count: {len(outputs)}")

    for o in outputs[:5]:
        title = o["title"][:60]
        meta_keys = list(o.get("metadata", {}).keys())
        data_keys = list(o.get("data", {}).keys())
        print(f"    - [{o['output_type']}] {title}")
        print(f"      data: {data_keys[:6]}{'...' if len(data_keys) > 6 else ''}")

    has_email = "email" in output_types
    has_file = "file" in output_types
    check(
        "Outputs include email or file types",
        has_email or has_file,
        f"types={output_types}",
    )

    return outputs


# ---------------------------------------------------------------------------
# Test 5: Verify deterministic view row insertion
# ---------------------------------------------------------------------------

def test_deterministic_insertion(before: dict):
    print("\n--- Verifying deterministic view row insertion ---")

    after_s1 = get_view_data(SHEET_1, COMPONENT_1)
    after_s2 = get_view_data(SHEET_2, COMPONENT_2)

    print(f"    Sheet 1: {before['sheet1_total']} -> {after_s1['total']} rows")
    print(f"    Sheet 2: {before['sheet2_total']} -> {after_s2['total']} rows")

    new_s1 = after_s1["total"] - before["sheet1_total"]
    new_s2 = after_s2["total"] - before["sheet2_total"]
    total_new = new_s1 + new_s2

    check(
        "New rows appeared in at least one sheet",
        total_new > 0,
        f"new_s1={new_s1}, new_s2={new_s2}",
    )

    cols_s1 = after_s1["columns"]

    if new_s1 > 0 and "property_name" in cols_s1:
        name_idx = cols_s1.index("property_name")
        new_names = []
        for row in after_s1["rows"]:
            val = row[name_idx] if name_idx < len(row) else None
            if val and str(val).strip():
                new_names.append(str(val))

        check(
            "New rows have populated property names",
            len(new_names) > before["sheet1_total"],
            f"populated_names={len(new_names)}, before_total={before['sheet1_total']}",
        )

    if after_s1["total"] > 0:
        email_keys = [k for k in cols_s1 if "email" in k.lower()]
        if email_keys:
            email_idx = cols_s1.index(email_keys[0])
            emails_found = sum(
                1 for row in after_s1["rows"]
                if email_idx < len(row) and row[email_idx] and "@" in str(row[email_idx])
            )
            print(f"    Rows with email addresses: {emails_found}")

    return after_s1, after_s2


# ---------------------------------------------------------------------------
# Test 6: Verify cell content quality
# ---------------------------------------------------------------------------

def test_cell_quality(after_s1: dict, outputs: list):
    print("\n--- Verifying cell content quality ---")

    cols = after_s1["columns"]
    rows = after_s1["rows"]

    if not rows:
        check("Has rows to verify", False, "no rows")
        return

    non_empty_cells = 0
    total_cells = 0
    for row in rows:
        for i, val in enumerate(row):
            if i < len(cols) and not cols[i].startswith("_"):
                total_cells += 1
                if val is not None and str(val).strip():
                    non_empty_cells += 1

    fill_rate = non_empty_cells / total_cells if total_cells > 0 else 0
    print(f"    Cell fill rate: {fill_rate:.1%} ({non_empty_cells}/{total_cells})")
    check(
        "Cell fill rate > 5%",
        fill_rate > 0.05,
        f"fill_rate={fill_rate:.1%}",
    )

    email_outputs = [o for o in outputs if o["output_type"] == "email"]
    if email_outputs:
        email_data = email_outputs[0].get("data", {})
        to_addr = email_data.get("to", "")
        subject = email_data.get("subject", "")
        if to_addr:
            found_email_in_view = False
            for row in rows:
                for val in row:
                    if val and to_addr in str(val):
                        found_email_in_view = True
                        break
                if found_email_in_view:
                    break
            check(
                f"Email recipient '{to_addr}' appears in view",
                found_email_in_view,
            )
        if subject:
            print(f"    Email subject from output: {subject[:60]}")


# ---------------------------------------------------------------------------
# Test 7: RunRows remapping
# ---------------------------------------------------------------------------

def test_run_rows():
    print("\n--- Testing RunRows remapping ---")

    t0 = time.time()
    resp = api_post(
        f"/views/{VIEW_ID}/sheets/{SHEET_1}/components/{COMPONENT_1}/run",
        body={"limit": 10},
    )
    elapsed = time.time() - t0
    check("RunRows (10) succeeds", resp["success"], resp.get("error", ""))
    print(f"    RunRows took {elapsed:.1f}s, total={resp['data']['total']}")

    check("RunRows returns data", resp["data"]["total"] > 0, f"total={resp['data']['total']}")


# ---------------------------------------------------------------------------
# Test 8: Data consistency
# ---------------------------------------------------------------------------

def test_data_consistency():
    print("\n--- Testing data consistency ---")

    d1a = get_view_data(SHEET_1, COMPONENT_1)
    d1b = get_view_data(SHEET_1, COMPONENT_1)

    check("Consecutive reads return same total", d1a["total"] == d1b["total"])
    check("Consecutive reads return same columns", d1a["columns"] == d1b["columns"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global passed, failed

    print("=" * 60)
    print("Deterministic View Row Insertion — Worker E2E Test")
    print("=" * 60)
    print(f"API:       {API_BASE}")
    print(f"View:      {VIEW_ID}")
    print(f"Agent:     {AGENT_ID}")
    print(f"Workspace: {WORKSPACE_EXT_ID}")

    try:
        r = requests.get(base_url(f"/views/{VIEW_ID}"), headers=HEADERS, timeout=5)
        if r.status_code != 200:
            print(f"\n\033[31mCannot reach API or view not found (HTTP {r.status_code})\033[0m")
            sys.exit(1)
    except requests.ConnectionError:
        print(f"\n\033[31mCannot connect to {API_BASE}\033[0m")
        sys.exit(1)

    print("\n--- Snapshot: before state ---")
    before = snapshot_view_data()
    print(f"    Sheet 1: {before['sheet1_total']} rows, {len(before['sheet1_columns'])} columns")
    print(f"    Sheet 2: {before['sheet2_total']} rows, {len(before['sheet2_columns'])} columns")

    task = create_task()
    task_id = task["id"]

    task = test_task_execution(task_id)
    outputs = test_outputs_created(task_id)
    after_s1, after_s2 = test_deterministic_insertion(before)
    test_cell_quality(after_s1, outputs)
    test_run_rows()
    test_data_consistency()

    print("\n" + "=" * 60)
    total = passed + failed
    if failed == 0:
        print(f"\033[32mAll {total} checks passed.\033[0m")
    else:
        print(f"\033[31m{failed}/{total} checks failed:\033[0m")
        for e in errors:
            print(f"  - {e}")
    print("=" * 60)

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
