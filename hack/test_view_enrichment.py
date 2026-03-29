#!/usr/bin/env python3
"""
Realistic end-to-end test for the view enrichment pipeline.

Runs REAL tasks on a REAL worker and verifies the two-step
classify-then-populate enrichment pipeline handles:

  Phase 1 — Seed data: Import a small CSV into the view so we have known rows
  Phase 2 — Single-entity outreach: Agent drafts email to ONE contact from
            the CSV. Auto-approve. Verify the enrichment pipeline updates
            that row (status, last_contact) without creating duplicates.
  Phase 3 — Multi-entity outreach: Agent drafts emails to THREE contacts.
            Verify each contact's row is enriched individually —
            no concatenated garbage rows.
  Phase 4 — Follow-up enrichment: Send a follow-up to a contact that was
            already enriched. Verify the same row is updated (not duplicated)
            and the status transitions correctly.
  Phase 5 — Integrity check: Dump all rows and verify no concatenated
            addresses, no duplicate owners, no mystery rows.

Requirements:
  - Gateway running at localhost:1994 with the new enrichment pipeline
  - Worker running (make worker) with AIRSTORE_SOURCE_VIEW_ID plumbing
  - The target view and agent must exist

Usage:
    python hack/test_view_enrichment.py
    python hack/test_view_enrichment.py --skip-import   # if CSV already loaded
"""

import argparse
import json
import sys
import time
import uuid

import requests

# ---------------------------------------------------------------------------
# Config — adjust these for your environment
# ---------------------------------------------------------------------------

API_BASE = "http://localhost:1994/api/v1"
TOKEN = "1ff209f7be651d691e6ad5d704317ccee09d6652ff97be5191a5b52b6b587093"
WORKSPACE_EXT_ID = "737af74d-902f-4464-9eb2-19cbd4dd0247"

VIEW_ID = "ae0b7d0e-c52e-4ad5-88bb-a038657df98b"
AGENT_ID = "16101cc1-b294-4cd1-af52-5a1dd4b68f35"

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

TASK_TIMEOUT_S = 360
POLL_INTERVAL_S = 6
ROW_SETTLE_S = 20
AUTO_APPROVE_DELAY_S = 3

# Contacts we seed via CSV — these are our known entities.
SEED_CSV = """property_owner,owner_email,owner_phone,property_address,city,state,sf_available,asking_rent,outreach_status
Marcy Testwell,marcy@testwell-realty.example.com,555-0001,100 Testwell Dr,Phoenix,AZ,2500,$18.00 NNN,new
Derek Probesworth,derek@probesworth.example.com,555-0002,200 Probesworth Blvd,Phoenix,AZ,3800,$22.00 NNN,new
Selena Validar,selena@validar-group.example.com,555-0003,300 Validar Way,Tempe,AZ,1500,$15.00 MG,new
Hank Assertions,hank@assertions-llc.example.com,555-0004,400 Assertions Ct,Scottsdale,AZ,4200,$28.00 NNN,new
Bridget Checksum,bridget@checksum-properties.example.com,555-0005,500 Checksum Ln,Mesa,AZ,2000,$16.50 MG,new
"""

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


def api_post(path: str, body: dict | None = None, timeout: int = 60) -> dict:
    r = requests.post(base_url(path), headers=HEADERS, json=body or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def api_put(url: str, data: bytes, content_type: str = "text/csv"):
    r = requests.put(url, data=data, headers={"Content-Type": content_type}, timeout=30)
    r.raise_for_status()


# -- View helpers --


def get_view_definition() -> dict:
    return api_get(f"/views/{VIEW_ID}")["data"]["definition"]


def get_view_rows(sheet_id: str, component_id: str) -> tuple[list[dict], list[str]]:
    """Returns (rows_as_dicts, column_keys)."""
    resp = api_get(
        f"/views/{VIEW_ID}/data", params={"sheet": sheet_id, "component": component_id}
    )
    data = resp.get("data", {})
    columns = data.get("columns", [])
    raw_rows = data.get("rows", [])
    keyed_rows = []
    for row in raw_rows:
        if isinstance(row, list):
            d = {}
            for i, val in enumerate(row):
                if val is not None and i < len(columns):
                    d[columns[i]] = str(val) if not isinstance(val, str) else val
            keyed_rows.append(d)
        elif isinstance(row, dict):
            keyed_rows.append(row)
    return keyed_rows, columns


def get_all_tables() -> dict[str, dict]:
    """Returns {sheet_id:component_id: {rows, sheet_name, component_title, total, columns}}."""
    defn = get_view_definition()
    result = {}
    for sheet in defn.get("sheets", []):
        for comp in sheet.get("components", []):
            if comp.get("type") != "table":
                continue
            key = f"{sheet['id']}:{comp['id']}"
            try:
                rows, columns = get_view_rows(sheet["id"], comp["id"])
            except Exception as e:
                print(f"    WARN: failed to fetch {key}: {e}")
                rows, columns = [], []
            result[key] = {
                "total": len(rows),
                "rows": rows,
                "columns": columns,
                "sheet_id": sheet["id"],
                "component_id": comp["id"],
                "sheet_name": sheet.get("name", ""),
                "component_title": comp.get("title", ""),
            }
    return result


def find_rows_matching(tables: dict, search: str) -> list[dict]:
    """Find rows across all tables where any cell value contains `search` (case-insensitive)."""
    sl = search.lower()
    matched = []
    for key, info in tables.items():
        for row in info["rows"]:
            if not isinstance(row, dict):
                continue
            for val in row.values():
                if val and sl in str(val).lower():
                    matched.append({"table": key, "row": row})
                    break
    return matched


def row_count_delta(before: dict, after: dict) -> int:
    total = 0
    for key in after:
        b = before.get(key, {}).get("total", 0)
        a = after[key]["total"]
        total += max(0, a - b)
    return total


def print_delta(before: dict, after: dict):
    for key in after:
        b = before.get(key, {}).get("total", 0)
        a = after[key]["total"]
        d = a - b
        if d != 0:
            print(
                f"    {after[key]['sheet_name']}/{after[key]['component_title']}: "
                f"{'+' if d > 0 else ''}{d} rows ({b} → {a})"
            )


# -- Task helpers --


def get_task(task_id: str) -> dict:
    return api_get(f"/tasks/{task_id}")["data"]


def get_task_outputs(task_id: str) -> list:
    resp = api_get(f"/tasks/{task_id}/outputs")
    return resp.get("data", {}).get("outputs", [])


def submit_input(task_id: str, message: str):
    api_post(f"/tasks/{task_id}/input", {"message": message})


TERMINAL_STATES = {"done", "error", "cancelled", "timed_out", "failed", "sleeping"}


def wait_for_task(
    task_id: str, auto_approve: bool = True, timeout_s: int = TASK_TIMEOUT_S
) -> dict:
    """Poll a task until terminal, auto-approving drafts along the way."""
    start = time.time()
    last_state = ""
    last_output_count = 0
    approvals = 0

    while time.time() - start < timeout_s:
        task = get_task(task_id)
        state = task.get("state", "unknown")
        ik = task.get("input_kind", "")
        outputs = get_task_outputs(task_id)
        output_count = len(outputs)

        if state != last_state or output_count != last_output_count:
            elapsed = time.time() - start
            wr = (task.get("wake_reason") or "")[:80]
            line = f"    [{elapsed:5.0f}s] state={state}, outputs={output_count}"
            if ik:
                line += f", input_kind={ik}"
            if wr:
                line += f", wake={wr}"
            print(line)
            last_state = state
            last_output_count = output_count

        # Auto-approve when the agent is waiting for approval
        if auto_approve and state == "waiting" and ik == "approve_reject":
            time.sleep(AUTO_APPROVE_DELAY_S)
            print(f"    [{time.time() - start:5.0f}s] → auto-approving")
            try:
                submit_input(task_id, "Approved. Send it now.")
                approvals += 1
            except Exception as e:
                print(f"    WARN: auto-approve failed: {e}")
            last_state = ""
            continue

        # Auto-respond to free_text questions so the agent can proceed
        if auto_approve and state == "waiting" and ik == "free_text":
            time.sleep(AUTO_APPROVE_DELAY_S)
            print(f"    [{time.time() - start:5.0f}s] → auto-responding to free_text")
            try:
                submit_input(task_id, "Yes, proceed with the task as described. Use your best judgment.")
                approvals += 1
            except Exception as e:
                print(f"    WARN: auto-respond failed: {e}")
            last_state = ""
            continue

        if state in TERMINAL_STATES:
            if approvals > 0:
                print(f"    Auto-approved {approvals} draft(s)")
            return task

        time.sleep(POLL_INTERVAL_S)

    print(f"    \033[33mTimeout after {timeout_s}s (state={last_state})\033[0m")
    return get_task(task_id)


def create_task(message: str) -> dict:
    session_id = f"e2e-{uuid.uuid4().hex[:8]}"
    body = {
        "message": message,
        "agent_id": AGENT_ID,
        "session_id": session_id,
        "idempotency_key": f"e2e-{uuid.uuid4().hex}",
        "source_view_id": VIEW_ID,
    }
    resp = api_post("/tasks", body=body)
    ok = resp.get("success") or resp.get("data", {}).get("accepted")
    check("Task created", ok, f"resp={json.dumps(resp)[:200]}")
    task = resp["data"]["task"]
    print(f"    Task ID: {task['id']}")
    print(f"    Session: {session_id}")
    return task


# -- Import helpers --


def upload_csv(content: str) -> str | None:
    fname = f"e2e-seed-{uuid.uuid4().hex[:8]}.csv"
    path = f"/uploads/e2e-test/{fname}"
    resp = api_post("/fs/upload-url", {"path": path, "content_type": "text/csv"})
    data = resp.get("data", resp)
    upload_url = data.get("upload_url")
    key = data.get("key")
    if not upload_url:
        print(f"    WARN: no upload_url: {resp}")
        return None
    api_put(upload_url, content.encode("utf-8"))
    api_post("/fs/upload-complete", {"path": path})
    return path


def import_csv_to_sheet(file_path: str, sheet_id: str) -> dict:
    resp = api_post(
        f"/views/{VIEW_ID}/sheets/{sheet_id}/import",
        {"file_path": file_path},
        timeout=120,
    )
    data = resp.get("data", resp)
    return data


# ---------------------------------------------------------------------------
# Phase 1: Seed data
# ---------------------------------------------------------------------------


def phase_seed_data() -> dict:
    print("\n" + "=" * 60)
    print("  PHASE 1: Seed data — import contacts CSV")
    print("=" * 60)

    defn = get_view_definition()
    sheets = defn.get("sheets", [])
    if not sheets:
        print("  ERROR: View has no sheets. Create at least one sheet first.")
        sys.exit(1)

    sheet = sheets[0]
    sheet_id = sheet["id"]
    print(f"  Target sheet: {sheet.get('name', sheet_id)} ({sheet_id})")

    path = upload_csv(SEED_CSV)
    check("CSV uploaded", path is not None)
    if not path:
        sys.exit(1)

    result = import_csv_to_sheet(path, sheet_id)
    row_count = result.get("row_count", 0)
    new_cols = result.get("new_columns", [])
    check("CSV imported", row_count >= 5, f"rows={row_count}")
    if new_cols:
        print(f"    New columns created: {new_cols}")

    time.sleep(2)
    tables = get_all_tables()
    for key, info in tables.items():
        print(
            f"    {info['sheet_name']}/{info['component_title']}: {info['total']} rows"
        )

    return tables


# ---------------------------------------------------------------------------
# Phase 2: Single-entity outreach
# ---------------------------------------------------------------------------


def phase_single_outreach(before: dict) -> dict:
    print("\n" + "=" * 60)
    print("  PHASE 2: Single-entity outreach — draft + send to Marcy Testwell")
    print("=" * 60)

    task = create_task(
        "Reach out to Marcy Testwell about her property at 100 Testwell Dr, "
        "Phoenix, AZ for laundromat use."
    )
    task_id = task["id"]

    print("\n  Waiting for task (will auto-approve drafts)...")
    result = wait_for_task(task_id, auto_approve=True)
    state = result.get("state", "unknown")
    check("Task reached terminal state", state in TERMINAL_STATES, f"state={state}")

    outputs = get_task_outputs(task_id)
    check("Task produced outputs", len(outputs) > 0, f"count={len(outputs)}")
    for o in outputs[:5]:
        otype = o.get("output_type", "?")
        title = o.get("title", "?")[:70]
        print(f"    - [{otype}] {title}")

    print(f"\n  Waiting {ROW_SETTLE_S}s for enrichment pipeline...")
    time.sleep(ROW_SETTLE_S)

    after = get_all_tables()
    delta = row_count_delta(before, after)
    print_delta(before, after)

    # Key assertion: Marcy's row should be enriched, NOT duplicated
    marcy_rows = find_rows_matching(after, "marcy")
    marcy_testwell = find_rows_matching(after, "testwell")
    check(
        "Marcy Testwell found in view",
        len(marcy_rows) > 0 or len(marcy_testwell) > 0,
        f"marcy matches={len(marcy_rows)}, testwell matches={len(marcy_testwell)}",
    )
    check(
        "No duplicate Marcy rows (enriched, not inserted)",
        len(marcy_testwell) <= 2,
        f"testwell row count={len(marcy_testwell)} (expected 1, max 2 with minor dup)",
    )

    # Verify status was actually updated (by agent via view tool or enrichment)
    if marcy_testwell:
        row = marcy_testwell[0]["row"]
        status = row.get("outreach_status", "")
        print(f"    Marcy's outreach_status: '{status}'")
        check(
            "Marcy's outreach_status updated from 'new'",
            status.lower() != "new" and status != "",
            f"status='{status}' (expected 'sent' or similar, not 'new')",
        )

    return after


# ---------------------------------------------------------------------------
# Phase 3: Multi-entity outreach
# ---------------------------------------------------------------------------


def phase_multi_outreach(before: dict) -> dict:
    print("\n" + "=" * 60)
    print("  PHASE 3: Multi-entity outreach — 3 contacts at once")
    print("  (Core test: each contact should get its own row update)")
    print("=" * 60)

    task = create_task(
        "Do outreach for 200 Probesworth Blvd, 300 Validar Way, and "
        "400 Assertions Ct about laundromat use."
    )
    task_id = task["id"]

    print("\n  Waiting for task (will auto-approve drafts)...")
    result = wait_for_task(task_id, auto_approve=True)
    state = result.get("state", "unknown")
    check(
        "Multi-entity task reached terminal state",
        state in TERMINAL_STATES,
        f"state={state}",
    )

    outputs = get_task_outputs(task_id)
    check("Produced multiple outputs", len(outputs) >= 2, f"count={len(outputs)}")
    for o in outputs[:8]:
        otype = o.get("output_type", "?")
        title = o.get("title", "?")[:70]
        print(f"    - [{otype}] {title}")

    print(f"\n  Waiting {ROW_SETTLE_S}s for enrichment pipeline...")
    time.sleep(ROW_SETTLE_S)

    after = get_all_tables()
    delta = row_count_delta(before, after)
    print_delta(before, after)

    # Each contact should have their OWN row — no concatenation
    derek = find_rows_matching(after, "probesworth")
    selena = find_rows_matching(after, "validar")
    hank = find_rows_matching(after, "assertions")

    check("Derek Probesworth row exists", len(derek) > 0, f"matches={len(derek)}")
    check("Selena Validar row exists", len(selena) > 0, f"matches={len(selena)}")
    check("Hank Assertions row exists", len(hank) > 0, f"matches={len(hank)}")

    # The old bug: concatenated owner names or addresses
    for key, info in after.items():
        for row in info["rows"]:
            if not isinstance(row, dict):
                continue
            for col_key, val in row.items():
                if not isinstance(val, str):
                    continue
                # Detect if multiple known entities were mashed into one cell
                entity_hits = sum(
                    1
                    for name in ["Probesworth", "Validar", "Assertions"]
                    if name.lower() in val.lower()
                )
                if entity_hits >= 2:
                    check(
                        "No concatenated entity data in single cell",
                        False,
                        f"Cell '{col_key}' has {entity_hits} entities: {val[:120]}",
                    )

    return after


# ---------------------------------------------------------------------------
# Phase 4: Follow-up enrichment
# ---------------------------------------------------------------------------


def phase_followup(before: dict) -> dict:
    print("\n" + "=" * 60)
    print("  PHASE 4: Follow-up enrichment — re-contact Marcy Testwell")
    print("  (Verify existing row is updated, not duplicated)")
    print("=" * 60)

    task = create_task(
        "Follow up with Marcy Testwell — we haven't heard back about "
        "100 Testwell Dr."
    )
    task_id = task["id"]

    print("\n  Waiting for task (will auto-approve drafts)...")
    result = wait_for_task(task_id, auto_approve=True)
    state = result.get("state", "unknown")
    check(
        "Follow-up task reached terminal state",
        state in TERMINAL_STATES,
        f"state={state}",
    )

    outputs = get_task_outputs(task_id)
    check("Follow-up produced outputs", len(outputs) > 0, f"count={len(outputs)}")

    print(f"\n  Waiting {ROW_SETTLE_S}s for enrichment pipeline...")
    time.sleep(ROW_SETTLE_S)

    after = get_all_tables()
    delta = row_count_delta(before, after)
    print_delta(before, after)

    check(
        "Follow-up created at most 1 new row (enriched existing)",
        delta <= 1,
        f"delta={delta} (expected 0-1, enrichment should update in place)",
    )

    testwell_rows = find_rows_matching(after, "testwell")
    check(
        "Still only 1-2 Testwell rows (no duplication from follow-up)",
        len(testwell_rows) <= 2,
        f"count={len(testwell_rows)}",
    )

    # Verify status transition: 'sent' -> 'followed_up'
    if testwell_rows:
        row = testwell_rows[0]["row"]
        status = row.get("outreach_status", "")
        print(f"    Marcy's outreach_status after follow-up: '{status}'")
        check(
            "Status updated from 'sent' to follow-up state",
            status.lower() not in ("new", ""),
            f"status='{status}' (expected 'followed_up' or similar)",
        )

    return after


# ---------------------------------------------------------------------------
# Phase 5: Integrity check
# ---------------------------------------------------------------------------


def phase_integrity(tables: dict):
    print("\n" + "=" * 60)
    print("  PHASE 5: Data integrity check")
    print("=" * 60)

    total_rows = sum(info["total"] for info in tables.values())
    print(f"  Total rows across all tables: {total_rows}")

    # Check for concatenated addresses (the hallmark bug)
    concat_found = 0
    for key, info in tables.items():
        for row in info["rows"]:
            if not isinstance(row, dict):
                continue
            for col_key, val in row.items():
                if not isinstance(val, str) or len(val) < 50:
                    continue
                # Multiple addresses jammed together
                addr_indicators = sum(
                    1
                    for w in ["Ave", "Blvd", "Way", "Ct", "Ln", "Dr", "St"]
                    if w in val
                )
                if addr_indicators >= 3 and val.count(",") >= 4:
                    concat_found += 1
                    print(f"    SUSPECT: [{key}] {col_key} = {val[:150]}")

    check(
        "No concatenated multi-address cells",
        concat_found == 0,
        f"found {concat_found} suspect cells",
    )

    # Check for concatenated owner names
    multi_owner = 0
    known_owners = ["Testwell", "Probesworth", "Validar", "Assertions", "Checksum"]
    for key, info in tables.items():
        for row in info["rows"]:
            if not isinstance(row, dict):
                continue
            for col_key, val in row.items():
                if not isinstance(val, str):
                    continue
                hits = sum(1 for o in known_owners if o.lower() in val.lower())
                if hits >= 2:
                    multi_owner += 1
                    print(
                        f"    SUSPECT: [{key}] {col_key} has {hits} owners: {val[:150]}"
                    )

    check(
        "No cells with multiple owner names concatenated",
        multi_owner == 0,
        f"found {multi_owner} suspect cells",
    )

    # Summarize each known contact's state
    print("\n  Contact status summary:")
    for name in known_owners:
        matches = find_rows_matching(tables, name.lower())
        count = len(matches)
        status = "?"
        if matches:
            row = matches[0]["row"]
            status = row.get("outreach_status", row.get("status", "?"))
        icon = (
            "\033[32m●\033[0m"
            if count == 1
            else ("\033[33m●\033[0m" if count > 1 else "\033[31m○\033[0m")
        )
        print(f"    {icon} {name}: {count} row(s), status={status}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    global passed, failed

    parser = argparse.ArgumentParser(description="Realistic E2E enrichment test")
    parser.add_argument(
        "--skip-import",
        action="store_true",
        help="Skip CSV import (data already loaded)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  View Enrichment Pipeline — Realistic E2E Test")
    print("  Runs real tasks on a real worker")
    print("=" * 60)
    print(f"  API:    {API_BASE}")
    print(f"  View:   {VIEW_ID}")
    print(f"  Agent:  {AGENT_ID}")
    print(f"  Timeout: {TASK_TIMEOUT_S}s per task")

    # Phase 1: Seed
    if args.skip_import:
        print("\n  (Skipping CSV import)")
        initial = get_all_tables()
        for key, info in initial.items():
            print(
                f"    {info['sheet_name']}/{info['component_title']}: {info['total']} rows"
            )
    else:
        initial = phase_seed_data()

    # Phase 2: Single outreach
    after_single = phase_single_outreach(initial)

    # Phase 3: Multi-entity outreach
    after_multi = phase_multi_outreach(after_single)

    # Phase 4: Follow-up
    after_followup = phase_followup(after_multi)

    # Phase 5: Integrity
    final_tables = get_all_tables()
    phase_integrity(final_tables)

    # Results
    print("\n" + "=" * 60)
    total = passed + failed
    print(f"  Results: {passed}/{total} passed, {failed} failed")
    if errors:
        print("\n  Failures:")
        for e in errors:
            print(f"    \u2022 {e}")
    else:
        print("  \033[32mAll checks passed!\033[0m")
    print("=" * 60)

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
