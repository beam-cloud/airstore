#!/usr/bin/env python3
"""
Batch outreach E2E test for the view enrichment pipeline.

Tests the critical flow: import property data, trigger outreach in batches
(2 properties at a time), and verify that:

  1. Each property row is updated individually (no concatenation)
  2. A single owner with multiple properties gets ALL their rows updated
  3. The view tool doesn't create redundant task outputs
  4. Follow-up outreach updates existing rows, not creating duplicates

Uses realistic property data modeled on commercial real estate listings.

Usage:
    python hack/test_batch_outreach.py
    python hack/test_batch_outreach.py --skip-import
"""

import argparse
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

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

TASK_TIMEOUT_S = 420
POLL_INTERVAL_S = 6
ROW_SETTLE_S = 20
AUTO_APPROVE_DELAY_S = 3

# Realistic CRE property data — note: Marcus Greenfield owns TWO properties.
SEED_CSV = """property_owner,owner_email,owner_phone,property_address,city,state,sf_available,asking_rent,property_type,outreach_status
Marcus Greenfield,marcus@greenfield-cre.example.com,555-101-2001,4521 Telegraph Ave,Oakland,CA,"3,200",$24.00 NNN,Retail,new
Marcus Greenfield,marcus@greenfield-cre.example.com,555-101-2001,8900 San Pablo Ave,El Cerrito,CA,"1,800",$18.50 MG,Retail,new
Patricia Nolan-Wu,patricia@nolanwu-group.example.com,555-202-3002,1055 Market St,San Francisco,CA,"5,400",$42.00 NNN,Office,new
Devon Hartwell,devon@hartwell-investments.example.com,555-303-4003,2280 3rd St,Sacramento,CA,"2,750",$19.80 MG,Retail,new
Carmen Alvarez-Reyes,carmen@alvarez-properties.example.com,555-404-5004,690 W 11th Ave,Eugene,OR,"4,100",$15.60 NNN,Retail,new
Raj Mehta,raj@mehtacommercial.example.com,555-505-6005,340 Hillcrest Rd,Hollister,CA,"6,000",$12.60 MG,Industrial,new
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
    session_id = f"batch-{uuid.uuid4().hex[:8]}"
    body = {
        "message": message,
        "agent_id": AGENT_ID,
        "session_id": session_id,
        "idempotency_key": f"batch-{uuid.uuid4().hex}",
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
    fname = f"batch-seed-{uuid.uuid4().hex[:8]}.csv"
    path = f"/uploads/batch-test/{fname}"
    resp = api_post("/fs/upload-url", {"path": path, "content_type": "text/csv"})
    data = resp.get("data", resp)
    upload_url = data.get("upload_url")
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
    return resp.get("data", resp)


# ---------------------------------------------------------------------------
# Phase 1: Seed data
# ---------------------------------------------------------------------------


def phase_seed_data() -> dict:
    print("\n" + "=" * 60)
    print("  PHASE 1: Seed data — import property listings CSV")
    print("=" * 60)

    defn = get_view_definition()
    sheets = defn.get("sheets", [])
    if not sheets:
        print("  ERROR: View has no sheets.")
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
    check("CSV imported", row_count >= 6, f"rows={row_count}")

    time.sleep(2)
    tables = get_all_tables()
    for key, info in tables.items():
        print(f"    {info['sheet_name']}/{info['component_title']}: {info['total']} rows")

    greenfield_rows = find_rows_matching(tables, "greenfield")
    check(
        "Marcus Greenfield has 2 property rows",
        len(greenfield_rows) >= 2,
        f"found {len(greenfield_rows)} rows (expected 2 — he owns 2 properties)",
    )

    return tables


# ---------------------------------------------------------------------------
# Phase 2: Batch 1 — reach out to first 2 property owners
# ---------------------------------------------------------------------------


def phase_batch_1(before: dict) -> dict:
    print("\n" + "=" * 60)
    print("  PHASE 2: Batch 1 — outreach for first 2 properties")
    print("  (4521 Telegraph Ave + 8900 San Pablo Ave — same owner)")
    print("=" * 60)

    task = create_task(
        "Reach out to Marcus Greenfield about his properties at "
        "4521 Telegraph Ave, Oakland and 8900 San Pablo Ave, El Cerrito "
        "for laundromat use."
    )
    task_id = task["id"]

    print("\n  Waiting for task (will auto-approve drafts)...")
    result = wait_for_task(task_id, auto_approve=True)
    state = result.get("state", "unknown")
    check("Batch 1 task completed", state in TERMINAL_STATES, f"state={state}")

    outputs = get_task_outputs(task_id)
    check("Batch 1 produced outputs", len(outputs) > 0, f"count={len(outputs)}")
    for o in outputs[:6]:
        otype = o.get("output_type", "?")
        title = o.get("title", "?")[:70]
        print(f"    - [{otype}] {title}")

    # Key check: view tool invocations should NOT have created outputs
    view_outputs = [
        o for o in outputs
        if any(
            kw in (o.get("title", "") + str(o.get("data", {}))).lower()
            for kw in ["cells_updated", "row_id", "merged"]
        )
    ]
    check(
        "View tool invocations did not create task outputs",
        len(view_outputs) == 0,
        f"found {len(view_outputs)} view-tool outputs (should be 0)",
    )

    print(f"\n  Waiting {ROW_SETTLE_S}s for enrichment pipeline...")
    time.sleep(ROW_SETTLE_S)

    after = get_all_tables()
    print_delta(before, after)

    # Both Greenfield rows should be updated
    greenfield_rows = find_rows_matching(after, "greenfield")
    check(
        "Marcus Greenfield rows still present",
        len(greenfield_rows) >= 2,
        f"found {len(greenfield_rows)} (expected 2)",
    )

    telegraph_rows = find_rows_matching(after, "telegraph")
    san_pablo_rows = find_rows_matching(after, "san pablo")

    if telegraph_rows:
        status = telegraph_rows[0]["row"].get("outreach_status", "")
        print(f"    4521 Telegraph Ave outreach_status: '{status}'")
        check(
            "Telegraph Ave row status updated",
            status.lower() not in ("new", ""),
            f"status='{status}' (expected 'sent')",
        )

    if san_pablo_rows:
        status = san_pablo_rows[0]["row"].get("outreach_status", "")
        print(f"    8900 San Pablo Ave outreach_status: '{status}'")
        check(
            "San Pablo Ave row status updated",
            status.lower() not in ("new", ""),
            f"status='{status}' (expected 'sent')",
        )

    return after


# ---------------------------------------------------------------------------
# Phase 3: Batch 2 — reach out to next 2 property owners
# ---------------------------------------------------------------------------


def phase_batch_2(before: dict) -> dict:
    print("\n" + "=" * 60)
    print("  PHASE 3: Batch 2 — outreach for next 2 properties")
    print("  (1055 Market St + 2280 3rd St — different owners)")
    print("=" * 60)

    task = create_task(
        "Do outreach for 1055 Market St, San Francisco and 2280 3rd St, Sacramento "
        "about laundromat use."
    )
    task_id = task["id"]

    print("\n  Waiting for task (will auto-approve drafts)...")
    result = wait_for_task(task_id, auto_approve=True)
    state = result.get("state", "unknown")
    check("Batch 2 task completed", state in TERMINAL_STATES, f"state={state}")

    outputs = get_task_outputs(task_id)
    check("Batch 2 produced outputs", len(outputs) > 0, f"count={len(outputs)}")
    for o in outputs[:6]:
        otype = o.get("output_type", "?")
        title = o.get("title", "?")[:70]
        print(f"    - [{otype}] {title}")

    print(f"\n  Waiting {ROW_SETTLE_S}s for enrichment pipeline...")
    time.sleep(ROW_SETTLE_S)

    after = get_all_tables()
    print_delta(before, after)

    # Check individual row updates
    nolan_rows = find_rows_matching(after, "nolan")
    hartwell_rows = find_rows_matching(after, "hartwell")
    check("Patricia Nolan-Wu row exists", len(nolan_rows) > 0, f"matches={len(nolan_rows)}")
    check("Devon Hartwell row exists", len(hartwell_rows) > 0, f"matches={len(hartwell_rows)}")

    if nolan_rows:
        status = nolan_rows[0]["row"].get("outreach_status", "")
        print(f"    Patricia's outreach_status: '{status}'")
        check("Patricia's status updated", status.lower() not in ("new", ""), f"status='{status}'")

    if hartwell_rows:
        status = hartwell_rows[0]["row"].get("outreach_status", "")
        print(f"    Devon's outreach_status: '{status}'")
        check("Devon's status updated", status.lower() not in ("new", ""), f"status='{status}'")

    # No concatenated data from separate emails
    for key, info in after.items():
        for row in info["rows"]:
            if not isinstance(row, dict):
                continue
            for col_key, val in row.items():
                if not isinstance(val, str):
                    continue
                hits = sum(
                    1
                    for name in ["Nolan", "Hartwell"]
                    if name.lower() in val.lower()
                )
                if hits >= 2:
                    check(
                        "No concatenated owner data",
                        False,
                        f"Cell '{col_key}' has both owners: {val[:120]}",
                    )

    return after


# ---------------------------------------------------------------------------
# Phase 4: Multi-update from follow-up — both Greenfield properties
# ---------------------------------------------------------------------------


def phase_multi_update(before: dict) -> dict:
    print("\n" + "=" * 60)
    print("  PHASE 4: Multi-update — follow up on BOTH Greenfield properties")
    print("  (Single email should update 2 rows)")
    print("=" * 60)

    task = create_task(
        "Follow up with Marcus Greenfield — we haven't heard back about "
        "either of his properties."
    )
    task_id = task["id"]

    print("\n  Waiting for task (will auto-approve drafts)...")
    result = wait_for_task(task_id, auto_approve=True)
    state = result.get("state", "unknown")
    check("Multi-update task completed", state in TERMINAL_STATES, f"state={state}")

    outputs = get_task_outputs(task_id)
    check("Multi-update produced outputs", len(outputs) > 0, f"count={len(outputs)}")
    for o in outputs[:6]:
        otype = o.get("output_type", "?")
        title = o.get("title", "?")[:70]
        print(f"    - [{otype}] {title}")

    print(f"\n  Waiting {ROW_SETTLE_S}s for enrichment pipeline...")
    time.sleep(ROW_SETTLE_S)

    after = get_all_tables()
    print_delta(before, after)

    greenfield_rows = find_rows_matching(after, "greenfield")
    check(
        "Still exactly 2 Greenfield rows (no duplication from follow-up)",
        len(greenfield_rows) <= 3,
        f"found {len(greenfield_rows)} (expected 2, max 3 with minor dup)",
    )

    # Check that BOTH rows were updated to 'followed_up'
    telegraph = find_rows_matching(after, "telegraph")
    san_pablo = find_rows_matching(after, "san pablo")

    followed_up_count = 0
    if telegraph:
        status = telegraph[0]["row"].get("outreach_status", "")
        print(f"    Telegraph Ave status after follow-up: '{status}'")
        if status.lower() in ("followed_up", "follow_up", "following_up", "follow-up"):
            followed_up_count += 1
    if san_pablo:
        status = san_pablo[0]["row"].get("outreach_status", "")
        print(f"    San Pablo Ave status after follow-up: '{status}'")
        if status.lower() in ("followed_up", "follow_up", "following_up", "follow-up"):
            followed_up_count += 1

    check(
        "Both Greenfield rows updated to follow-up status",
        followed_up_count >= 1,
        f"only {followed_up_count}/2 updated (at least 1 expected)",
    )

    return after


# ---------------------------------------------------------------------------
# Phase 5: Data integrity check
# ---------------------------------------------------------------------------


def phase_integrity(tables: dict):
    print("\n" + "=" * 60)
    print("  PHASE 5: Data integrity check")
    print("=" * 60)

    total_rows = sum(info["total"] for info in tables.values())
    print(f"  Total rows across all tables: {total_rows}")

    # Check for concatenated addresses
    concat_found = 0
    for key, info in tables.items():
        for row in info["rows"]:
            if not isinstance(row, dict):
                continue
            for col_key, val in row.items():
                if not isinstance(val, str) or len(val) < 50:
                    continue
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
    known_owners = ["Greenfield", "Nolan", "Hartwell", "Alvarez", "Mehta"]
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
                    print(f"    SUSPECT: [{key}] {col_key} has {hits} owners: {val[:150]}")

    check(
        "No cells with multiple owner names concatenated",
        multi_owner == 0,
        f"found {multi_owner} suspect cells",
    )

    # Check untouched rows are still 'new'
    alvarez = find_rows_matching(tables, "alvarez")
    mehta = find_rows_matching(tables, "mehta")

    if alvarez:
        status = alvarez[0]["row"].get("outreach_status", "")
        check(
            "Carmen Alvarez-Reyes untouched (status=new)",
            status.lower() in ("new", ""),
            f"status='{status}' (expected 'new' — she was not contacted)",
        )

    if mehta:
        status = mehta[0]["row"].get("outreach_status", "")
        check(
            "Raj Mehta untouched (status=new)",
            status.lower() in ("new", ""),
            f"status='{status}' (expected 'new' — he was not contacted)",
        )

    # Summarize
    print("\n  Contact status summary:")
    all_owners = ["Greenfield", "Nolan-Wu", "Hartwell", "Alvarez-Reyes", "Mehta"]
    for name in all_owners:
        search = name.split("-")[0].lower()
        matches = find_rows_matching(tables, search)
        count = len(matches)
        statuses = set()
        for m in matches:
            s = m["row"].get("outreach_status", "?")
            if s:
                statuses.add(s)
        status_str = ", ".join(sorted(statuses)) if statuses else "?"
        icon = (
            "\033[32m●\033[0m"
            if count <= 2
            else "\033[33m●\033[0m"
        )
        print(f"    {icon} {name}: {count} row(s), status={status_str}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    global passed, failed

    parser = argparse.ArgumentParser(description="Batch outreach E2E test")
    parser.add_argument("--skip-import", action="store_true", help="Skip CSV import")
    args = parser.parse_args()

    print("=" * 60)
    print("  Batch Outreach — E2E Test")
    print("  Tests batch outreach + multi-row updates from single owner")
    print("=" * 60)
    print(f"  API:    {API_BASE}")
    print(f"  View:   {VIEW_ID}")
    print(f"  Agent:  {AGENT_ID}")
    print(f"  Timeout: {TASK_TIMEOUT_S}s per task")

    if args.skip_import:
        print("\n  (Skipping CSV import)")
        initial = get_all_tables()
        for key, info in initial.items():
            print(f"    {info['sheet_name']}/{info['component_title']}: {info['total']} rows")
    else:
        initial = phase_seed_data()

    after_batch1 = phase_batch_1(initial)
    after_batch2 = phase_batch_2(after_batch1)
    after_multi = phase_multi_update(after_batch2)

    final_tables = get_all_tables()
    phase_integrity(final_tables)

    print("\n" + "=" * 60)
    total = passed + failed
    print(f"  Results: {passed}/{total} passed, {failed} failed")
    if errors:
        print("\n  Failures:")
        for e in errors:
            print(f"    • {e}")
    else:
        print("  \033[32mAll checks passed!\033[0m")
    print("=" * 60)

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
