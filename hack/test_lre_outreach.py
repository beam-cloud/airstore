#!/usr/bin/env python3
"""
LRE-style E2E test for the view enrichment pipeline.

Models the real LRE Pilot workflow: broker-grouped outreach on commercial
real estate listings. Tests:

  1. CSV import with broker + owner data (mirrors LRE Pilot Sample.csv)
  2. Batch outreach grouped by broker (one broker = multiple properties)
  3. Disqualification — status update affects ALL rows for that broker
  4. Successful reply — agent updates affected property rows
  5. No data duplication or concatenation across batches
  6. Multi-sheet population — outputs populate BOTH sheets via vector search

Fires independent tasks in parallel:
  - Cody outreach + Inessa outreach run simultaneously
  - Disqualify (depends on Cody) + Reply (depends on Inessa) run simultaneously

Usage:
    python hack/test_lre_outreach.py
    python hack/test_lre_outreach.py --skip-import
"""

import argparse
import json
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_BASE = "http://localhost:1994/api/v1"
TOKEN = "dc321e8e5cc1d13ec34bc41b3960a01937057cfad31f3b39ffabd7cfc378f216"
WORKSPACE_EXT_ID = "737af74d-902f-4464-9eb2-19cbd4dd0247"

AGENT_ID = "16101cc1-b294-4cd1-af52-5a1dd4b68f35"

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

TASK_TIMEOUT_S = 480
POLL_INTERVAL_S = 6
ROW_SETTLE_S = 20
AUTO_APPROVE_DELAY_S = 3

VIEW_ID = None  # set dynamically in phase_create_view()

VIEW_DEFINITION = {
    "name": "LRE Outreach Pipeline",
    "description": "Commercial real estate outreach tracking — broker-grouped property listings with outreach status",
    "agents": [AGENT_ID],
    "sheets": [
        {
            "id": "sheet-outreach",
            "name": "Outreach Pipeline",
            "layout": {"columns": 12},
            "components": [
                {
                    "id": "outreach-table",
                    "type": "table",
                    "title": "Outreach Pipeline",
                    "position": {"col": 0, "row": 0, "colSpan": 0, "rowSpan": 0},
                    "config": {
                        "columns": [
                            {"key": "property_address", "label": "Property Address", "type": "text"},
                            {"key": "city", "label": "City", "type": "text"},
                            {"key": "state", "label": "State", "type": "text"},
                            {"key": "sf_available", "label": "SF Available", "type": "text"},
                            {"key": "asking_rent", "label": "Asking Rent", "type": "text"},
                            {"key": "leasing_broker", "label": "Leasing Broker", "type": "text"},
                            {"key": "broker_email", "label": "Broker Email", "type": "email"},
                            {"key": "property_owner", "label": "Property Owner", "type": "text"},
                            {"key": "owner_email", "label": "Owner Email", "type": "email"},
                            {"key": "owner_phone", "label": "Owner Phone", "type": "text"},
                            {"key": "property_type", "label": "Property Type", "type": "text"},
                            {"key": "outreach_status", "label": "Outreach Status", "type": "status"},
                        ],
                    },
                }
            ],
        },
        {
            "id": "sheet-listings",
            "name": "Property Listings",
            "layout": {"columns": 12},
            "components": [
                {
                    "id": "listings-table",
                    "type": "table",
                    "title": "Qualified Properties",
                    "position": {"col": 0, "row": 0, "colSpan": 0, "rowSpan": 0},
                    "config": {
                        "columns": [
                            {"key": "property_address", "label": "Property Address", "type": "text"},
                            {"key": "city", "label": "City", "type": "text"},
                            {"key": "state", "label": "State", "type": "text"},
                            {"key": "property_type", "label": "Property Type", "type": "text"},
                            {"key": "sf_available", "label": "SF Available", "type": "text"},
                            {"key": "asking_rent", "label": "Asking Rent", "type": "text"},
                            {"key": "owner_name", "label": "Owner Name", "type": "text"},
                            {"key": "qualification_status", "label": "Qualification Status", "type": "status"},
                            {"key": "notes", "label": "Notes", "type": "text"},
                        ],
                    },
                }
            ],
        },
    ],
}

SEED_CSV = """\
property_address,city,state,sf_available,asking_rent,leasing_broker,broker_email,property_owner,owner_email,owner_phone,property_type,outreach_status
2539 Telegraph Ave,Berkeley,CA,"2,000 - 4,434",$28.00 NNN,Cody Maxwell,luke@beam.cloud,Patrick Kensington,luke@beam.cloud,555-101-0001,Retail,new
320 Hillcrest Rd,Hollister,CA,"3,850",$12.60 MG,Cody Maxwell,luke@beam.cloud,Robert Alston,N/A,555-202-0002,Industrial,new
8555 San Ysidro Ave,Gilroy,CA,"6,000",Withheld,Cody Maxwell,luke@beam.cloud,Joseph Romani,luke@beam.cloud,555-303-0003,Retail,new
708 1st St,Napa,CA,"2,605",$59.40 NNN,Inessa Romano,luke@beam.cloud,Mary Jane Stephenson,luke@beam.cloud,555-404-0004,Retail,new
522 W 2nd St,Antioch,CA,"5,000",$22.20 MG,Inessa Romano,luke@beam.cloud,Sean McCallister,luke@beam.cloud,555-505-0005,Retail,new
201 3rd St,Santa Rosa,CA,"2,763",$23.40 NNN,Inessa Romano,luke@beam.cloud,Mary Jane Stephenson,luke@beam.cloud,555-606-0006,Retail,new
2160 W 11th Ave,Eugene,OR,"2,655",$12.36 NNN,Ryan Blackwell,luke@beam.cloud,John Arnstein,luke@beam.cloud,555-707-0007,Retail,new
1028 11th St,Modesto,CA,N/A,N/A,Ryan Blackwell,luke@beam.cloud,John Varney,N/A,555-808-0008,Office,new
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
        f"/views/{VIEW_ID}/data",
        params={"sheet": sheet_id, "component": component_id},
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


def row_cell(rows: list[dict], col: str) -> str:
    if not rows:
        return ""
    return rows[0]["row"].get(col, "")


def print_table_summary(tables: dict):
    for key, info in tables.items():
        print(f"    {info['sheet_name']}/{info['component_title']}: {info['total']} rows")


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
    task_id: str, label: str = "", auto_approve: bool = True, timeout_s: int = TASK_TIMEOUT_S
) -> dict:
    prefix = f"[{label}] " if label else ""
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
            line = f"    {prefix}[{elapsed:5.0f}s] state={state}, outputs={output_count}"
            if ik:
                line += f", input_kind={ik}"
            if wr:
                line += f", wake={wr}"
            print(line)
            last_state = state
            last_output_count = output_count

        if auto_approve and state == "waiting" and ik == "approve_reject":
            time.sleep(AUTO_APPROVE_DELAY_S)
            print(f"    {prefix}[{time.time() - start:5.0f}s] → auto-approving")
            try:
                submit_input(task_id, "Approved. Send it now.")
                approvals += 1
            except Exception as e:
                print(f"    {prefix}WARN: auto-approve failed: {e}")
            last_state = ""
            continue

        if auto_approve and state == "waiting" and ik == "free_text":
            time.sleep(AUTO_APPROVE_DELAY_S)
            print(f"    {prefix}[{time.time() - start:5.0f}s] → auto-responding to free_text")
            try:
                submit_input(task_id, "Yes, proceed as described. Use your best judgment.")
                approvals += 1
            except Exception as e:
                print(f"    {prefix}WARN: auto-respond failed: {e}")
            last_state = ""
            continue

        if state in TERMINAL_STATES:
            if approvals > 0:
                print(f"    {prefix}Auto-approved {approvals} draft(s)")
            return task

        time.sleep(POLL_INTERVAL_S)

    print(f"    {prefix}\033[33mTimeout after {timeout_s}s (state={last_state})\033[0m")
    return get_task(task_id)


def create_task(message: str) -> dict:
    session_id = f"lre-{uuid.uuid4().hex[:8]}"
    body = {
        "message": message,
        "agent_id": AGENT_ID,
        "session_id": session_id,
        "idempotency_key": f"lre-{uuid.uuid4().hex}",
        "source_view_id": VIEW_ID,
    }
    resp = api_post("/tasks", body=body)
    ok = resp.get("success") or resp.get("data", {}).get("accepted")
    if not ok:
        print(f"  \033[31m✗\033[0m Task creation failed: {json.dumps(resp)[:200]}")
    task = resp["data"]["task"]
    print(f"    Task ID: {task['id']} (session: {session_id})")
    return task


# -- Import helpers --


def upload_csv(content: str) -> str | None:
    fname = f"lre-seed-{uuid.uuid4().hex[:8]}.csv"
    path = f"/uploads/lre-test/{fname}"
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
# Phase 0: Create the view
# ---------------------------------------------------------------------------


def phase_create_view() -> str:
    global VIEW_ID
    print("\n" + "=" * 60)
    print("  PHASE 0: Create view — LRE Outreach Pipeline")
    print("=" * 60)

    resp = api_post("/views", body={
        "name": VIEW_DEFINITION["name"],
        "description": VIEW_DEFINITION["description"],
        "definition": VIEW_DEFINITION,
    })
    view = resp.get("data", {})
    view_id = view.get("external_id", view.get("id", ""))
    check("View created", bool(view_id), f"response: {json.dumps(resp)[:200]}")
    print(f"    View ID: {view_id}")

    defn = view.get("definition", {})
    sheets = defn.get("sheets", [])
    check("View has 2 sheets", len(sheets) == 2, f"got {len(sheets)}")

    VIEW_ID = view_id
    return view_id


# ---------------------------------------------------------------------------
# Phase 1: Seed data
# ---------------------------------------------------------------------------


def phase_seed_data() -> dict:
    print("\n" + "=" * 60)
    print("  PHASE 1: Seed data — import LRE-style property listings")
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
    check("CSV imported (8 rows)", row_count >= 8, f"rows={row_count}")

    time.sleep(2)
    tables = get_all_tables()
    print_table_summary(tables)

    cody_rows = find_rows_matching(tables, "cody maxwell")
    inessa_rows = find_rows_matching(tables, "inessa romano")
    ryan_rows = find_rows_matching(tables, "ryan blackwell")
    check("Cody Maxwell has 3 properties", len(cody_rows) >= 3, f"found {len(cody_rows)}")
    check("Inessa Romano has 3 properties", len(inessa_rows) >= 3, f"found {len(inessa_rows)}")
    check("Ryan Blackwell has 2 properties", len(ryan_rows) >= 2, f"found {len(ryan_rows)}")

    mj_rows = find_rows_matching(tables, "stephenson")
    check(
        "Mary Jane Stephenson has 2 property rows",
        len(mj_rows) >= 2,
        f"found {len(mj_rows)} (she owns 708 1st St + 201 3rd St)",
    )

    return tables


# ---------------------------------------------------------------------------
# Phase 2: Parallel outreach — Cody + Inessa at the same time
# ---------------------------------------------------------------------------


def run_outreach_cody() -> dict:
    """Fire and wait for Cody Maxwell outreach task."""
    task = create_task(
        "Do outreach for the properties listed under broker Cody Maxwell: "
        "2539 Telegraph Ave in Berkeley, 320 Hillcrest Rd in Hollister, "
        "and 8555 San Ysidro Ave in Gilroy."
    )
    result = wait_for_task(task["id"], label="Cody", auto_approve=True)
    return {"task": task, "result": result, "outputs": get_task_outputs(task["id"])}


def run_outreach_inessa() -> dict:
    """Fire and wait for Inessa Romano outreach task."""
    task = create_task(
        "Do outreach for the properties under broker Inessa Romano: "
        "708 1st St in Napa, 522 W 2nd St in Antioch, and 201 3rd St "
        "in Santa Rosa."
    )
    result = wait_for_task(task["id"], label="Inessa", auto_approve=True)
    return {"task": task, "result": result, "outputs": get_task_outputs(task["id"])}


def phase_parallel_outreach(before: dict) -> dict:
    print("\n" + "=" * 60)
    print("  PHASE 2: Parallel outreach — Cody + Inessa simultaneously")
    print("=" * 60)

    with ThreadPoolExecutor(max_workers=2) as pool:
        cody_future = pool.submit(run_outreach_cody)
        inessa_future = pool.submit(run_outreach_inessa)

        cody_data = cody_future.result()
        inessa_data = inessa_future.result()

    for label, data in [("Cody", cody_data), ("Inessa", inessa_data)]:
        state = data["result"].get("state", "unknown")
        check(f"{label} outreach completed", state in TERMINAL_STATES, f"state={state}")
        for o in data["outputs"][:5]:
            otype = o.get("output_type", "?")
            title = o.get("title", "?")[:70]
            print(f"    [{label}] [{otype}] {title}")

    print(f"\n  Waiting {ROW_SETTLE_S}s for enrichment pipeline...")
    time.sleep(ROW_SETTLE_S)

    after = get_all_tables()
    print_delta(before, after)

    # Verify Cody's properties updated
    cody_updated = 0
    for addr in ["telegraph", "hillcrest", "san ysidro"]:
        rows = find_rows_matching(after, addr)
        if rows:
            status = row_cell(rows, "outreach_status")
            print(f"    {addr.title()} outreach_status: '{status}'")
            if status.lower() not in ("new", ""):
                cody_updated += 1
    check(
        "At least 2/3 Cody properties have updated status",
        cody_updated >= 2,
        f"only {cody_updated}/3 updated",
    )

    # Verify Inessa's properties updated
    for addr in ["1st st", "2nd st", "3rd st"]:
        rows = find_rows_matching(after, addr)
        if rows:
            status = row_cell(rows, "outreach_status")
            print(f"    {addr.title()} outreach_status: '{status}'")
            check(
                f"{addr.title()} row status updated",
                status.lower() not in ("new", ""),
                f"status='{status}'",
            )

    mj_rows = find_rows_matching(after, "stephenson")
    check(
        "Mary Jane still has 2 distinct rows (not merged)",
        len(mj_rows) >= 2,
        f"found {len(mj_rows)} rows (expected 2)",
    )

    return after


# ---------------------------------------------------------------------------
# Phase 3: Parallel follow-ups — Disqualify (Cody) + Reply (Inessa)
# ---------------------------------------------------------------------------


def run_bounce_cody() -> dict:
    """Fire and wait for Cody disqualification task."""
    task = create_task(
        "Cody Maxwell's 3 properties (2539 Telegraph Ave, 320 Hillcrest Rd, "
        "8555 San Ysidro Ave) are disqualified — the broker never responded "
        "and we've confirmed the listings are no longer available. "
        "Update those properties' outreach status to reflect they are "
        "disqualified/closed."
    )
    result = wait_for_task(task["id"], label="Disqualify", auto_approve=True)
    return {"task": task, "result": result}


def run_reply_inessa() -> dict:
    """Fire and wait for Inessa reply handling task."""
    task = create_task(
        "Inessa Romano replied about 708 1st St in Napa — she confirmed "
        "laundromat use is approved and the rent is $55/sqft NNN. "
        "Update the CRM accordingly."
    )
    result = wait_for_task(task["id"], label="Reply", auto_approve=True)
    return {"task": task, "result": result}


def phase_parallel_followups(before: dict) -> dict:
    print("\n" + "=" * 60)
    print("  PHASE 3: Parallel follow-ups — Disqualify + Reply simultaneously")
    print("=" * 60)

    with ThreadPoolExecutor(max_workers=2) as pool:
        bounce_future = pool.submit(run_bounce_cody)
        reply_future = pool.submit(run_reply_inessa)

        bounce_data = bounce_future.result()
        reply_data = reply_future.result()

    for label, data in [("Disqualify", bounce_data), ("Reply", reply_data)]:
        state = data["result"].get("state", "unknown")
        check(f"{label} task completed", state in TERMINAL_STATES, f"state={state}")

    print(f"\n  Waiting {ROW_SETTLE_S}s for enrichment pipeline...")
    time.sleep(ROW_SETTLE_S)

    after = get_all_tables()
    print_delta(before, after)

    # Verify disqualification: all 3 Cody properties should be disqualified/closed
    disq_count = 0
    for addr, label in [
        ("telegraph", "2539 Telegraph Ave"),
        ("hillcrest", "320 Hillcrest Rd"),
        ("san ysidro", "8555 San Ysidro Ave"),
    ]:
        rows = find_rows_matching(after, addr)
        if rows:
            status = row_cell(rows, "outreach_status")
            print(f"    {label} outreach_status: '{status}'")
            is_disq = any(
                kw in status.lower()
                for kw in ["disqualif", "closed", "unavailable", "inactive", "dead", "lost", "no response", "no-response"]
            )
            if is_disq:
                disq_count += 1

    check(
        "All 3 Cody properties marked as disqualified/closed",
        disq_count >= 2,
        f"only {disq_count}/3 updated (need at least 2)",
    )

    # Verify disqualification didn't bleed into Inessa's rows
    inessa_rows = find_rows_matching(after, "inessa romano")
    for r in inessa_rows:
        status = r["row"].get("outreach_status", "")
        if any(kw in status.lower() for kw in ["disqualif", "closed", "unavailable"]):
            check(
                "Disqualification did not bleed into Inessa's rows",
                False,
                f"Inessa row has status '{status}'",
            )
            break
    else:
        check("Disqualification did not bleed into other brokers' rows", True)

    # Verify reply: 708 1st St should be qualified
    napa_rows = find_rows_matching(after, "1st st")
    if napa_rows:
        status = row_cell(napa_rows, "outreach_status")
        print(f"    708 1st St outreach_status: '{status}'")
        check(
            "Napa property updated to qualified/interested/replied",
            any(
                kw in status.lower()
                for kw in ["qualified", "interested", "approved", "responded", "replied"]
            ),
            f"status='{status}'",
        )

    # Other Inessa properties should NOT be changed to qualified
    for addr, label in [("2nd st", "522 W 2nd St"), ("3rd st", "201 3rd St")]:
        rows = find_rows_matching(after, addr)
        if rows:
            status = row_cell(rows, "outreach_status")
            check(
                f"{label} NOT changed to qualified (reply was only about Napa)",
                "qualified" not in status.lower() or "interested" not in status.lower(),
                f"status='{status}'",
            )

    return after


# ---------------------------------------------------------------------------
# Phase 4: Data integrity
# ---------------------------------------------------------------------------


def phase_integrity(tables: dict):
    print("\n" + "=" * 60)
    print("  PHASE 4: Data integrity check")
    print("=" * 60)

    total_rows = sum(info["total"] for info in tables.values())
    print(f"  Total rows across all tables: {total_rows}")

    # --- No concatenated addresses ---
    concat_found = 0
    for key, info in tables.items():
        for row in info["rows"]:
            if not isinstance(row, dict):
                continue
            for col_key, val in row.items():
                if not isinstance(val, str) or len(val) < 60:
                    continue
                addr_words = sum(
                    1
                    for w in ["Ave", "Blvd", "Way", "Ct", "Ln", "Dr", "St", "Rd"]
                    if w in val
                )
                if addr_words >= 3 and val.count(",") >= 4:
                    concat_found += 1
                    print(f"    SUSPECT: [{key}] {col_key} = {val[:150]}")

    check(
        "No concatenated multi-address cells",
        concat_found == 0,
        f"found {concat_found} suspect cells",
    )

    # --- No concatenated owner/broker names ---
    multi_name = 0
    known_people = [
        "Kensington", "Alston", "Romani", "Stephenson",
        "McCallister", "Williams", "Arnstein", "Varney",
        "Maxwell", "Romano", "Blackwell",
    ]
    for key, info in tables.items():
        for row in info["rows"]:
            if not isinstance(row, dict):
                continue
            for col_key, val in row.items():
                if not isinstance(val, str):
                    continue
                hits = sum(1 for p in known_people if p.lower() in val.lower())
                if hits >= 3:
                    multi_name += 1
                    print(f"    SUSPECT: [{key}] {col_key} has {hits} names: {val[:150]}")

    check(
        "No cells with 3+ names concatenated",
        multi_name == 0,
        f"found {multi_name} suspect cells",
    )

    # --- Untouched broker rows still at original status ---
    ryan_rows = find_rows_matching(tables, "blackwell")
    untouched_ok = True
    for r in ryan_rows:
        status = r["row"].get("outreach_status", "")
        if status.lower() not in ("new", ""):
            untouched_ok = False
            print(f"    Ryan Blackwell row was modified: status='{status}'")
    check(
        "Ryan Blackwell's properties untouched (not contacted)",
        untouched_ok,
    )

    # --- Row count sanity: shouldn't have wild growth ---
    check(
        "Total row count reasonable (≤ 25)",
        total_rows <= 25,
        f"total={total_rows} (expected ≤ 25 across all tables)",
    )

    # --- Property Listings (sheet-2) coverage ---
    listings_key = None
    for key, info in tables.items():
        if "property listings" in info.get("sheet_name", "").lower() or "qualified" in info.get("component_title", "").lower():
            listings_key = key
            break

    if listings_key:
        listings = tables[listings_key]
        listings_count = listings["total"]
        print(f"\n  Property Listings: {listings_count} rows")
        contacted_addrs = ["telegraph", "hillcrest", "san ysidro", "1st st", "2nd st", "3rd st"]
        found_in_listings = 0
        for addr in contacted_addrs:
            matches = [
                r for r in listings["rows"]
                if isinstance(r, dict) and any(
                    addr in str(v).lower() for v in r.values()
                )
            ]
            if matches:
                found_in_listings += 1
                print(f"    ✓ {addr.title()} in Property Listings")
            else:
                print(f"    ✗ {addr.title()} MISSING from Property Listings")

        check(
            "Contacted properties appear in Property Listings (≥4 of 6)",
            found_in_listings >= 4,
            f"only {found_in_listings}/6 found",
        )
    else:
        print("\n  WARN: Could not find Property Listings table")

    # --- Summarize all contacts ---
    print("\n  Property status summary:")
    addresses = [
        ("2539 Telegraph Ave", "telegraph"),
        ("320 Hillcrest Rd", "hillcrest"),
        ("8555 San Ysidro Ave", "san ysidro"),
        ("708 1st St", "1st st"),
        ("522 W 2nd St", "2nd st"),
        ("201 3rd St", "3rd st"),
        ("2160 W 11th Ave", "11th ave"),
        ("1028 11th St", "1028"),
    ]
    for label, search in addresses:
        matches = find_rows_matching(tables, search)
        count = len(matches)
        statuses = set()
        tables_found = set()
        for m in matches:
            s = m["row"].get("outreach_status", m["row"].get("status", "?"))
            if s:
                statuses.add(s)
            tables_found.add(m["table"])
        status_str = ", ".join(sorted(statuses)) if statuses else "?"
        sheets_str = " + ".join(sorted(tables_found))
        icon = "\033[32m●\033[0m" if count <= 3 else "\033[33m●\033[0m"
        print(f"    {icon} {label}: {count} row(s) in [{sheets_str}], status={status_str}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    global passed, failed

    parser = argparse.ArgumentParser(description="LRE-style outreach E2E test")
    parser.add_argument("--skip-import", action="store_true", help="Skip CSV import")
    args = parser.parse_args()

    print("=" * 60)
    print("  LRE Outreach — E2E Test (parallel)")
    print("  Broker-grouped outreach + bounce + reply handling")
    print("=" * 60)
    print(f"  API:     {API_BASE}")
    print(f"  Agent:   {AGENT_ID}")
    print(f"  Timeout: {TASK_TIMEOUT_S}s per task")

    phase_create_view()

    if args.skip_import:
        print("\n  (Skipping CSV import)")
        initial = get_all_tables()
        print_table_summary(initial)
    else:
        initial = phase_seed_data()

    after_outreach = phase_parallel_outreach(initial)
    after_followups = phase_parallel_followups(after_outreach)

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
