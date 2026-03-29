#!/usr/bin/env python3
"""
Sales Pipeline E2E test — a completely different domain from LRE to validate
ViewSync, multi-sheet population, and dedup generically.

Models a B2B SaaS sales workflow:
  1. Import seed data — target accounts with contacts
  2. Research & outreach — agent researches companies and sends intro emails
  3. Meeting follow-up — agent logs a meeting outcome for one deal
  4. Verify multi-sheet population, no duplication, proper status updates

Usage:
    python hack/test_sales_pipeline.py
"""

import json
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

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
ROW_SETTLE_S = 25
AUTO_APPROVE_DELAY_S = 3

VIEW_DEFINITION = {
    "name": "Enterprise Sales Pipeline",
    "description": "B2B SaaS sales pipeline tracking deals and meeting activity",
    "agents": [AGENT_ID],
    "sheets": [
        {
            "id": "sheet-deals",
            "name": "Deal Tracker",
            "layout": {"columns": 12},
            "components": [
                {
                    "id": "deals-table",
                    "type": "table",
                    "title": "Active Deals",
                    "position": {"col": 0, "row": 0, "colSpan": 0, "rowSpan": 0},
                    "config": {
                        "columns": [
                            {"key": "company", "label": "Company", "type": "text"},
                            {"key": "contact_name", "label": "Contact", "type": "text"},
                            {"key": "contact_email", "label": "Email", "type": "email"},
                            {"key": "industry", "label": "Industry", "type": "text"},
                            {"key": "deal_size", "label": "Deal Size", "type": "text"},
                            {"key": "stage", "label": "Stage", "type": "status"},
                            {"key": "next_step", "label": "Next Step", "type": "text"},
                        ]
                    },
                }
            ],
        },
        {
            "id": "sheet-activity",
            "name": "Activity Log",
            "layout": {"columns": 12},
            "components": [
                {
                    "id": "activity-table",
                    "type": "table",
                    "title": "Recent Activity",
                    "position": {"col": 0, "row": 0, "colSpan": 0, "rowSpan": 0},
                    "config": {
                        "columns": [
                            {"key": "company", "label": "Company", "type": "text"},
                            {"key": "contact_name", "label": "Contact", "type": "text"},
                            {"key": "activity_type", "label": "Type", "type": "text"},
                            {"key": "summary", "label": "Summary", "type": "text"},
                            {"key": "outcome", "label": "Outcome", "type": "text"},
                            {"key": "next_steps", "label": "Next Steps", "type": "text"},
                        ]
                    },
                }
            ],
        },
    ],
}

SEED_CSV = """\
company,contact_name,contact_email,industry,deal_size,stage,next_step
Meridian Health Systems,Dr. Sarah Chen,luke@beam.cloud,Healthcare,$120K ARR,prospecting,Initial outreach
Cobalt Manufacturing,James Rivera,luke@beam.cloud,Manufacturing,$85K ARR,prospecting,Initial outreach
Pinnacle Financial Group,Amanda Torres,luke@beam.cloud,Finance,$200K ARR,prospecting,Initial outreach
NovaTech Solutions,Raj Patel,luke@beam.cloud,Technology,$65K ARR,prospecting,Initial outreach
Greenfield Agriculture,Lisa Nakamura,luke@beam.cloud,Agriculture,$45K ARR,prospecting,Initial outreach
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


def get_view_rows(view_id: str, sheet_id: str, component_id: str) -> list[dict]:
    resp = api_get(
        f"/views/{view_id}/data",
        params={"sheet": sheet_id, "component": component_id},
    )
    data = resp.get("data", {})
    columns = data.get("columns", [])
    raw_rows = data.get("rows", [])
    keyed = []
    for row in raw_rows:
        if isinstance(row, list):
            d = {}
            for i, val in enumerate(row):
                if val is not None and i < len(columns):
                    d[columns[i]] = str(val) if not isinstance(val, str) else val
            keyed.append(d)
        elif isinstance(row, dict):
            keyed.append(row)
    return keyed


def find_rows(rows: list[dict], search: str) -> list[dict]:
    sl = search.lower()
    return [r for r in rows if any(sl in str(v).lower() for v in r.values())]


# -- Task helpers --


TERMINAL_STATES = {"done", "error", "cancelled", "timed_out", "failed", "sleeping"}


def get_task(task_id: str) -> dict:
    return api_get(f"/tasks/{task_id}")["data"]


def get_task_outputs(task_id: str) -> list:
    return api_get(f"/tasks/{task_id}/outputs").get("data", {}).get("outputs", [])


def submit_input(task_id: str, message: str):
    api_post(f"/tasks/{task_id}/input", {"message": message})


def wait_for_task(
    task_id: str, label: str = "", auto_approve: bool = True, timeout_s: int = TASK_TIMEOUT_S
) -> dict:
    prefix = f"[{label}] " if label else ""
    start = time.time()
    last_state = ""
    last_oc = 0
    approvals = 0

    while time.time() - start < timeout_s:
        task = get_task(task_id)
        state = task.get("state", "unknown")
        ik = task.get("input_kind", "")
        oc = len(get_task_outputs(task_id))

        if state != last_state or oc != last_oc:
            elapsed = time.time() - start
            line = f"    {prefix}[{elapsed:5.0f}s] state={state}, outputs={oc}"
            if ik:
                line += f", input_kind={ik}"
            print(line)
            last_state = state
            last_oc = oc

        if auto_approve and state == "waiting" and ik in ("approve_reject", "free_text"):
            time.sleep(AUTO_APPROVE_DELAY_S)
            msg = (
                "Approved. Send it now."
                if ik == "approve_reject"
                else "Yes, proceed as described."
            )
            print(f"    {prefix}[{time.time() - start:5.0f}s] → auto-responding ({ik})")
            try:
                submit_input(task_id, msg)
                approvals += 1
            except Exception as e:
                print(f"    {prefix}WARN: auto-respond failed: {e}")
            last_state = ""
            continue

        if state in TERMINAL_STATES:
            if approvals:
                print(f"    {prefix}Auto-approved {approvals} interaction(s)")
            return task

        time.sleep(POLL_INTERVAL_S)

    print(f"    {prefix}\033[33mTimeout after {timeout_s}s\033[0m")
    return get_task(task_id)


def create_task(view_id: str, message: str) -> dict:
    session_id = f"sales-{uuid.uuid4().hex[:8]}"
    body = {
        "message": message,
        "agent_id": AGENT_ID,
        "session_id": session_id,
        "idempotency_key": f"sales-{uuid.uuid4().hex}",
        "source_view_id": view_id,
    }
    resp = api_post("/tasks", body=body)
    task = resp["data"]["task"]
    print(f"    Task ID: {task['id']} (session: {session_id})")
    return task


# -- Upload helpers --


def upload_csv(content: str) -> str | None:
    fname = f"sales-seed-{uuid.uuid4().hex[:8]}.csv"
    path = f"/uploads/sales-test/{fname}"
    resp = api_post("/fs/upload-url", {"path": path, "content_type": "text/csv"})
    data = resp.get("data", resp)
    upload_url = data.get("upload_url")
    if not upload_url:
        print(f"    WARN: no upload_url: {resp}")
        return None
    api_put(upload_url, content.encode("utf-8"))
    api_post("/fs/upload-complete", {"path": path})
    return path


# ---------------------------------------------------------------------------
# Phase 0: Create the view
# ---------------------------------------------------------------------------


def phase_create_view() -> str:
    print("\n" + "=" * 60)
    print("  PHASE 0: Create view — Enterprise Sales Pipeline")
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
    for s in sheets:
        print(f"    Sheet: {s.get('name')} ({s.get('id')})")

    return view_id


# ---------------------------------------------------------------------------
# Phase 1: Seed data
# ---------------------------------------------------------------------------


def phase_seed_data(view_id: str):
    print("\n" + "=" * 60)
    print("  PHASE 1: Seed data — import target accounts")
    print("=" * 60)

    path = upload_csv(SEED_CSV)
    check("CSV uploaded", path is not None)
    if not path:
        sys.exit(1)

    resp = api_post(
        f"/views/{view_id}/sheets/sheet-deals/import",
        {"file_path": path},
        timeout=120,
    )
    result = resp.get("data", resp)
    row_count = result.get("row_count", 0)
    check("CSV imported (5 rows)", row_count >= 5, f"rows={row_count}")

    time.sleep(3)
    deals = get_view_rows(view_id, "sheet-deals", "deals-table")
    print(f"    Deal Tracker: {len(deals)} rows")
    check("All 5 companies imported", len(deals) >= 5, f"got {len(deals)}")

    for company in ["Meridian", "Cobalt", "Pinnacle", "NovaTech", "Greenfield"]:
        matches = find_rows(deals, company)
        check(f"{company} present in Deal Tracker", len(matches) >= 1, f"found {len(matches)}")


# ---------------------------------------------------------------------------
# Phase 2: Parallel outreach — two groups
# ---------------------------------------------------------------------------


def run_outreach_healthcare_finance(view_id: str) -> dict:
    task = create_task(
        view_id,
        "Reach out to the healthcare and finance prospects: "
        "Dr. Sarah Chen at Meridian Health Systems and Amanda Torres at "
        "Pinnacle Financial Group. Send them personalized intro emails "
        "about our SaaS platform."
    )
    result = wait_for_task(task["id"], label="HC+Fin", auto_approve=True)
    return {"task": task, "result": result, "outputs": get_task_outputs(task["id"])}


def run_outreach_manufacturing_tech(view_id: str) -> dict:
    task = create_task(
        view_id,
        "Reach out to the manufacturing and tech prospects: "
        "James Rivera at Cobalt Manufacturing and Raj Patel at "
        "NovaTech Solutions. Send personalized intro emails about "
        "our SaaS platform."
    )
    result = wait_for_task(task["id"], label="Mfg+Tech", auto_approve=True)
    return {"task": task, "result": result, "outputs": get_task_outputs(task["id"])}


def phase_outreach(view_id: str):
    print("\n" + "=" * 60)
    print("  PHASE 2: Parallel outreach — HC+Finance & Mfg+Tech")
    print("=" * 60)

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(run_outreach_healthcare_finance, view_id)
        f2 = pool.submit(run_outreach_manufacturing_tech, view_id)
        r1 = f1.result()
        r2 = f2.result()

    for label, data in [("HC+Fin", r1), ("Mfg+Tech", r2)]:
        state = data["result"].get("state", "unknown")
        check(f"{label} outreach completed", state in TERMINAL_STATES, f"state={state}")
        for o in data["outputs"][:5]:
            otype = o.get("output_type", "?")
            title = o.get("title", "?")[:70]
            print(f"    [{label}] [{otype}] {title}")

    print(f"\n  Waiting {ROW_SETTLE_S}s for enrichment pipeline...")
    time.sleep(ROW_SETTLE_S)

    deals = get_view_rows(view_id, "sheet-deals", "deals-table")
    activity = get_view_rows(view_id, "sheet-activity", "activity-table")
    print(f"    Deal Tracker: {len(deals)} rows")
    print(f"    Activity Log: {len(activity)} rows")

    updated = 0
    for company in ["Meridian", "Cobalt", "Pinnacle", "NovaTech"]:
        matches = find_rows(deals, company)
        if matches:
            stage = matches[0].get("stage", "")
            print(f"    {company} stage: '{stage}'")
            if stage.lower() not in ("prospecting", "new", ""):
                updated += 1

    check(
        "At least 2/4 contacted companies have updated stage",
        updated >= 2,
        f"only {updated}/4 updated",
    )

    greenfield = find_rows(deals, "greenfield")
    if greenfield:
        stage = greenfield[0].get("stage", "")
        check(
            "Greenfield Agriculture untouched (not contacted)",
            stage.lower() in ("prospecting", "new", ""),
            f"stage='{stage}'",
        )


# ---------------------------------------------------------------------------
# Phase 3: Meeting follow-up — one deal progresses
# ---------------------------------------------------------------------------


def phase_meeting(view_id: str):
    print("\n" + "=" * 60)
    print("  PHASE 3: Meeting follow-up — Pinnacle Financial")
    print("=" * 60)

    task = create_task(
        view_id,
        "Amanda Torres from Pinnacle Financial Group replied positively. "
        "We had a 30-minute discovery call. She's interested in the "
        "enterprise plan ($200K ARR). Her team needs SOC 2 compliance docs. "
        "Schedule a technical demo for next week. Update the CRM."
    )
    result = wait_for_task(task["id"], label="Meeting", auto_approve=True)
    state = result.get("state", "unknown")
    check("Meeting task completed", state in TERMINAL_STATES, f"state={state}")

    print(f"\n  Waiting {ROW_SETTLE_S}s for enrichment pipeline...")
    time.sleep(ROW_SETTLE_S)

    deals = get_view_rows(view_id, "sheet-deals", "deals-table")
    activity = get_view_rows(view_id, "sheet-activity", "activity-table")
    print(f"    Deal Tracker: {len(deals)} rows")
    print(f"    Activity Log: {len(activity)} rows")

    pinnacle = find_rows(deals, "pinnacle")
    if pinnacle:
        stage = pinnacle[0].get("stage", "")
        print(f"    Pinnacle stage: '{stage}'")
        check(
            "Pinnacle progressed past prospecting",
            stage.lower() not in ("prospecting", "new", ""),
            f"stage='{stage}'",
        )

    pinnacle_activity = find_rows(activity, "pinnacle")
    print(f"    Pinnacle entries in Activity Log: {len(pinnacle_activity)}")


# ---------------------------------------------------------------------------
# Phase 4: Data integrity
# ---------------------------------------------------------------------------


def phase_integrity(view_id: str):
    print("\n" + "=" * 60)
    print("  PHASE 4: Data integrity check")
    print("=" * 60)

    deals = get_view_rows(view_id, "sheet-deals", "deals-table")
    activity = get_view_rows(view_id, "sheet-activity", "activity-table")
    total = len(deals) + len(activity)
    print(f"  Deal Tracker: {len(deals)} rows")
    print(f"  Activity Log: {len(activity)} rows")
    print(f"  Total: {total} rows")

    # No wild row growth
    check("Deal Tracker row count reasonable (≤ 10)", len(deals) <= 10, f"got {len(deals)}")
    check("Activity Log row count reasonable (≤ 15)", len(activity) <= 15, f"got {len(activity)}")

    # No empty rows
    empty = 0
    for row in deals + activity:
        non_empty = sum(1 for v in row.values() if v and str(v).strip())
        if non_empty <= 2:
            empty += 1
    check("No nearly-empty rows", empty == 0, f"found {empty} rows with ≤2 cells")

    # No concatenated company names
    companies = ["Meridian", "Cobalt", "Pinnacle", "NovaTech", "Greenfield"]
    concat = 0
    for row in deals + activity:
        for val in row.values():
            if not isinstance(val, str):
                continue
            hits = sum(1 for c in companies if c.lower() in val.lower())
            if hits >= 3:
                concat += 1
                print(f"    SUSPECT concatenation: {val[:120]}")
    check("No cells with 3+ company names concatenated", concat == 0, f"found {concat}")

    # Each seed company should appear exactly once in Deal Tracker (no duplication)
    for company in companies:
        matches = find_rows(deals, company)
        check(
            f"{company} appears exactly once in Deal Tracker",
            len(matches) == 1,
            f"found {len(matches)} rows",
        )

    # Activity Log coverage — contacted companies should appear
    contacted = ["Meridian", "Cobalt", "Pinnacle", "NovaTech"]
    found_in_activity = 0
    for company in contacted:
        matches = find_rows(activity, company)
        if matches:
            found_in_activity += 1
            print(f"    ✓ {company} in Activity Log")
        else:
            print(f"    ✗ {company} MISSING from Activity Log")

    check(
        f"Contacted companies appear in Activity Log (≥2 of {len(contacted)})",
        found_in_activity >= 2,
        f"only {found_in_activity}/{len(contacted)} found",
    )

    # Greenfield should NOT be in Activity Log
    greenfield_activity = find_rows(activity, "greenfield")
    check(
        "Greenfield (not contacted) absent from Activity Log",
        len(greenfield_activity) == 0,
        f"found {len(greenfield_activity)} rows",
    )

    # Summary
    print("\n  Deal status summary:")
    for company in companies:
        deal_matches = find_rows(deals, company)
        act_matches = find_rows(activity, company)
        stage = deal_matches[0].get("stage", "?") if deal_matches else "?"
        tables = []
        if deal_matches:
            tables.append("deals")
        if act_matches:
            tables.append(f"activity({len(act_matches)})")
        icon = "\033[32m●\033[0m" if len(deal_matches) <= 1 else "\033[33m●\033[0m"
        print(f"    {icon} {company}: stage={stage}, in [{' + '.join(tables)}]")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def cleanup_view(view_id: str):
    try:
        r = requests.delete(
            base_url(f"/views/{view_id}"),
            headers=HEADERS,
            timeout=30,
        )
        if r.ok:
            print(f"  Cleaned up view {view_id}")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    global passed, failed

    print("=" * 60)
    print("  Sales Pipeline — E2E Test")
    print("  B2B SaaS outreach + meeting + multi-sheet sync")
    print("=" * 60)
    print(f"  API:     {API_BASE}")
    print(f"  Agent:   {AGENT_ID}")
    print(f"  Timeout: {TASK_TIMEOUT_S}s per task")

    start = time.time()

    view_id = phase_create_view()
    phase_seed_data(view_id)
    phase_outreach(view_id)
    phase_meeting(view_id)
    phase_integrity(view_id)

    elapsed = time.time() - start

    print("\n" + "=" * 60)
    total = passed + failed
    print(f"  Results: {passed}/{total} passed, {failed} failed")
    print(f"  Elapsed: {elapsed:.0f}s")
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
