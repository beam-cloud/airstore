#!/usr/bin/env python3
"""Quick test: restaurant research prompt → check for fragment/duplicate rows."""

import json
import sys
import time
import uuid

import requests

API_BASE = "http://localhost:1994/api/v1"
TOKEN = "dc321e8e5cc1d13ec34bc41b3960a01937057cfad31f3b39ffabd7cfc378f216"
WORKSPACE_EXT_ID = "737af74d-902f-4464-9eb2-19cbd4dd0247"
VIEW_ID = "ae0b7d0e-c52e-4ad5-88bb-a038657df98b"
AGENT_ID = "16101cc1-b294-4cd1-af52-5a1dd4b68f35"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
TIMEOUT_S = 300
POLL_S = 6

PROMPT = (
    "Find the best Chinese restaurants in NYC's Lower East Side (LES) only. "
    "Focus on places with great ambiance, good cocktail programs, and memorable vibes. "
    "For each restaurant provide: name, neighborhood (should be LES), cuisine style, "
    "vibe description, cocktail highlights, price range, standout dishes, reservation tips, "
    "rating, and a link if available."
)


def api(method, path, **kw):
    r = getattr(requests, method)(f"{API_BASE}/{path}", headers=HEADERS, **kw)
    r.raise_for_status()
    return r.json() if r.content else {}


def create_task(prompt):
    body = {
        "agent_id": AGENT_ID,
        "message": prompt,
        "session_id": f"rest-{uuid.uuid4().hex[:8]}",
        "idempotency_key": f"rest-{uuid.uuid4().hex}",
        "source_view_id": VIEW_ID,
    }
    resp = api("post", f"workspaces/{WORKSPACE_EXT_ID}/tasks", json=body)
    data = resp.get("data", resp)
    task = data.get("task", data)
    return task["id"]


def wait_task(task_id):
    t0 = time.time()
    while time.time() - t0 < TIMEOUT_S:
        resp = api("get", f"workspaces/{WORKSPACE_EXT_ID}/tasks/{task_id}")
        t = resp.get("data", resp)
        state = t.get("state", t.get("status", "unknown"))
        out_resp = api("get", f"workspaces/{WORKSPACE_EXT_ID}/tasks/{task_id}/outputs")
        out_data = out_resp.get("data", out_resp)
        outputs = out_data.get("outputs", out_data if isinstance(out_data, list) else [])
        n_out = len(outputs)
        elapsed = int(time.time() - t0)
        print(f"  [{elapsed:>5}s] state={state}, outputs={n_out}")

        if state in ("done", "sleeping", "error", "cancelled", "timed_out", "failed"):
            return state, outputs
        time.sleep(POLL_S)

    print("  TIMEOUT!")
    return "timeout", []


TABLES = [
    ("sheet-1", "c1", "Outreach Pipeline", "Contacts & Outreach"),
    ("sheet-2", "c2", "Property Listings", "Qualified Properties"),
]


def get_all_tables():
    result = {}
    for sheet_id, comp_id, sheet_name, comp_title in TABLES:
        key = f"{sheet_id}:{comp_id}"
        try:
            resp = api(
                "get",
                f"workspaces/{WORKSPACE_EXT_ID}/views/{VIEW_ID}/data",
                params={"sheet": sheet_id, "component": comp_id},
            )
            data = resp.get("data", resp)
            rows = data.get("rows", [])
            columns = data.get("columns", [])
            result[key] = {
                "total": len(rows),
                "rows": rows,
                "columns": columns,
                "sheet_name": sheet_name,
                "component_title": comp_title,
            }
        except Exception as e:
            print(f"  WARN: failed to fetch {sheet_name}/{comp_title}: {e}")
            result[key] = {
                "total": 0,
                "rows": [],
                "columns": [],
                "sheet_name": sheet_name,
                "component_title": comp_title,
            }
    return result


def main():
    print("=" * 60)
    print("  Restaurant Enrichment Test")
    print("=" * 60)
    print(f"  Prompt: {PROMPT[:80]}...")
    print()

    task_id = create_task(PROMPT)
    print(f"  Task: {task_id}")
    print()

    state, outputs = wait_task(task_id)
    print(f"\n  Final state: {state}")
    n_out = len(outputs) if isinstance(outputs, list) else 0
    print(f"  Outputs: {n_out}")

    if isinstance(outputs, list):
        for o in outputs:
            print(f"    - [{o.get('output_type','')}] {o.get('title','')}")

    print("\n  Waiting 25s for enrichment pipeline...")
    time.sleep(25)

    tables = get_all_tables()
    print()
    passed = 0
    failed = 0

    for key, info in tables.items():
        name = f"{info['sheet_name']}/{info['component_title']}"
        rows = info["rows"]
        total = info["total"]
        print(f"  Table: {name} — {total} rows")

        empty_rows = 0
        fragment_rows = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            non_empty = sum(
                1 for v in row.values()
                if isinstance(v, str) and len(v.strip()) > 2
                and v.strip().lower() not in ("n/a", "new", "none", "sent", "draft")
            )
            if non_empty <= 1:
                empty_rows += 1
                print(f"    EMPTY ROW: {json.dumps({k: v for k, v in row.items() if v}, indent=None)[:200]}")
            elif non_empty <= 2:
                fragment_rows += 1
                print(f"    FRAGMENT ROW: {json.dumps({k: v for k, v in row.items() if v}, indent=None)[:200]}")

        if empty_rows == 0:
            print(f"    ✓ No empty rows")
            passed += 1
        else:
            print(f"    ✗ {empty_rows} empty row(s)")
            failed += 1

        if fragment_rows == 0:
            print(f"    ✓ No fragment rows")
            passed += 1
        else:
            print(f"    ✗ {fragment_rows} fragment row(s)")
            failed += 1

        dup_values = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            for col_key, val in row.items():
                if not isinstance(val, str) or len(val) < 10:
                    continue
                val_norm = val.strip().lower()
                dup_values.setdefault((col_key, val_norm), []).append(1)

        duplicated = {k: len(v) for k, v in dup_values.items() if len(v) > 1}
        if not duplicated:
            print(f"    ✓ No duplicated cell values")
            passed += 1
        else:
            for (col, val), count in list(duplicated.items())[:5]:
                print(f"    DUP: {col} = '{val[:60]}' × {count}")
            failed += 1

        print()

    print("=" * 60)
    total_checks = passed + failed
    print(f"  Results: {passed}/{total_checks} passed, {failed} failed")
    if failed == 0:
        print("  \033[32mAll checks passed!\033[0m")
    print("=" * 60)
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
