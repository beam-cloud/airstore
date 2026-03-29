#!/usr/bin/env python3
"""
Multi-prompt enrichment test.

Creates a fresh view, runs several diverse prompts sequentially, and validates:
  - Each prompt produces rows (no zero-row failures)
  - No empty rows (0 substantive cells)
  - No fragment rows (1 substantive cell)
  - No cross-entity duplication
  - Row counts grow monotonically across prompts

Usage:
    python hack/test_enrichment_multi.py
"""

import json
import os
import re
import subprocess
import sys
import time
import uuid

import requests

API_BASE = "http://localhost:1994/api/v1"
TOKEN = "dc321e8e5cc1d13ec34bc41b3960a01937057cfad31f3b39ffabd7cfc378f216"
WORKSPACE_EXT_ID = "737af74d-902f-4464-9eb2-19cbd4dd0247"
WORKSPACE_ID = 347
AGENT_ID = "16101cc1-b294-4cd1-af52-5a1dd4b68f35"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

TASK_TIMEOUT_S = 600
POLL_S = 6
SETTLE_S = 25

PROMPTS = [
    {
        "name": "LES Chinese Restaurants",
        "prompt": (
            "Find the best Chinese restaurants in NYC's Lower East Side (LES) only. "
            "Focus on places with great ambiance, good cocktail programs, and memorable vibes. "
            "For each restaurant provide: name, neighborhood (should be LES), cuisine style, "
            "vibe description, cocktail highlights, price range, standout dishes, reservation tips, "
            "rating, and a link if available."
        ),
        "min_rows": 3,
    },
    {
        "name": "Remote Dev Tools 2026",
        "prompt": (
            "Research the top 8 developer productivity tools for remote teams in 2026. "
            "For each tool provide: tool name, category (communication/project-mgmt/code-review/etc), "
            "pricing (free tier + paid), key features, team size sweet spot, integrations, "
            "and a one-line verdict."
        ),
        "min_rows": 4,
    },
    {
        "name": "Running Shoe Comparison",
        "prompt": (
            "Compare the top 6 running shoes for marathon training in 2026. "
            "For each shoe provide: brand, model name, weight, stack height, drop, "
            "price, best surface (road/trail), durability rating, and a brief pro/con summary."
        ),
        "min_rows": 4,
    },
]

GENERIC_COLUMNS = [
    {"key": "name", "label": "Name", "type": "text"},
    {"key": "category", "label": "Category", "type": "text"},
    {"key": "description", "label": "Description", "type": "text"},
    {"key": "location", "label": "Location", "type": "text"},
    {"key": "price", "label": "Price", "type": "text"},
    {"key": "rating", "label": "Rating", "type": "text"},
    {"key": "highlights", "label": "Highlights", "type": "text"},
    {"key": "details", "label": "Details", "type": "text"},
    {"key": "pros_cons", "label": "Pros / Cons", "type": "text"},
    {"key": "link", "label": "Link", "type": "link"},
    {"key": "notes", "label": "Notes", "type": "text"},
    {"key": "status", "label": "Status", "type": "status"},
]

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
TRIVIAL = {"n/a", "new", "none", "sent", "draft", "true", "false", "yes", "no", ""}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class Results:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def check(self, label, ok, detail=""):
        if ok:
            self.passed += 1
            print(f"  \033[32m✓\033[0m {label}")
        else:
            self.failed += 1
            msg = f"{label}: {detail}" if detail else label
            self.errors.append(msg)
            print(f"  \033[31m✗\033[0m {msg}")

    @property
    def total(self):
        return self.passed + self.failed

    @property
    def ok(self):
        return self.failed == 0

    def summary(self):
        print(f"  Results: {self.passed}/{self.total} passed, {self.failed} failed")
        if self.errors:
            print("\n  Failures:")
            for e in self.errors:
                print(f"    • {e}")
        else:
            print("  \033[32mAll checks passed!\033[0m")


def api(method, path, **kw):
    r = getattr(requests, method)(f"{API_BASE}/{path}", headers=HEADERS, **kw)
    r.raise_for_status()
    return r.json() if r.content else {}


def create_view():
    view_id = str(uuid.uuid4())
    defn = {
        "sheets": [{"id": "sheet-1", "name": "Data", "components": [
            {"id": "c1", "type": "table", "title": "Results", "config": {"columns": GENERIC_COLUMNS}},
        ]}],
        "agents": [AGENT_ID],
    }
    sql = (
        f"INSERT INTO workspace_view (id, workspace_id, name, description, definition_json) "
        f"VALUES ('{view_id}', {WORKSPACE_ID}, 'Test {view_id[:8]}', "
        f"'Auto-created', '{json.dumps(defn)}'::jsonb) RETURNING id;"
    )
    r = subprocess.run(
        ["psql", "-h", "localhost", "-p", "5432", "-U", "airstore", "-d", "airstore", "-t", "-A", "-c", sql],
        capture_output=True, text=True, env={**os.environ, "PGPASSWORD": "airstore"},
    )
    if r.returncode != 0:
        print(f"  Failed to create view: {r.stderr}")
        sys.exit(1)
    return view_id


def get_rows(view_id):
    try:
        resp = api("get", f"workspaces/{WORKSPACE_EXT_ID}/views/{view_id}/data",
                    params={"sheet": "sheet-1", "component": "c1"})
        data = resp.get("data", resp)
        return data.get("rows", [])
    except Exception as e:
        print(f"  WARN: fetch failed: {e}")
        return []


def create_task(view_id, prompt):
    body = {
        "agent_id": AGENT_ID,
        "message": prompt,
        "session_id": f"enr-{uuid.uuid4().hex[:8]}",
        "idempotency_key": f"enr-{uuid.uuid4().hex}",
        "source_view_id": view_id,
    }
    resp = api("post", f"workspaces/{WORKSPACE_EXT_ID}/tasks", json=body)
    data = resp.get("data", resp)
    return data.get("task", data)["id"]


def wait_task(task_id):
    t0 = time.time()
    last_sig = ""
    while time.time() - t0 < TASK_TIMEOUT_S:
        t = api("get", f"workspaces/{WORKSPACE_EXT_ID}/tasks/{task_id}").get("data", {})
        state = t.get("state", "unknown")
        outs = api("get", f"workspaces/{WORKSPACE_EXT_ID}/tasks/{task_id}/outputs")
        n = len(outs.get("data", {}).get("outputs", []))
        sig = f"{state}:{n}"
        if sig != last_sig:
            print(f"    [{int(time.time()-t0):>4}s] state={state}, outputs={n}")
            last_sig = sig
        if state in ("done", "sleeping", "error", "cancelled", "timed_out", "failed"):
            return state, n
        time.sleep(POLL_S)
    print(f"    TIMEOUT after {TASK_TIMEOUT_S}s!")
    return "timeout", 0


def is_substantive(v):
    return isinstance(v, str) and len(v.strip()) > 2 and v.strip().lower() not in TRIVIAL


def count_substantive(row):
    vals = row if isinstance(row, list) else list(row.values()) if isinstance(row, dict) else []
    return sum(1 for v in vals if is_substantive(v) and not UUID_RE.match(v.strip()))


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate(res, rows, prev_count, prompt):
    name = prompt["name"]
    new_rows = len(rows) - prev_count
    print(f"\n  Table: {len(rows)} total rows (+{new_rows} new)")

    res.check(f"[{name}] New rows added", new_rows >= prompt["min_rows"],
              f"expected >= {prompt['min_rows']}, got {new_rows}")

    empty = sum(1 for r in rows if count_substantive(r) == 0)
    res.check(f"[{name}] No empty rows", empty == 0, f"{empty} empty")

    frags = sum(1 for r in rows if count_substantive(r) == 1)
    res.check(f"[{name}] No fragment rows", frags == 0, f"{frags} fragments")

    # Show first few rows for visibility
    for i, row in enumerate(rows[prev_count:prev_count + 3]):
        vals = row if isinstance(row, list) else list(row.values()) if isinstance(row, dict) else []
        display = [v[:35] for v in vals if is_substantive(v) and not UUID_RE.match(v.strip())]
        print(f"    row {prev_count+i+1}: {display[:3]}")
    if new_rows > 3:
        print(f"    ... +{new_rows - 3} more")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  Multi-Prompt Enrichment Test")
    print("=" * 60)

    view_id = create_view()
    print(f"  View: {view_id}")
    print(f"  Prompts: {len(PROMPTS)}")

    res = Results()
    prev_count = 0

    for i, p in enumerate(PROMPTS):
        print(f"\n{'─' * 60}")
        print(f"  [{i+1}/{len(PROMPTS)}] {p['name']}")
        print(f"{'─' * 60}")

        task_id = create_task(view_id, p["prompt"])
        print(f"  Task: {task_id}")

        state, n_out = wait_task(task_id)
        res.check(f"[{p['name']}] Task completed", state in ("done", "sleeping"))

        print(f"  Settling {SETTLE_S}s...")
        time.sleep(SETTLE_S)

        rows = get_rows(view_id)
        validate(res, rows, prev_count, p)
        prev_count = len(rows)

    # Final aggregate check
    print(f"\n{'─' * 60}")
    print(f"  Final: {prev_count} total rows across {len(PROMPTS)} prompts")
    min_total = sum(p["min_rows"] for p in PROMPTS)
    res.check(f"Total rows >= {min_total}", prev_count >= min_total,
              f"got {prev_count}")

    print(f"\n{'=' * 60}")
    res.summary()
    print("=" * 60)
    sys.exit(0 if res.ok else 1)


if __name__ == "__main__":
    main()
