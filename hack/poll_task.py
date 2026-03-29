#!/usr/bin/env python3
"""Poll a task until it reaches a target state, auto-approving along the way."""
import json, sys, time, requests

API = "http://localhost:1994/api/v1"
TOKEN = "e8c8f312c3a7959d56b8186521add2374bc1cd7c896a2a21ba1a12a402e9f72f"
WS = "737af74d-902f-4464-9eb2-19cbd4dd0247"
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

task_id = sys.argv[1] if len(sys.argv) > 1 else None
target = sys.argv[2] if len(sys.argv) > 2 else "sleeping"
if not task_id:
    print("Usage: python3 hack/poll_task.py <task_id> [target_state]")
    sys.exit(1)

start = time.time()
last = None
while time.time() - start < 600:
    try:
        r = requests.get(f"{API}/workspaces/{WS}/tasks/{task_id}", headers=H, timeout=10)
        t = r.json().get("data", {})
        s = t.get("state", "?")
        ik = t.get("input_kind", "")
        el = int(time.time() - start)
        if s != last:
            wr = (t.get("wake_reason") or "")[:80]
            print(f"[{el:3d}s] state={s} ik={ik}" + (f" wake={wr}" if wr else ""))
            last = s
        if s == "waiting" and ik == "approve_reject":
            print(f"[{el:3d}s] -> auto-approving")
            requests.post(f"{API}/workspaces/{WS}/tasks/{task_id}/input", headers=H,
                          json={"message": "Approved. Send it now."}, timeout=10)
            last = None
        if s == target:
            print(f"\nReached target state '{target}'")
            break
        if s in ("error", "cancelled", "completed") and s != target:
            print(f"\nTerminal state '{s}' (wanted '{target}')")
            break
    except Exception as e:
        print(f"[{int(time.time()-start):3d}s] err: {e}")
    time.sleep(6)
else:
    print(f"\nTimeout after 600s, last={last}")
