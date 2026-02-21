#!/bin/bash
set -euo pipefail

#
# End-to-end browser automation test.
#
# Reads skills/browser-test/SKILL.md, creates a task via the REST API,
# and streams logs until completion.
#
# Usage:
#   ./bin/test_browser_task.sh
#   ./bin/test_browser_task.sh -w myworkspace
#   GATEWAY_HTTP=http://host:1994 ./bin/test_browser_task.sh
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
SKILL_FILE="$ROOT_DIR/skills/browser-test/SKILL.md"

GATEWAY_HTTP="${GATEWAY_HTTP:-http://localhost:1994}"
TOKEN="${AIRSTORE_TOKEN:-airstore-dev-admin-token}"
WORKSPACE_NAME="${WORKSPACE_NAME:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -w|--workspace) WORKSPACE_NAME="$2"; shift 2 ;;
        -g|--gateway)   GATEWAY_HTTP="$2"; shift 2 ;;
        -t|--token)     TOKEN="$2"; shift 2 ;;
        -h|--help)      sed -n '3,/^$/p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

# --- Helpers ---------------------------------------------------------------

api() {
    local method="$1" path="$2"; shift 2
    curl -sf -X "$method" "$GATEWAY_HTTP/api/v1$path" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        "$@"
}

info()  { printf "\033[1;34m==> %s\033[0m\n" "$*"; }
ok()    { printf "\033[1;32m  ✓ %s\033[0m\n" "$*"; }
err()   { printf "\033[1;31m  ✗ %s\033[0m\n" "$*" >&2; }
dim()   { printf "\033[2m%s\033[0m\n" "$*"; }

# --- Preflight -------------------------------------------------------------

info "Preflight"
command -v jq &>/dev/null || { err "jq is required"; exit 1; }
ok "jq"

curl -sf "$GATEWAY_HTTP/api/v1/health" &>/dev/null || {
    err "Gateway unreachable at $GATEWAY_HTTP"
    dim "Start with: okteto up  or  ./bin/airstore mount"
    exit 1
}
ok "Gateway at $GATEWAY_HTTP"

[[ -f "$SKILL_FILE" ]] || { err "Skill not found: $SKILL_FILE"; exit 1; }
ok "Skill: $SKILL_FILE"

# --- Workspace -------------------------------------------------------------

if [[ -z "$WORKSPACE_NAME" ]]; then
    WORKSPACE_NAME=$(api GET /workspaces | jq -r '.data[0].name // empty')
fi
[[ -n "$WORKSPACE_NAME" ]] || { err "No workspaces found"; exit 1; }
ok "Workspace: $WORKSPACE_NAME"

# --- Extract prompt --------------------------------------------------------

info "Reading skill"
PROMPT=$(awk 'BEGIN{f=0} /^---$/{f++;next} f>=2{print}' "$SKILL_FILE")
[[ -n "$PROMPT" ]] || { err "Empty prompt in SKILL.md"; exit 1; }
ok "Prompt: $(echo "$PROMPT" | wc -l | tr -d ' ') lines"

# --- Create task -----------------------------------------------------------

info "Creating task"
RESPONSE=$(api POST /tasks -d "$(jq -n \
    --arg ws "$WORKSPACE_NAME" \
    --arg prompt "$PROMPT" \
    '{workspace_name: $ws, prompt: $prompt}')")

TASK_ID=$(echo "$RESPONSE" | jq -r '.data.external_id')
[[ -n "$TASK_ID" && "$TASK_ID" != "null" ]] || {
    err "Failed to create task"
    echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
    exit 1
}
ok "Task $TASK_ID"

# --- Stream logs -----------------------------------------------------------

info "Streaming logs (Ctrl-C to detach)"
dim "Task: $TASK_ID"
dim "URL:  $GATEWAY_HTTP/api/v1/tasks/$TASK_ID/logs/stream"
echo

curl -sN "$GATEWAY_HTTP/api/v1/tasks/$TASK_ID/logs/stream" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: text/event-stream" 2>/dev/null | while IFS= read -r line; do
    [[ "$line" == data:* ]] || continue
    json="${line#data: }"
    type=$(echo "$json" | jq -r '.type // empty' 2>/dev/null) || continue
    case "$type" in
        log)
            text=$(echo "$json" | jq -r '.data // empty' 2>/dev/null)
            [[ -n "$text" ]] && echo "$text"
            ;;
        status)
            exit_code=$(echo "$json" | jq -r '.exit_code // empty' 2>/dev/null)
            echo
            if [[ "$exit_code" == "0" ]]; then
                ok "Task completed (exit 0)"
            else
                status=$(echo "$json" | jq -r '.status // empty' 2>/dev/null)
                err "Task finished: status=$status exit=$exit_code"
            fi
            break
            ;;
    esac
done

# --- Final status ----------------------------------------------------------

echo
info "Result"
api GET "/tasks/$TASK_ID" | jq '{
    id: .data.external_id,
    status: .data.status,
    exit_code: .data.exit_code,
    created: .data.created_at,
    started: .data.started_at,
    finished: .data.finished_at,
    error: .data.error
}'
