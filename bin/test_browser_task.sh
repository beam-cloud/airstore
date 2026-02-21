#!/bin/bash
set -euo pipefail

#
# End-to-end browser automation test.
#
# Reads skills/browser-test/SKILL.md, creates a task via the REST API,
# and streams logs until completion.
#
# Usage:
#   ./bin/test_browser_task.sh                          # defaults
#   ./bin/test_browser_task.sh --workspace myworkspace  # explicit workspace
#   GATEWAY_HTTP=http://host:1994 ./bin/test_browser_task.sh
#
# Environment:
#   GATEWAY_HTTP   Gateway HTTP address   (default: http://localhost:1994)
#   AIRSTORE_TOKEN Auth token             (default: airstore-dev-admin-token)
#   WORKSPACE_NAME Workspace name         (auto-detected if empty)
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
SKILL_FILE="$ROOT_DIR/skills/browser-test/SKILL.md"

GATEWAY_HTTP="${GATEWAY_HTTP:-http://localhost:1994}"
TOKEN="${AIRSTORE_TOKEN:-airstore-dev-admin-token}"
WORKSPACE_NAME="${WORKSPACE_NAME:-}"

# --------------------------------------------------------------------------
# Parse flags
# --------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --workspace|-w) WORKSPACE_NAME="$2"; shift 2 ;;
        --gateway|-g)   GATEWAY_HTTP="$2"; shift 2 ;;
        --token|-t)     TOKEN="$2"; shift 2 ;;
        --help|-h)
            sed -n '3,/^$/p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
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

# --------------------------------------------------------------------------
# Preflight
# --------------------------------------------------------------------------
info "Preflight checks"

if ! command -v jq &>/dev/null; then
    err "jq is required"; exit 1
fi
ok "jq found"

if ! curl -sf "$GATEWAY_HTTP/api/v1/health" &>/dev/null; then
    err "Gateway unreachable at $GATEWAY_HTTP"
    dim "Start with: okteto up  or  ./bin/airstore mount"
    exit 1
fi
ok "Gateway reachable at $GATEWAY_HTTP"

if [[ ! -f "$SKILL_FILE" ]]; then
    err "Skill file not found: $SKILL_FILE"
    exit 1
fi
ok "Skill file: $SKILL_FILE"

# --------------------------------------------------------------------------
# Resolve workspace
# --------------------------------------------------------------------------
if [[ -z "$WORKSPACE_NAME" ]]; then
    WORKSPACE_NAME=$(api GET /workspaces | jq -r '.data[0].name // empty')
fi
if [[ -z "$WORKSPACE_NAME" ]]; then
    err "No workspaces found. Create one first."
    exit 1
fi
ok "Workspace: $WORKSPACE_NAME"

# --------------------------------------------------------------------------
# Extract prompt from SKILL.md (everything after the closing ---)
# --------------------------------------------------------------------------
info "Reading skill"

PROMPT=$(awk '
    BEGIN { fence=0 }
    /^---$/ { fence++; next }
    fence >= 2 { print }
' "$SKILL_FILE")

if [[ -z "$PROMPT" ]]; then
    err "No instructions found in SKILL.md"
    exit 1
fi

PROMPT_LINES=$(echo "$PROMPT" | wc -l | tr -d ' ')
ok "Extracted prompt ($PROMPT_LINES lines)"

# --------------------------------------------------------------------------
# Create task
# --------------------------------------------------------------------------
info "Creating task"

PAYLOAD=$(jq -n \
    --arg ws "$WORKSPACE_NAME" \
    --arg prompt "$PROMPT" \
    '{workspace_name: $ws, prompt: $prompt}')

RESPONSE=$(api POST /tasks -d "$PAYLOAD")
TASK_ID=$(echo "$RESPONSE" | jq -r '.data.external_id')
STATUS=$(echo "$RESPONSE" | jq -r '.data.status')

if [[ -z "$TASK_ID" || "$TASK_ID" == "null" ]]; then
    err "Failed to create task"
    echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
    exit 1
fi

ok "Task $TASK_ID ($STATUS)"

# --------------------------------------------------------------------------
# Stream logs via SSE
# --------------------------------------------------------------------------
info "Streaming logs (Ctrl-C to detach)"
dim "Task: $TASK_ID"
dim "Logs: $GATEWAY_HTTP/api/v1/tasks/$TASK_ID/logs/stream"
echo ""

# Stream SSE, parsing "data:" lines. Print log text, stop on status event.
curl -sN "$GATEWAY_HTTP/api/v1/tasks/$TASK_ID/logs/stream" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: text/event-stream" 2>/dev/null | while IFS= read -r line; do

    # SSE data lines start with "data: "
    if [[ "$line" == data:* ]]; then
        json="${line#data: }"

        type=$(echo "$json" | jq -r '.type // empty' 2>/dev/null)

        case "$type" in
            log)
                text=$(echo "$json" | jq -r '.data // empty' 2>/dev/null)
                if [[ -n "$text" ]]; then
                    echo "$text"
                fi
                ;;
            status)
                status=$(echo "$json" | jq -r '.status // empty' 2>/dev/null)
                exit_code=$(echo "$json" | jq -r '.exit_code // empty' 2>/dev/null)
                echo ""
                if [[ "$exit_code" == "0" ]]; then
                    ok "Task completed (exit $exit_code)"
                else
                    err "Task finished with status=$status exit=$exit_code"
                fi
                break
                ;;
        esac
    fi
done

# --------------------------------------------------------------------------
# Final status
# --------------------------------------------------------------------------
echo ""
info "Task result"
FINAL=$(api GET "/tasks/$TASK_ID")
echo "$FINAL" | jq '{
    id: .data.external_id,
    status: .data.status,
    exit_code: .data.exit_code,
    created: .data.created_at,
    started: .data.started_at,
    finished: .data.finished_at,
    error: .data.error
}'
