#!/usr/bin/env bash
#
# Integration tests against a live Airstore gateway via HTTP API.
# Runs I/O smoke tests and compression A/B regression tests.
# Outputs structured results to RESULTS_JSON.
#
# Usage:
#   AIRSTORE_WS_TOKEN=<token> bash e2e/integration_test.sh
#
# Environment:
#   AIRSTORE_WS_TOKEN           (required)  Workspace auth token
#   AIRSTORE_GATEWAY_HTTP       (optional)  HTTP API base, default https://api.airstore.ai
#   AIRSTORE_QUERY_PATH         (optional)  Source path, default /sources/gmail/unread-emails
#   COMPRESSION_MIN_REDUCTION   (optional)  Min avg % reduction, default 10
#   RESULTS_JSON                (optional)  Output path, default e2e/results.json
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TOKEN="${AIRSTORE_WS_TOKEN:-}"
GATEWAY_HTTP="${AIRSTORE_GATEWAY_HTTP:-https://api.airstore.ai}"
QUERY_PATH="${AIRSTORE_QUERY_PATH:-/sources/gmail/unread-emails}"
MIN_REDUCTION="${COMPRESSION_MIN_REDUCTION:-10}"
RESULTS="${RESULTS_JSON:-e2e/results.json}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

ERRORS=0
PASS_COUNT=0

pass() { echo -e "  ${GREEN}PASS${NC}  $1"; PASS_COUNT=$((PASS_COUNT+1)); }
fail() { echo -e "  ${RED}FAIL${NC}  $1"; ERRORS=$((ERRORS+1)); }
info() { echo -e "  ${YELLOW}....${NC}  $1"; }

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

if [ -z "$TOKEN" ]; then
    echo "ERROR: AIRSTORE_WS_TOKEN is required"
    exit 1
fi

# Resolve workspace ID from token via /auth/whoami
info "Resolving workspace..."
WHOAMI=$(curl -sf -H "Authorization: Bearer $TOKEN" "$GATEWAY_HTTP/api/v1/auth/whoami") || {
    echo "ERROR: Failed to resolve token (is the gateway reachable?)"
    exit 1
}
WORKSPACE=$(echo "$WHOAMI" | jq -r '.data.workspace_id // empty' 2>/dev/null)

if [ -z "$WORKSPACE" ]; then
    echo "ERROR: Token did not resolve to a workspace"
    echo "Response: $WHOAMI"
    exit 1
fi

BASE_URL="$GATEWAY_HTTP/api/v1/workspaces/$WORKSPACE"
echo "Workspace: $WORKSPACE"
echo "Gateway:   $GATEWAY_HTTP"
echo "Query:     $QUERY_PATH"
echo ""

# Percent-encode a string for safe use in URL query values.
# Preserves '/' since the server expects path separators.
url_encode() {
    python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1], safe='/'))" "$1"
}

api_get() {
    curl -sf -H "Authorization: Bearer $TOKEN" "$BASE_URL$1"
}

# api_get_with_headers performs a GET and writes the response body and headers
# to temp files. Avoids subshell so header values are accessible to the caller.
#   Usage: api_get_with_headers "/path" && BODY=$(resp_body)
_RESP_HEADERS_FILE=$(mktemp)
_RESP_BODY_FILE=$(mktemp)
trap 'rm -f "$_RESP_HEADERS_FILE" "$_RESP_BODY_FILE"' EXIT

api_get_with_headers() {
    : > "$_RESP_HEADERS_FILE"
    : > "$_RESP_BODY_FILE"
    if curl -sf -D "$_RESP_HEADERS_FILE" -o "$_RESP_BODY_FILE" \
        -H "Authorization: Bearer $TOKEN" "$BASE_URL$1"; then
        return 0
    else
        return $?
    fi
}

resp_body() { cat "$_RESP_BODY_FILE"; }

# Extract a header value from the last api_get_with_headers call.
header_val() {
    grep -i "^$1:" "$_RESP_HEADERS_FILE" 2>/dev/null | head -1 | sed 's/^[^:]*: *//' | tr -d '\r\n'
}

api_post() {
    curl -sf -X POST -H "Authorization: Bearer $TOKEN" "$BASE_URL$1"
}

# Millisecond timer (GNU date +%s%N on Linux, perl fallback for macOS)
now_ms() {
    local ns
    ns=$(date +%s%N 2>/dev/null)
    if [[ "$ns" =~ ^[0-9]+$ ]]; then
        echo $(( ns / 1000000 ))
    else
        perl -MTime::HiRes=time -e 'printf "%d\n", time()*1000'
    fi
}

# ---------------------------------------------------------------------------
# JSON result builder
# ---------------------------------------------------------------------------

IO_TESTS_JSON="[]"
COMP_FILES_JSON="[]"

add_io_test() {
    local name="$1" passed="$2" latency="$3" bytes="${4:-0}"
    IO_TESTS_JSON=$(echo "$IO_TESTS_JSON" | jq \
        --arg n "$name" --argjson p "$passed" --argjson l "$latency" --argjson b "$bytes" \
        '. + [{"name":$n,"passed":$p,"latency_ms":$l,"bytes":$b}]')
}

add_comp_file() {
    local name="$1" raw="$2" strip="$3" pct="$4" raw_lat="$5" strip_lat="$6" raw_tok="$7" strip_tok="$8"
    local tok_saved=$((raw_tok - strip_tok))
    COMP_FILES_JSON=$(echo "$COMP_FILES_JSON" | jq \
        --arg n "$name" --argjson r "$raw" --argjson s "$strip" \
        --argjson p "$pct" --argjson rl "$raw_lat" --argjson sl "$strip_lat" \
        --argjson rt "$raw_tok" --argjson st "$strip_tok" --argjson ts "$tok_saved" \
        '. + [{"name":$n,"raw_bytes":$r,"strip_bytes":$s,"reduction_pct":$p,"raw_latency_ms":$rl,"strip_latency_ms":$sl,"raw_tokens":$rt,"strip_tokens":$st,"tokens_saved":$ts}]')
}

# ============================================================================
# Phase 1: I/O Smoke Tests (via HTTP API)
# ============================================================================

echo "=== Phase 1: I/O Smoke Tests ==="
echo ""

IO_PASSED=true

# Test 1: List /sources/
info "GET /fs/list?path=/sources/ ..."
T0=$(now_ms)
if SOURCES_JSON=$(api_get "/fs/list?path=/sources/" 2>/dev/null); then
    T1=$(now_ms)
    ENTRIES=$(echo "$SOURCES_JSON" | jq '.data.entries | length' 2>/dev/null || echo 0)
    if [ "$ENTRIES" -gt 0 ]; then
        pass "list /sources/ ($ENTRIES entries, $((T1-T0))ms)"
        add_io_test "list_sources" true $((T1-T0))
    else
        fail "list /sources/ returned 0 entries"
        add_io_test "list_sources" false $((T1-T0))
        IO_PASSED=false
    fi
else
    T1=$(now_ms)
    fail "list /sources/ request failed"
    add_io_test "list_sources" false $((T1-T0))
    IO_PASSED=false
fi

# Test 2: List /sources/gmail/
info "GET /fs/list?path=/sources/gmail/ ..."
T0=$(now_ms)
if GMAIL_JSON=$(api_get "/fs/list?path=/sources/gmail/" 2>/dev/null); then
    T1=$(now_ms)
    ENTRIES=$(echo "$GMAIL_JSON" | jq '.data.entries | length' 2>/dev/null || echo 0)
    pass "list /sources/gmail/ ($ENTRIES entries, $((T1-T0))ms)"
    add_io_test "list_gmail" true $((T1-T0))
else
    T1=$(now_ms)
    fail "list /sources/gmail/ request failed"
    add_io_test "list_gmail" false $((T1-T0))
    IO_PASSED=false
fi

# Test 3: List query path (with refresh to ensure fresh smart query execution)
info "GET /fs/list?path=$QUERY_PATH&refresh=true ..."
T0=$(now_ms)
if LIST_JSON=$(api_get "/fs/list?path=$(url_encode "$QUERY_PATH")&refresh=true" 2>/dev/null); then
    T1=$(now_ms)
    FILE_COUNT=$(echo "$LIST_JSON" | jq '[.data.entries[] | select(.is_folder==false and (.name | startswith(".")|not))] | length' 2>/dev/null || echo 0)
    pass "list $QUERY_PATH ($FILE_COUNT files, $((T1-T0))ms)"
    add_io_test "list_query_path" true $((T1-T0))
else
    T1=$(now_ms)
    fail "list $QUERY_PATH request failed"
    add_io_test "list_query_path" false $((T1-T0))
    IO_PASSED=false
fi

# Test 4: Read first file
FILES=$(echo "$LIST_JSON" | jq -r '.data.entries[] | select(.is_folder==false and (.name | startswith(".")|not)) | .path' 2>/dev/null || true)
FIRST_FILE=$(echo "$FILES" | head -1)

if [ -n "$FIRST_FILE" ]; then
    FIRST_FNAME=$(basename "$FIRST_FILE")
    ENC_FIRST=$(url_encode "$FIRST_FILE")
    info "GET /fs/read?path=$FIRST_FILE ..."
    T0=$(now_ms)
    if READ_BODY=$(api_get "/fs/read?path=$ENC_FIRST" 2>/dev/null); then
        T1=$(now_ms)
        READ_BYTES=${#READ_BODY}
        if [ "$READ_BYTES" -gt 0 ]; then
            pass "read $FIRST_FNAME ($READ_BYTES bytes, $((T1-T0))ms)"
            add_io_test "read_file" true $((T1-T0)) "$READ_BYTES"
        else
            fail "read $FIRST_FNAME returned empty content"
            add_io_test "read_file" false $((T1-T0)) 0
            IO_PASSED=false
        fi
    else
        T1=$(now_ms)
        fail "read $FIRST_FNAME request failed"
        add_io_test "read_file" false $((T1-T0)) 0
        IO_PASSED=false
    fi

    # Test 5: Stat first file
    info "GET /fs/stat?path=$FIRST_FILE ..."
    T0=$(now_ms)
    if STAT_JSON=$(api_get "/fs/stat?path=$ENC_FIRST" 2>/dev/null); then
        T1=$(now_ms)
        STAT_SIZE=$(echo "$STAT_JSON" | jq '.data.size // 0' 2>/dev/null || echo 0)
        if [ "$STAT_SIZE" -gt 0 ]; then
            pass "stat $FIRST_FNAME (size=$STAT_SIZE, $((T1-T0))ms)"
            add_io_test "stat_file" true $((T1-T0)) "$STAT_SIZE"
        else
            pass "stat $FIRST_FNAME (size=0 — virtual file, $((T1-T0))ms)"
            add_io_test "stat_file" true $((T1-T0)) 0
        fi
    else
        T1=$(now_ms)
        fail "stat $FIRST_FNAME request failed"
        add_io_test "stat_file" false $((T1-T0)) 0
        IO_PASSED=false
    fi
else
    info "No files found to read/stat, skipping"
fi

echo ""

# ============================================================================
# Phase 2: Compression A/B Regression Tests
# ============================================================================

echo "=== Phase 2: Compression A/B Tests ==="
echo ""

# Flush the compression cache so every test shows actual compression work.
info "Flushing compression cache..."
if FLUSH_JSON=$(api_post "/cache/flush" 2>/dev/null); then
    KEYS_DEL=$(echo "$FLUSH_JSON" | jq '.data.keys_deleted // 0' 2>/dev/null || echo 0)
    pass "Compression cache flushed ($KEYS_DEL keys deleted)"
else
    info "Cache flush unavailable (non-fatal)"
fi

COMP_PASSED=true
CACHE_CONSISTENT=true
TOTAL_RAW=0
TOTAL_STRIP=0
TOTAL_RAW_TOK=0
TOTAL_STRIP_TOK=0
TOTAL_PCT=0

FILE_COUNT=$(echo "$FILES" | grep -c . 2>/dev/null || true)

if [ "$FILE_COUNT" -eq 0 ]; then
    info "No files found in $QUERY_PATH, skipping compression tests"
else
    pass "Testing $FILE_COUNT files"
    echo ""
    printf "  %-50s %8s %8s %8s %8s %6s %8s %8s\n" "FILE" "raw" "strip" "raw_tok" "str_tok" "red%" "raw_ms" "strip_ms"
    echo "  $(printf '%0.s-' {1..114})"

    while IFS= read -r FILE_PATH; do
        [ -z "$FILE_PATH" ] && continue
        FNAME=$(basename "$FILE_PATH")
        ENC_PATH=$(url_encode "$FILE_PATH")

        # Read raw
        T0=$(now_ms)
        RAW_BODY=$(api_get "/fs/read?path=$ENC_PATH" 2>/dev/null) || { fail "Raw read failed: $FNAME"; continue; }
        T1=$(now_ms)
        RAW_MS=$((T1-T0))
        RAW_BYTES=${#RAW_BODY}

        # Read with strip compression (capture headers for real token counts)
        T0=$(now_ms)
        if ! api_get_with_headers "/fs/read?path=$ENC_PATH&compression=strip" 2>/dev/null; then
            fail "Strip read failed: $FNAME"; continue
        fi
        STRIP_BODY=$(resp_body)
        T1=$(now_ms)
        STRIP_MS=$((T1-T0))
        STRIP_BYTES=${#STRIP_BODY}

        TOTAL_RAW=$((TOTAL_RAW + RAW_BYTES))
        TOTAL_STRIP=$((TOTAL_STRIP + STRIP_BYTES))

        # Use real token counts from server headers; fall back to estimate.
        HDR_RAW_TOK=$(header_val "X-Compression-Original-Tokens")
        HDR_STRIP_TOK=$(header_val "X-Compression-Compressed-Tokens")
        if [ -n "$HDR_RAW_TOK" ] && [ "$HDR_RAW_TOK" -gt 0 ] 2>/dev/null; then
            RAW_TOK=$HDR_RAW_TOK
            STRIP_TOK=${HDR_STRIP_TOK:-0}
        else
            # Fallback estimate (~4 bytes/token for cl100k_base)
            RAW_TOK=$((RAW_BYTES / 4))
            STRIP_TOK=$((STRIP_BYTES / 4))
        fi
        TOTAL_RAW_TOK=$((TOTAL_RAW_TOK + RAW_TOK))
        TOTAL_STRIP_TOK=$((TOTAL_STRIP_TOK + STRIP_TOK))

        if [ "$RAW_BYTES" -gt 0 ]; then
            PCT=$(( (RAW_BYTES - STRIP_BYTES) * 100 / RAW_BYTES ))
        else
            PCT=0
        fi

        # Check for inflation
        if [ "$STRIP_BYTES" -gt "$RAW_BYTES" ]; then
            printf "  ${RED}%-50s %8d %8d %8d %8d %5d%% %8d %8d  INFLATED${NC}\n" \
                "${FNAME:0:50}" "$RAW_BYTES" "$STRIP_BYTES" "$RAW_TOK" "$STRIP_TOK" "$PCT" "$RAW_MS" "$STRIP_MS"
            COMP_PASSED=false
            ERRORS=$((ERRORS+1))
        else
            printf "  %-50s %8d %8d %8d %8d %5d%% %8d %8d\n" \
                "${FNAME:0:50}" "$RAW_BYTES" "$STRIP_BYTES" "$RAW_TOK" "$STRIP_TOK" "$PCT" "$RAW_MS" "$STRIP_MS"
        fi

        add_comp_file "$FNAME" "$RAW_BYTES" "$STRIP_BYTES" "$PCT" "$RAW_MS" "$STRIP_MS" "$RAW_TOK" "$STRIP_TOK"
    done <<< "$FILES"

    echo "  $(printf '%0.s-' {1..96})"

    if [ "$TOTAL_RAW" -gt 0 ]; then
        TOTAL_PCT=$(( (TOTAL_RAW - TOTAL_STRIP) * 100 / TOTAL_RAW ))
        printf "  %-50s %8d %8d %5d%%\n" "TOTAL" "$TOTAL_RAW" "$TOTAL_STRIP" "$TOTAL_PCT"
    fi
    echo ""

    # Check minimum reduction threshold
    if [ "$TOTAL_RAW" -gt 0 ] && [ "$TOTAL_PCT" -lt "$MIN_REDUCTION" ]; then
        fail "Average reduction ${TOTAL_PCT}% is below minimum ${MIN_REDUCTION}%"
        COMP_PASSED=false
    else
        pass "Average reduction: ${TOTAL_PCT}% (threshold: ${MIN_REDUCTION}%)"
    fi

    # ---------------------------------------------------------------
    # Cache consistency: read same file 3 times with strip
    # ---------------------------------------------------------------
    if [ -n "$FIRST_FILE" ]; then
        ENC_FIRST=${ENC_FIRST:-$(url_encode "$FIRST_FILE")}
        info "Cache consistency check on $(basename "$FIRST_FILE")..."
        R1=$(api_get "/fs/read?path=$ENC_FIRST&compression=strip")
        sleep 1
        R2=$(api_get "/fs/read?path=$ENC_FIRST&compression=strip")
        R3=$(api_get "/fs/read?path=$ENC_FIRST&compression=strip")

        if [ "$R1" = "$R2" ] && [ "$R2" = "$R3" ]; then
            pass "Cache consistent (3 reads identical)"
        else
            fail "Cache inconsistency: reads returned different content"
            CACHE_CONSISTENT=false
            COMP_PASSED=false
        fi
    fi
fi

echo ""

# ============================================================================
# Write results.json
# ============================================================================

mkdir -p "$(dirname "$RESULTS")"

jq -n \
    --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg gw "$GATEWAY_HTTP" \
    --arg qp "$QUERY_PATH" \
    --argjson io_passed "$IO_PASSED" \
    --argjson io_tests "$IO_TESTS_JSON" \
    --argjson comp_passed "$COMP_PASSED" \
    --argjson min_red "$MIN_REDUCTION" \
    --argjson avg_red "$TOTAL_PCT" \
    --argjson total_raw "$TOTAL_RAW" \
    --argjson total_strip "$TOTAL_STRIP" \
    --argjson total_raw_tok "$TOTAL_RAW_TOK" \
    --argjson total_strip_tok "$TOTAL_STRIP_TOK" \
    --argjson cache "$CACHE_CONSISTENT" \
    --argjson comp_files "$COMP_FILES_JSON" \
    '{
        timestamp: $ts,
        gateway: $gw,
        query_path: $qp,
        io_tests: {
            passed: $io_passed,
            tests: $io_tests
        },
        compression: {
            passed: $comp_passed,
            min_reduction_pct: $min_red,
            avg_reduction_pct: $avg_red,
            total_raw_bytes: $total_raw,
            total_strip_bytes: $total_strip,
            total_raw_tokens: $total_raw_tok,
            total_strip_tokens: $total_strip_tok,
            cache_consistent: $cache,
            files: $comp_files
        }
    }' > "$RESULTS"

echo "Results written to $RESULTS"
echo ""

# ============================================================================
# Summary
# ============================================================================

if [ "$ERRORS" -gt 0 ]; then
    echo -e "${RED}FAILED${NC}: $ERRORS errors, $PASS_COUNT passed"
    exit 1
else
    echo -e "${GREEN}ALL PASSED${NC}: $PASS_COUNT tests"
    exit 0
fi
