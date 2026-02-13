#!/usr/bin/env bash
#
# Integration tests against a live Airstore gateway.
# Runs I/O smoke tests via the NFS mount and compression A/B regression
# tests via the HTTP API. Outputs structured results to RESULTS_JSON.
#
# Usage:
#   AIRSTORE_API_KEY=<token> bash e2e/integration_test.sh
#
# Environment:
#   AIRSTORE_API_KEY            (required)  Workspace auth token
#   AIRSTORE_GATEWAY_HTTP       (optional)  HTTP API base, default https://api.airstore.ai
#   AIRSTORE_MOUNT              (optional)  Mount point, default /tmp/airstore
#   AIRSTORE_QUERY_PATH         (optional)  Source path, default /sources/gmail/unread-emails
#   COMPRESSION_MIN_REDUCTION   (optional)  Min avg % reduction, default 10
#   RESULTS_JSON                (optional)  Output path, default e2e/results.json
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TOKEN="${AIRSTORE_API_KEY:-}"
GATEWAY_HTTP="${AIRSTORE_GATEWAY_HTTP:-https://api.airstore.ai}"
MOUNT="${AIRSTORE_MOUNT:-/tmp/airstore}"
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
    echo "ERROR: AIRSTORE_API_KEY is required"
    exit 1
fi

# Resolve workspace ID from token
WORKSPACE=$(curl -sf -H "Authorization: Bearer $TOKEN" "$GATEWAY_HTTP/api/v1/workspaces" \
    | jq -r '.data[0].external_id // .data[0].id // empty' 2>/dev/null)

if [ -z "$WORKSPACE" ]; then
    echo "ERROR: Could not resolve workspace ID from token"
    exit 1
fi
echo "Workspace: $WORKSPACE"
echo "Gateway:   $GATEWAY_HTTP"
echo "Mount:     $MOUNT"
echo "Query:     $QUERY_PATH"
echo ""

api_get() {
    curl -sf -H "Authorization: Bearer $TOKEN" "$GATEWAY_HTTP/api/v1/workspaces/$WORKSPACE$1"
}

# Millisecond timer helper (uses date +%s%N on linux, perl fallback on mac)
now_ms() {
    if date +%s%N >/dev/null 2>&1 && [ "$(date +%s%N)" != "%s%N" ]; then
        echo $(( $(date +%s%N) / 1000000 ))
    else
        perl -MTime::HiRes=time -e 'printf "%d\n", time()*1000'
    fi
}

# ---------------------------------------------------------------------------
# JSON result builder
# ---------------------------------------------------------------------------

# We build the JSON incrementally using temp files
IO_TESTS_JSON="[]"
COMP_FILES_JSON="[]"

add_io_test() {
    local name="$1" passed="$2" latency="$3" bytes="${4:-0}"
    IO_TESTS_JSON=$(echo "$IO_TESTS_JSON" | jq \
        --arg n "$name" --argjson p "$passed" --argjson l "$latency" --argjson b "$bytes" \
        '. + [{"name":$n,"passed":$p,"latency_ms":$l,"bytes":$b}]')
}

add_comp_file() {
    local name="$1" raw="$2" strip="$3" pct="$4" raw_lat="$5" strip_lat="$6"
    COMP_FILES_JSON=$(echo "$COMP_FILES_JSON" | jq \
        --arg n "$name" --argjson r "$raw" --argjson s "$strip" \
        --argjson p "$pct" --argjson rl "$raw_lat" --argjson sl "$strip_lat" \
        '. + [{"name":$n,"raw_bytes":$r,"strip_bytes":$s,"reduction_pct":$p,"raw_latency_ms":$rl,"strip_latency_ms":$sl}]')
}

# ============================================================================
# Phase 1: I/O Smoke Tests
# ============================================================================

echo "=== Phase 1: I/O Smoke Tests ==="
echo ""

IO_PASSED=true

# Test 1: ls /sources/
info "ls sources..."
T0=$(now_ms)
if ls "$MOUNT/sources/" >/dev/null 2>&1; then
    T1=$(now_ms)
    ENTRIES=$(ls "$MOUNT/sources/" | wc -l)
    if [ "$ENTRIES" -gt 0 ]; then
        pass "ls /sources/ ($ENTRIES entries, $((T1-T0))ms)"
        add_io_test "ls_sources" true $((T1-T0))
    else
        fail "ls /sources/ returned 0 entries"
        add_io_test "ls_sources" false $((T1-T0))
        IO_PASSED=false
    fi
else
    T1=$(now_ms)
    fail "ls /sources/ failed"
    add_io_test "ls_sources" false $((T1-T0))
    IO_PASSED=false
fi

# Test 2: ls /sources/gmail/
info "ls gmail..."
T0=$(now_ms)
if ls "$MOUNT/sources/gmail/" >/dev/null 2>&1; then
    T1=$(now_ms)
    pass "ls /sources/gmail/ ($((T1-T0))ms)"
    add_io_test "ls_gmail" true $((T1-T0))
else
    T1=$(now_ms)
    fail "ls /sources/gmail/ failed"
    add_io_test "ls_gmail" false $((T1-T0))
    IO_PASSED=false
fi

# Test 3: ls query path
info "ls query path..."
T0=$(now_ms)
QUERY_MOUNT_PATH="$MOUNT$QUERY_PATH"
if ls "$QUERY_MOUNT_PATH/" >/dev/null 2>&1; then
    T1=$(now_ms)
    FILE_LIST=$(ls "$QUERY_MOUNT_PATH/" 2>/dev/null | head -20)
    FILE_COUNT=$(echo "$FILE_LIST" | grep -c . || true)
    pass "ls $QUERY_PATH ($FILE_COUNT files, $((T1-T0))ms)"
    add_io_test "ls_query_path" true $((T1-T0))
else
    T1=$(now_ms)
    fail "ls $QUERY_PATH failed"
    add_io_test "ls_query_path" false $((T1-T0))
    IO_PASSED=false
fi

# Test 4: cat first file
FIRST_FILE=$(ls "$QUERY_MOUNT_PATH/" 2>/dev/null | grep -v '^\.' | head -1 || true)
if [ -n "$FIRST_FILE" ]; then
    info "cat $FIRST_FILE..."
    T0=$(now_ms)
    CONTENT=$(cat "$QUERY_MOUNT_PATH/$FIRST_FILE" 2>/dev/null) || true
    T1=$(now_ms)
    CONTENT_BYTES=${#CONTENT}
    if [ "$CONTENT_BYTES" -gt 0 ]; then
        pass "cat $FIRST_FILE ($CONTENT_BYTES bytes, $((T1-T0))ms)"
        add_io_test "cat_file" true $((T1-T0)) "$CONTENT_BYTES"
    else
        fail "cat $FIRST_FILE returned empty content"
        add_io_test "cat_file" false $((T1-T0)) 0
        IO_PASSED=false
    fi

    # Test 5: stat first file
    info "stat $FIRST_FILE..."
    T0=$(now_ms)
    if STAT_SIZE=$(stat -c%s "$QUERY_MOUNT_PATH/$FIRST_FILE" 2>/dev/null || stat -f%z "$QUERY_MOUNT_PATH/$FIRST_FILE" 2>/dev/null); then
        T1=$(now_ms)
        if [ "$STAT_SIZE" -gt 0 ]; then
            pass "stat $FIRST_FILE (size=$STAT_SIZE, $((T1-T0))ms)"
            add_io_test "stat_file" true $((T1-T0)) "$STAT_SIZE"
        else
            fail "stat $FIRST_FILE returned size 0"
            add_io_test "stat_file" false $((T1-T0)) 0
            IO_PASSED=false
        fi
    else
        T1=$(now_ms)
        fail "stat $FIRST_FILE failed"
        add_io_test "stat_file" false $((T1-T0)) 0
        IO_PASSED=false
    fi
else
    info "No files found to cat/stat, skipping"
fi

echo ""

# ============================================================================
# Phase 2: Compression A/B Regression Tests
# ============================================================================

echo "=== Phase 2: Compression A/B Tests ==="
echo ""

COMP_PASSED=true
CACHE_CONSISTENT=true
TOTAL_RAW=0
TOTAL_STRIP=0

# List files via API
info "Listing files in $QUERY_PATH..."
LIST_JSON=$(api_get "/fs/list?path=$QUERY_PATH") || { fail "List request failed"; COMP_PASSED=false; }

if [ "$COMP_PASSED" = true ]; then
    FILES=$(echo "$LIST_JSON" | jq -r '.data.entries[] | select(.is_folder==false and (.name | startswith(".")|not)) | .path' 2>/dev/null)
    FILE_COUNT=$(echo "$FILES" | grep -c . || true)

    if [ "$FILE_COUNT" -eq 0 ]; then
        info "No files found in $QUERY_PATH, skipping compression tests"
    else
        pass "Found $FILE_COUNT files"
        echo ""
        printf "  %-50s %8s %8s %6s %8s %8s\n" "FILE" "raw" "strip" "red%" "raw_ms" "strip_ms"
        echo "  $(printf '%0.s-' {1..96})"

        for FILE_PATH in $FILES; do
            FNAME=$(basename "$FILE_PATH")

            # Read raw
            T0=$(now_ms)
            RAW_BODY=$(api_get "/fs/read?path=$FILE_PATH" 2>/dev/null) || { fail "Raw read failed: $FNAME"; continue; }
            T1=$(now_ms)
            RAW_MS=$((T1-T0))
            RAW_BYTES=${#RAW_BODY}

            # Read with strip compression
            T0=$(now_ms)
            STRIP_BODY=$(api_get "/fs/read?path=$FILE_PATH&compression=strip" 2>/dev/null) || { fail "Strip read failed: $FNAME"; continue; }
            T1=$(now_ms)
            STRIP_MS=$((T1-T0))
            STRIP_BYTES=${#STRIP_BODY}

            TOTAL_RAW=$((TOTAL_RAW + RAW_BYTES))
            TOTAL_STRIP=$((TOTAL_STRIP + STRIP_BYTES))

            if [ "$RAW_BYTES" -gt 0 ]; then
                PCT=$(( (RAW_BYTES - STRIP_BYTES) * 100 / RAW_BYTES ))
            else
                PCT=0
            fi

            # Check for inflation
            if [ "$STRIP_BYTES" -gt "$RAW_BYTES" ]; then
                printf "  ${RED}%-50s %8d %8d %5d%% %8d %8d  INFLATED${NC}\n" \
                    "${FNAME:0:50}" "$RAW_BYTES" "$STRIP_BYTES" "$PCT" "$RAW_MS" "$STRIP_MS"
                COMP_PASSED=false
                ERRORS=$((ERRORS+1))
            else
                printf "  %-50s %8d %8d %5d%% %8d %8d\n" \
                    "${FNAME:0:50}" "$RAW_BYTES" "$STRIP_BYTES" "$PCT" "$RAW_MS" "$STRIP_MS"
            fi

            add_comp_file "$FNAME" "$RAW_BYTES" "$STRIP_BYTES" "$PCT" "$RAW_MS" "$STRIP_MS"
        done

        echo "  $(printf '%0.s-' {1..96})"

        if [ "$TOTAL_RAW" -gt 0 ]; then
            TOTAL_PCT=$(( (TOTAL_RAW - TOTAL_STRIP) * 100 / TOTAL_RAW ))
            printf "  %-50s %8d %8d %5d%%\n" "TOTAL" "$TOTAL_RAW" "$TOTAL_STRIP" "$TOTAL_PCT"
        else
            TOTAL_PCT=0
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
        FIRST_FILE=$(echo "$FILES" | head -1)
        if [ -n "$FIRST_FILE" ]; then
            info "Cache consistency check on $(basename "$FIRST_FILE")..."
            R1=$(api_get "/fs/read?path=$FIRST_FILE&compression=strip")
            sleep 1
            R2=$(api_get "/fs/read?path=$FIRST_FILE&compression=strip")
            R3=$(api_get "/fs/read?path=$FIRST_FILE&compression=strip")

            if [ "$R1" = "$R2" ] && [ "$R2" = "$R3" ]; then
                pass "Cache consistent (3 reads identical)"
            else
                fail "Cache inconsistency: reads returned different content"
                CACHE_CONSISTENT=false
                COMP_PASSED=false
            fi
        fi
    fi
fi

echo ""

# ============================================================================
# Write results.json
# ============================================================================

mkdir -p "$(dirname "$RESULTS")"

AVG_PCT=${TOTAL_PCT:-0}

jq -n \
    --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg gw "$GATEWAY_HTTP" \
    --arg qp "$QUERY_PATH" \
    --argjson io_passed "$IO_PASSED" \
    --argjson io_tests "$IO_TESTS_JSON" \
    --argjson comp_passed "$COMP_PASSED" \
    --argjson min_red "$MIN_REDUCTION" \
    --argjson avg_red "$AVG_PCT" \
    --argjson total_raw "$TOTAL_RAW" \
    --argjson total_strip "$TOTAL_STRIP" \
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
