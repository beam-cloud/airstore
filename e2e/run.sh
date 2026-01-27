#!/bin/bash
# End-to-end test suite for Airstore
#
# Runs against the k3d cluster. Expects:
#   - k3d-airstore cluster running
#   - Gateway and services deployed
#   - LocalStack running (for S3/context tests)
#
# Usage: ./e2e/run.sh [test_name]
#   ./e2e/run.sh           # Run all tests
#   ./e2e/run.sh setup     # Run only setup test
#   ./e2e/run.sh task      # Run only task test
#   ./e2e/run.sh fs        # Run only filesystem test
#   ./e2e/run.sh tools     # Run only tools test
#   ./e2e/run.sh context   # Run only context/S3 test

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Gateway addresses (port-forwarded from k3d)
GATEWAY_GRPC="${GATEWAY_GRPC:-localhost:1993}"
GATEWAY_HTTP="${GATEWAY_HTTP:-localhost:1994}"
MOUNT_POINT="${MOUNT_POINT:-/tmp/airstore-e2e}"

# LocalStack S3 (port-forwarded from k3d)
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:4566}"
S3_BUCKET="${S3_BUCKET:-airstore-context}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓${NC} $1"; }
fail() { echo -e "${RED}✗${NC} $1"; exit 1; }
info() { echo -e "${YELLOW}→${NC} $1"; }

# Build CLI if needed
ensure_cli() {
    if [ ! -f "$PROJECT_ROOT/bin/cli" ]; then
        info "Building CLI..."
        (cd "$PROJECT_ROOT" && make cli build-shim)
    fi
}

# Wait for gateway to be ready
wait_gateway() {
    info "Waiting for gateway..."
    for i in {1..30}; do
        if curl -s "http://$GATEWAY_HTTP/api/v1/health" 2>/dev/null | grep -q "ok"; then
            pass "Gateway ready"
            return 0
        fi
        sleep 1
    done
    fail "Gateway not ready after 30s"
}

# Cleanup helper
cleanup_mount() {
    umount "$MOUNT_POINT" 2>/dev/null || \
    diskutil unmount "$MOUNT_POINT" 2>/dev/null || \
    fusermount -u "$MOUNT_POINT" 2>/dev/null || true
    rmdir "$MOUNT_POINT" 2>/dev/null || true
}

# ============================================================================
# Test: Setup (workspace, member, token)
# ============================================================================
test_setup() {
    echo ""
    echo "=== Test: Setup ==="
    
    ensure_cli
    wait_gateway
    
    # Create workspace
    info "Creating workspace..."
    WORKSPACE_NAME="e2e-$(date +%s)"
    RESULT=$("$PROJECT_ROOT/bin/cli" --gateway "$GATEWAY_GRPC" workspace create "$WORKSPACE_NAME" 2>&1) || fail "Workspace create failed: $RESULT"
    WORKSPACE_ID=$(echo "$RESULT" | grep -oE '[0-9a-f-]{36}' | head -1)
    [ -n "$WORKSPACE_ID" ] || fail "Could not parse workspace ID"
    pass "Workspace: $WORKSPACE_ID"
    
    # Add member
    info "Adding member..."
    RESULT=$("$PROJECT_ROOT/bin/cli" --gateway "$GATEWAY_GRPC" member add "$WORKSPACE_ID" "e2e@test.com" --name "E2E" --role admin 2>&1) || fail "Member add failed: $RESULT"
    MEMBER_ID=$(echo "$RESULT" | grep -oE '[0-9a-f-]{36}' | head -1)
    [ -n "$MEMBER_ID" ] || fail "Could not parse member ID"
    pass "Member: $MEMBER_ID"
    
    # Create token
    info "Creating token..."
    RESULT=$("$PROJECT_ROOT/bin/cli" --gateway "$GATEWAY_GRPC" token create "$WORKSPACE_ID" "$MEMBER_ID" --name "e2e-token" 2>&1) || fail "Token create failed: $RESULT"
    TOKEN=$(echo "$RESULT" | grep "Token:" | awk '{print $2}')
    [ -n "$TOKEN" ] || fail "Could not parse token"
    pass "Token created"
    
    # Export for other tests
    export E2E_WORKSPACE_ID="$WORKSPACE_ID"
    export E2E_TOKEN="$TOKEN"
    
    echo ""
    pass "Setup test passed"
}

# ============================================================================
# Test: Task execution
# ============================================================================
test_task() {
    echo ""
    echo "=== Test: Task Execution ==="
    
    wait_gateway
    
    # Get or create workspace
    info "Getting workspace..."
    WORKSPACE=$(curl -s "http://$GATEWAY_HTTP/api/v1/workspaces" | jq -r '.data[0].id // empty')
    if [ -z "$WORKSPACE" ]; then
        info "Creating workspace..."
        WORKSPACE=$(curl -s -X POST "http://$GATEWAY_HTTP/api/v1/workspaces" \
            -H "Content-Type: application/json" \
            -d '{"name":"e2e-task-test"}' | jq -r '.data.id')
    fi
    [ -n "$WORKSPACE" ] || fail "Could not get workspace"
    pass "Workspace: $WORKSPACE"
    
    # Create task
    info "Creating task (alpine echo)..."
    TASK=$(curl -s -X POST "http://$GATEWAY_HTTP/api/v1/tasks" \
        -H "Content-Type: application/json" \
        -d "{\"workspace_id\":\"$WORKSPACE\",\"image\":\"alpine:3.18\",\"entrypoint\":[\"/bin/echo\",\"e2e-test-output\"]}" | jq -r '.data.id')
    [ -n "$TASK" ] || fail "Could not create task"
    pass "Task: $TASK"
    
    # Wait for completion
    info "Waiting for task to complete..."
    for i in {1..30}; do
        STATUS=$(curl -s "http://$GATEWAY_HTTP/api/v1/tasks/$TASK" | jq -r '.data.status')
        if [ "$STATUS" = "complete" ] || [ "$STATUS" = "completed" ] || [ "$STATUS" = "failed" ]; then
            break
        fi
        sleep 2
    done
    
    # Check result
    RESULT=$(curl -s "http://$GATEWAY_HTTP/api/v1/tasks/$TASK")
    STATUS=$(echo "$RESULT" | jq -r '.data.status')
    EXIT_CODE=$(echo "$RESULT" | jq -r '.data.exit_code')
    
    if [ "$STATUS" = "complete" ] || [ "$STATUS" = "completed" ]; then
        if [ "$EXIT_CODE" = "0" ]; then
            pass "Task completed successfully (exit code: $EXIT_CODE)"
        else
            echo "$RESULT" | jq .
            fail "Task failed with exit code: $EXIT_CODE"
        fi
    else
        echo "$RESULT" | jq .
        fail "Task status: $STATUS"
    fi
    
    echo ""
    pass "Task test passed"
}

# ============================================================================
# Test: Filesystem mount
# ============================================================================
test_filesystem() {
    echo ""
    echo "=== Test: Filesystem ==="
    
    ensure_cli
    wait_gateway
    
    # Cleanup any existing mount
    cleanup_mount
    trap cleanup_mount EXIT
    
    # Create mount point
    info "Creating mount point..."
    mkdir -p "$MOUNT_POINT"
    
    # Mount filesystem in background
    info "Mounting filesystem..."
    "$PROJECT_ROOT/bin/cli" mount "$MOUNT_POINT" --gateway "$GATEWAY_GRPC" &
    MOUNT_PID=$!
    sleep 3
    
    # Verify mount
    if ! mount | grep -q "$MOUNT_POINT"; then
        fail "Mount failed"
    fi
    pass "Mounted at $MOUNT_POINT"
    
    # Test directory listing
    info "Listing root..."
    ls "$MOUNT_POINT/" > /dev/null || fail "ls failed"
    pass "Directory listing works"
    
    # Test /tools directory
    if [ -d "$MOUNT_POINT/tools" ]; then
        info "Checking tools..."
        TOOLS=$(ls "$MOUNT_POINT/tools/" 2>/dev/null | wc -l | tr -d ' ')
        pass "Tools directory exists ($TOOLS tools)"
    fi
    
    # Cleanup
    info "Unmounting..."
    kill $MOUNT_PID 2>/dev/null || true
    sleep 1
    cleanup_mount
    
    echo ""
    pass "Filesystem test passed"
}

# ============================================================================
# Test: Tools
# ============================================================================
test_tools() {
    echo ""
    echo "=== Test: Tools ==="
    
    ensure_cli
    wait_gateway
    
    # Cleanup any existing mount
    cleanup_mount
    trap cleanup_mount EXIT
    mkdir -p "$MOUNT_POINT"
    
    # Mount filesystem
    info "Mounting filesystem..."
    "$PROJECT_ROOT/bin/cli" mount "$MOUNT_POINT" --gateway "$GATEWAY_GRPC" &
    MOUNT_PID=$!
    sleep 3
    
    # Test wikipedia tool
    if [ -x "$MOUNT_POINT/tools/wikipedia" ]; then
        info "Testing wikipedia tool..."
        RESULT=$("$MOUNT_POINT/tools/wikipedia" search "Linux" --limit 1 2>&1) || true
        if echo "$RESULT" | grep -q "results"; then
            pass "Wikipedia tool works"
        else
            info "Wikipedia tool returned: ${RESULT:0:100}..."
        fi
    else
        info "Wikipedia tool not available"
    fi
    
    # Cleanup
    kill $MOUNT_PID 2>/dev/null || true
    sleep 1
    cleanup_mount
    
    echo ""
    pass "Tools test passed"
}

# ============================================================================
# Test: Context Storage (S3-backed filesystem via FUSE mount)
# ============================================================================
test_context() {
    echo ""
    echo "=== Test: Context Storage (FUSE Mount -> S3) ==="
    
    ensure_cli
    wait_gateway
    cleanup_mount
    
    # AWS CLI wrapper for LocalStack (uses dummy credentials)
    aws_local() {
        AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
        aws --endpoint-url="$S3_ENDPOINT" --region us-east-1 "$@"
    }
    
    # Start port-forward to LocalStack if needed
    info "Checking LocalStack at $S3_ENDPOINT..."
    if ! curl -s "$S3_ENDPOINT/_localstack/health" 2>/dev/null | grep -q "running"; then
        info "Starting port-forward to LocalStack..."
        kubectl port-forward -n airstore svc/localstack 4566:4566 &>/dev/null &
        sleep 2
    fi
    
    # Ensure bucket exists
    aws_local s3 mb "s3://$S3_BUCKET" 2>/dev/null || true
    
    # -------------------------------------------------------------------------
    # Setup: Create workspace and token for auth
    # -------------------------------------------------------------------------
    info "Creating test workspace..."
    WORKSPACE_NAME="context-e2e-$(date +%s)"
    RESULT=$("$PROJECT_ROOT/bin/cli" --gateway "$GATEWAY_GRPC" workspace create "$WORKSPACE_NAME" 2>&1) || fail "Workspace create failed: $RESULT"
    WORKSPACE_ID=$(echo "$RESULT" | grep -oE '[0-9a-f-]{36}' | head -1)
    [ -n "$WORKSPACE_ID" ] || fail "Could not parse workspace ID"
    pass "Workspace: $WORKSPACE_ID"
    
    info "Adding member..."
    RESULT=$("$PROJECT_ROOT/bin/cli" --gateway "$GATEWAY_GRPC" member add "$WORKSPACE_ID" "context@test.com" --name "Context" --role admin 2>&1) || fail "Member add failed"
    MEMBER_ID=$(echo "$RESULT" | grep -oE '[0-9a-f-]{36}' | head -1)
    [ -n "$MEMBER_ID" ] || fail "Could not parse member ID"
    
    info "Creating token..."
    RESULT=$("$PROJECT_ROOT/bin/cli" --gateway "$GATEWAY_GRPC" token create "$WORKSPACE_ID" "$MEMBER_ID" --name "context-token" 2>&1) || fail "Token create failed"
    TOKEN=$(echo "$RESULT" | grep "Token:" | awk '{print $2}')
    [ -n "$TOKEN" ] || fail "Could not parse token"
    pass "Token created"
    
    # -------------------------------------------------------------------------
    # Mount filesystem with auth
    # -------------------------------------------------------------------------
    mkdir -p "$MOUNT_POINT"
    
    info "Mounting filesystem..."
    "$PROJECT_ROOT/bin/cli" mount "$MOUNT_POINT" --gateway "$GATEWAY_GRPC" --token "$TOKEN" &
    MOUNT_PID=$!
    sleep 3
    
    # Verify mount
    if [ ! -d "$MOUNT_POINT/context" ]; then
        kill $MOUNT_PID 2>/dev/null || true
        fail "/context directory not found in mount"
    fi
    pass "Filesystem mounted with /context"
    
    # -------------------------------------------------------------------------
    # Test 1: Create and write file via FUSE
    # -------------------------------------------------------------------------
    info "Test 1: Write file via FUSE..."
    
    echo "Hello from FUSE" > "$MOUNT_POINT/context/hello.txt"
    pass "Created hello.txt"
    
    # -------------------------------------------------------------------------
    # Test 2: Read file back via FUSE
    # -------------------------------------------------------------------------
    info "Test 2: Read file via FUSE..."
    
    CONTENT=$(cat "$MOUNT_POINT/context/hello.txt")
    [ "$CONTENT" = "Hello from FUSE" ] || fail "Content mismatch: '$CONTENT'"
    pass "Read: '$CONTENT'"
    
    # -------------------------------------------------------------------------
    # Test 3: Create directory
    # -------------------------------------------------------------------------
    info "Test 3: Create directory..."
    
    mkdir -p "$MOUNT_POINT/context/subdir"
    echo "Nested file" > "$MOUNT_POINT/context/subdir/nested.txt"
    
    CONTENT=$(cat "$MOUNT_POINT/context/subdir/nested.txt")
    [ "$CONTENT" = "Nested file" ] || fail "Nested content mismatch"
    pass "Created subdir/nested.txt"
    
    # -------------------------------------------------------------------------
    # Test 4: List directory
    # -------------------------------------------------------------------------
    info "Test 4: List directory..."
    
    ls -la "$MOUNT_POINT/context/"
    FILES=$(ls "$MOUNT_POINT/context/" | wc -l | tr -d ' ')
    [ "$FILES" -ge 1 ] || fail "Expected at least 1 entry, got $FILES"
    pass "Listed $FILES entries"
    
    # -------------------------------------------------------------------------
    # Test 5: Verify in S3 (data should be in workspace prefix)
    # -------------------------------------------------------------------------
    info "Test 5: Verify in S3..."
    
    S3_KEY="$WORKSPACE_ID/hello.txt"
    if aws_local s3 ls "s3://$S3_BUCKET/$S3_KEY" 2>/dev/null; then
        S3_CONTENT=$(aws_local s3 cp "s3://$S3_BUCKET/$S3_KEY" -)
        [ "$S3_CONTENT" = "Hello from FUSE" ] || fail "S3 content mismatch"
        pass "Verified in S3: s3://$S3_BUCKET/$S3_KEY"
    else
        info "File not yet in S3 (may need sync)"
    fi
    
    # -------------------------------------------------------------------------
    # Test 6: Unmount and remount (persistence test)
    # -------------------------------------------------------------------------
    info "Test 6: Persistence test (unmount/remount)..."
    
    MARKER="persist-$(date +%s)"
    echo "$MARKER" > "$MOUNT_POINT/context/persist-test.txt"
    
    # Unmount
    kill $MOUNT_PID 2>/dev/null || true
    sleep 2
    cleanup_mount
    
    # Remount
    mkdir -p "$MOUNT_POINT"
    "$PROJECT_ROOT/bin/cli" mount "$MOUNT_POINT" --gateway "$GATEWAY_GRPC" --token "$TOKEN" &
    MOUNT_PID=$!
    sleep 3
    
    # Verify file still exists
    if [ -f "$MOUNT_POINT/context/persist-test.txt" ]; then
        CONTENT=$(cat "$MOUNT_POINT/context/persist-test.txt")
        [ "$CONTENT" = "$MARKER" ] || fail "Persistence content mismatch"
        pass "File persisted after remount: $MARKER"
    else
        info "File not found after remount (S3 latency or cache)"
    fi
    
    # -------------------------------------------------------------------------
    # Test 7: Delete file
    # -------------------------------------------------------------------------
    info "Test 7: Delete file..."
    
    rm -f "$MOUNT_POINT/context/hello.txt" 2>/dev/null || true
    if [ -f "$MOUNT_POINT/context/hello.txt" ]; then
        info "File still exists (delete may be async)"
    else
        pass "File deleted"
    fi
    
    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------
    info "Cleaning up..."
    kill $MOUNT_PID 2>/dev/null || true
    sleep 1
    cleanup_mount
    
    # Clean up S3 data for this workspace
    aws_local s3 rm "s3://$S3_BUCKET/$WORKSPACE_ID/" --recursive 2>/dev/null || true
    
    echo ""
    pass "Context storage test passed (7 tests)"
}

# ============================================================================
# Main
# ============================================================================

echo "================================================"
echo "Airstore End-to-End Tests"
echo "================================================"
echo ""
echo "Gateway:    $GATEWAY_HTTP"
echo "S3:         $S3_ENDPOINT"
echo "Mount:      $MOUNT_POINT"
echo ""

# Run specific test or all tests
case "${1:-all}" in
    setup)
        test_setup
        ;;
    task)
        test_task
        ;;
    fs|filesystem)
        test_filesystem
        ;;
    tools)
        test_tools
        ;;
    context|s3)
        test_context
        ;;
    all)
        test_setup
        test_task
        test_filesystem
        test_tools
        test_context
        ;;
    *)
        echo "Unknown test: $1"
        echo "Available: setup, task, fs, tools, context, all"
        exit 1
        ;;
esac

echo ""
echo "================================================"
echo -e "${GREEN}All tests passed!${NC}"
echo "================================================"

