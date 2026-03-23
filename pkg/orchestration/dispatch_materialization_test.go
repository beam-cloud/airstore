package orchestration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestHandleRunMaterializationErrorRetriesSessionBusy(t *testing.T) {
	err := handleRunMaterializationError(
		context.Background(),
		&types.AgentTask{ID: "task-1", WorkspaceID: 347},
		fmt.Errorf("session 123 still held by run-1 after drain timeout"),
		nil,
		nil,
	)

	var retry *dispatchRetryRequest
	if !errors.As(err, &retry) {
		t.Fatalf("expected dispatchRetryRequest, got %v", err)
	}
	if retry.reason != "session_busy" {
		t.Fatalf("retry reason = %q, want session_busy", retry.reason)
	}
	if retry.delay != sessionBusyRequeueDelay {
		t.Fatalf("retry delay = %s, want %s", retry.delay, sessionBusyRequeueDelay)
	}
}

func TestHandleRunMaterializationErrorDropsFatalPayloadErrors(t *testing.T) {
	dropped := false
	notified := false
	err := handleRunMaterializationError(
		context.Background(),
		&types.AgentTask{ID: "task-1", WorkspaceID: 347},
		fmt.Errorf("missing session_id in payload"),
		func(_ context.Context, taskID string, reason string) error {
			dropped = true
			if taskID != "task-1" {
				t.Fatalf("taskID = %q, want task-1", taskID)
			}
			if reason != types.AgentTaskDropReasonRunMaterializationFail {
				t.Fatalf("reason = %q, want %q", reason, types.AgentTaskDropReasonRunMaterializationFail)
			}
			return nil
		},
		func(_ context.Context, workspaceID uint, taskID string) {
			notified = true
			if workspaceID != 347 {
				t.Fatalf("workspaceID = %d, want 347", workspaceID)
			}
			if taskID != "task-1" {
				t.Fatalf("notify taskID = %q, want task-1", taskID)
			}
		},
	)
	if err != nil {
		t.Fatalf("handleRunMaterializationError returned error: %v", err)
	}
	if !dropped {
		t.Fatal("expected fatal error to drop task")
	}
	if !notified {
		t.Fatal("expected fatal error to publish task update")
	}
}

func TestHandleRunMaterializationErrorKeepsUnexpectedErrorsPending(t *testing.T) {
	dropped := false
	err := handleRunMaterializationError(
		context.Background(),
		&types.AgentTask{ID: "task-1", WorkspaceID: 347},
		fmt.Errorf("check session lease: i/o timeout"),
		func(_ context.Context, _, _ string) error {
			dropped = true
			return nil
		},
		nil,
	)
	if err == nil {
		t.Fatal("expected unexpected materialization error to be returned")
	}
	if dropped {
		t.Fatal("unexpected materialization error should not drop task")
	}
}

func TestHandleRunMaterializationErrorReturnsDropFailure(t *testing.T) {
	err := handleRunMaterializationError(
		context.Background(),
		&types.AgentTask{ID: "task-1", WorkspaceID: 347},
		fmt.Errorf("missing session_id in payload"),
		func(_ context.Context, _, _ string) error {
			return errors.New("drop failed")
		},
		nil,
	)
	if err == nil {
		t.Fatal("expected drop failure to be returned")
	}
	if got := err.Error(); got == "" || !containsAll(got, []string{"missing session_id in payload", "drop failed"}) {
		t.Fatalf("error = %q, want both materialization and drop failures", got)
	}
}

func containsAll(s string, parts []string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}
