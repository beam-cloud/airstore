package orchestration

import (
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

func dispatchRetryGuardKey(task *types.AgentTask, run *types.AgentRun, nextAttempt int) string {
	taskID := "unknown"
	if task != nil && strings.TrimSpace(task.ID) != "" {
		taskID = strings.TrimSpace(task.ID)
	}
	return fmt.Sprintf("dispatch_retry:%s:%s:%d", taskID, dispatchRetryScope(task, run), nextAttempt)
}

func dispatchRetryScope(task *types.AgentTask, run *types.AgentRun) string {
	if run != nil {
		if runID := strings.TrimSpace(run.ID); runID != "" {
			return "run:" + runID
		}
	}
	if task == nil {
		return "unknown"
	}
	if task.DispatchedAt != nil && !task.DispatchedAt.IsZero() {
		return fmt.Sprintf("dispatch:%d", task.DispatchedAt.UTC().UnixNano())
	}
	if task.QueuedAt != nil && !task.QueuedAt.IsZero() {
		return fmt.Sprintf("queue:%d", task.QueuedAt.UTC().UnixNano())
	}
	if task.TargetRunID != nil {
		if targetRunID := strings.TrimSpace(*task.TargetRunID); targetRunID != "" {
			return "target:" + targetRunID
		}
	}
	if !task.AcceptedAt.IsZero() {
		return fmt.Sprintf("accepted:%d", task.AcceptedAt.UTC().UnixNano())
	}
	if !task.CreatedAt.IsZero() {
		return fmt.Sprintf("created:%d", task.CreatedAt.UTC().UnixNano())
	}
	return "task"
}
