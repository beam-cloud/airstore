package orchestration

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

func isFatalRunMaterializationError(err error) bool {
	if err == nil {
		return false
	}

	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "missing prompt/message in task payload"):
		return true
	case strings.Contains(lower, "missing session_id in payload"):
		return true
	case strings.Contains(lower, "timeout_ms must be >= 0"):
		return true
	case strings.Contains(lower, "agent provider is required in task payload"):
		return true
	case strings.Contains(lower, "is not supported"):
		return true
	case strings.Contains(lower, "invalid host:"):
		return true
	case strings.Contains(lower, "invalid security:"):
		return true
	case strings.Contains(lower, "invalid ask:"):
		return true
	case strings.Contains(lower, "invalid runtime_type:"):
		return true
	case strings.Contains(lower, "invalid workspace_access:"):
		return true
	case strings.Contains(lower, "invalid retry.max_attempts:"):
		return true
	case strings.Contains(lower, "invalid retry.delay_ms:"):
		return true
	default:
		return false
	}
}

func handleRunMaterializationError(
	ctx context.Context,
	task *types.AgentTask,
	err error,
	dropTask func(context.Context, string, string) error,
	notify func(context.Context, uint, string),
) error {
	if err == nil {
		return nil
	}

	taskID := ""
	workspaceID := uint(0)
	if task != nil {
		taskID = strings.TrimSpace(task.ID)
		workspaceID = task.WorkspaceID
	}

	if isSessionBusyError(err) {
		return &dispatchRetryRequest{
			reason: "session_busy",
			delay:  sessionBusyRequeueDelay,
		}
	}

	if !isFatalRunMaterializationError(err) {
		log.Warn().
			Err(err).
			Str("task_id", taskID).
			Msg("run materialization failed before run creation; leaving dispatch pending for retry")
		return fmt.Errorf("materialize run for task %s: %w", taskID, err)
	}

	reason := types.AgentTaskDropReasonRunMaterializationFail
	log.Warn().
		Err(err).
		Str("task_id", taskID).
		Str("drop_reason", reason).
		Msg("dropping task after fatal run materialization failure")

	if dropTask == nil {
		return fmt.Errorf("materialize run for task %s: no drop handler configured: %w", taskID, err)
	}
	if dropErr := dropTask(ctx, taskID, reason); dropErr != nil {
		return fmt.Errorf("materialize run for task %s: %v; drop task: %w", taskID, err, dropErr)
	}
	if notify != nil && task != nil {
		notify(ctx, workspaceID, taskID)
	}
	return nil
}
