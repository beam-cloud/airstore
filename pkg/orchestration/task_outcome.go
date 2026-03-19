package orchestration

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

func SyncTaskOutcome(
	ctx context.Context,
	backend repository.BackendRepository,
	task *types.AgentTask,
	run *types.AgentRun,
) error {
	if backend == nil || task == nil || run == nil {
		return nil
	}
	taskID := strings.TrimSpace(task.ID)
	if taskID == "" {
		return nil
	}

	if run.Status == types.AgentRunStatusOK {
		if err := activateApprovedOutputs(ctx, backend, task); err != nil {
			log.Warn().Err(err).Str("task_id", taskID).Msg("failed to activate approved outputs")
		}
	}

	return syncTaskCost(ctx, backend, task)
}

// activateApprovedOutputs transitions any "approved" outputs to "active" after
// a successful run. Once the agent has processed an approval and the run
// completes, the approved action is considered taken (e.g. email sent).
func activateApprovedOutputs(ctx context.Context, backend repository.BackendRepository, task *types.AgentTask) error {
	outputs, err := backend.ListTaskOutputs(ctx, task.WorkspaceID, task.ID)
	if err != nil {
		return fmt.Errorf("list outputs for activation: %w", err)
	}
	for _, out := range outputs {
		if out == nil || out.Status != types.TaskOutputStatusApproved {
			continue
		}
		if err := backend.UpdateTaskOutputStatus(ctx, task.WorkspaceID, out.ID, types.TaskOutputStatusActive); err != nil {
			return fmt.Errorf("activate approved output %s: %w", out.ID, err)
		}
	}
	return nil
}

func syncTaskCost(ctx context.Context, backend repository.BackendRepository, task *types.AgentTask) error {
	taskID := strings.TrimSpace(task.ID)
	runs, err := backend.ListAgentRunsFiltered(ctx, task.WorkspaceID, types.AgentRunListFilter{
		TaskID: &taskID,
		Limit:  500,
	})
	if err != nil {
		return err
	}
	totalCost := 0.0
	for _, run := range runs {
		if run == nil {
			continue
		}
		totalCost += run.CostUSD
	}
	if err := backend.UpdateTaskCost(ctx, task.ID, totalCost); err != nil {
		return err
	}
	task.CostUSD = totalCost
	return nil
}
