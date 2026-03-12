package orchestration

import (
	"context"
	"strings"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
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

	return syncTaskCost(ctx, backend, task)
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
