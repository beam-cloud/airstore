package views

import (
	"context"
	"sort"
	"strings"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type detailContextBackend interface {
	GetTaskByID(ctx context.Context, taskID string) (*types.AgentTask, error)
	ListTaskOutputs(ctx context.Context, workspaceID uint, taskID string) ([]*types.TaskOutput, error)
	ListSpawnBindingsForOutputs(ctx context.Context, outputIDs []string) ([]repository.SpawnBinding, error)
}

type rowDetailContextBackend interface {
	detailContextBackend
	ListSubtasks(ctx context.Context, parentTaskID string) ([]*types.AgentTask, error)
}

type boundDetailContext struct {
	DetailTaskBySourceOutput map[string]string
	TasksByID                map[string]*types.AgentTask
	OutputsByTask            map[string][]*types.TaskOutput
}

type RowDetailContext struct {
	ParentTaskID    string
	DetailTaskID    string
	SourceOutputIDs []string
	Task            *types.AgentTask
	Outputs         []*types.TaskOutput
	Subtasks        []*types.AgentTask
}

type rowDetailContextInput struct {
	ParentTaskID     string
	ParentTask       *types.AgentTask
	ParentOutputs    []*types.TaskOutput
	SourceOutputIDs  []string
	FallbackSubtasks []*types.AgentTask
	Bound            boundDetailContext
}

func ResolveRowDetailContext(
	ctx context.Context,
	backend rowDetailContextBackend,
	workspaceID uint,
	parentTask *types.AgentTask,
	parentOutputs []*types.TaskOutput,
	row *ViewRow,
) (RowDetailContext, error) {
	if parentTask == nil {
		return RowDetailContext{}, nil
	}
	sourceOutputIDs := rowDetailSourceOutputIDs(row)
	bound, err := fetchBoundDetailContext(ctx, backend, workspaceID, sourceOutputIDs)
	if err != nil {
		return RowDetailContext{}, err
	}
	subtasks, err := backend.ListSubtasks(ctx, parentTask.ID)
	if err != nil {
		return RowDetailContext{}, err
	}
	return buildRowDetailContext(rowDetailContextInput{
		ParentTaskID:     parentTask.ID,
		ParentTask:       parentTask,
		ParentOutputs:    parentOutputs,
		SourceOutputIDs:  sourceOutputIDs,
		FallbackSubtasks: subtasks,
		Bound:            bound,
	}), nil
}

func fetchBoundDetailContext(
	ctx context.Context,
	backend detailContextBackend,
	workspaceID uint,
	sourceOutputIDs []string,
) (boundDetailContext, error) {
	context := boundDetailContext{
		DetailTaskBySourceOutput: map[string]string{},
		TasksByID:                map[string]*types.AgentTask{},
		OutputsByTask:            map[string][]*types.TaskOutput{},
	}
	sourceOutputIDs = uniqueTrimmedStrings(sourceOutputIDs)
	if backend == nil || len(sourceOutputIDs) == 0 {
		return context, nil
	}
	bindings, err := backend.ListSpawnBindingsForOutputs(ctx, sourceOutputIDs)
	if err != nil {
		return context, err
	}
	if len(bindings) == 0 {
		return context, nil
	}

	latestBindingByOutput := make(map[string]repository.SpawnBinding, len(bindings))
	taskIDs := make([]string, 0, len(bindings))
	seenTasks := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		sourceOutputID := strings.TrimSpace(binding.SourceOutputID)
		taskID := strings.TrimSpace(binding.TaskID)
		if sourceOutputID == "" || taskID == "" {
			continue
		}
		if existing, ok := latestBindingByOutput[sourceOutputID]; !ok || binding.CreatedAt.After(existing.CreatedAt) {
			latestBindingByOutput[sourceOutputID] = binding
			context.DetailTaskBySourceOutput[sourceOutputID] = taskID
		}
		if _, ok := seenTasks[taskID]; ok {
			continue
		}
		seenTasks[taskID] = struct{}{}
		taskIDs = append(taskIDs, taskID)
	}
	sort.Strings(taskIDs)
	for _, taskID := range taskIDs {
		task, err := backend.GetTaskByID(ctx, taskID)
		if err == nil && task != nil {
			context.TasksByID[taskID] = task
		}
		outputs, err := backend.ListTaskOutputs(ctx, workspaceID, taskID)
		if err != nil || len(outputs) == 0 {
			continue
		}
		context.OutputsByTask[taskID] = dedupeOutputs(outputs)
	}
	return context, nil
}

func buildRowDetailContext(input rowDetailContextInput) RowDetailContext {
	sourceOutputIDs := uniqueTrimmedStrings(input.SourceOutputIDs)
	context := RowDetailContext{
		ParentTaskID:    strings.TrimSpace(input.ParentTaskID),
		DetailTaskID:    strings.TrimSpace(input.ParentTaskID),
		SourceOutputIDs: sourceOutputIDs,
		Task:            input.ParentTask,
	}

	detailTaskID := selectedDetailTaskID(sourceOutputIDs, input.Bound.DetailTaskBySourceOutput)
	if detailTaskID != "" {
		context.DetailTaskID = detailTaskID
	}
	if detailTask := input.Bound.TasksByID[context.DetailTaskID]; detailTask != nil {
		context.Task = detailTask
	}

	parentOutputs := rowSourceOutputs(input.ParentOutputs, sourceOutputIDs)
	if len(parentOutputs) == 0 {
		parentOutputs = dedupeOutputs(input.ParentOutputs)
	}
	combined := append([]*types.TaskOutput{}, parentOutputs...)
	if boundOutputs := input.Bound.OutputsByTask[context.DetailTaskID]; len(boundOutputs) > 0 {
		combined = append(combined, boundOutputs...)
	}
	context.Outputs = dedupeOutputs(combined)

	context.Subtasks = boundSubtasks(sourceOutputIDs, input.Bound)
	if len(context.Subtasks) == 0 {
		context.Subtasks = input.FallbackSubtasks
	}
	return context
}

func rowDetailSourceOutputIDs(row *ViewRow) []string {
	if row == nil || len(row.SourceOutputIDs) == 0 {
		return nil
	}
	return uniqueTrimmedStrings(row.SourceOutputIDs)
}

func selectedDetailTaskID(sourceOutputIDs []string, detailTaskBySourceOutput map[string]string) string {
	for _, outputID := range sourceOutputIDs {
		taskID := strings.TrimSpace(detailTaskBySourceOutput[outputID])
		if taskID != "" {
			return taskID
		}
	}
	return ""
}

func rowSourceOutputs(parentOutputs []*types.TaskOutput, sourceOutputIDs []string) []*types.TaskOutput {
	if len(sourceOutputIDs) == 0 || len(parentOutputs) == 0 {
		return nil
	}
	outputByID := make(map[string]*types.TaskOutput, len(parentOutputs))
	for _, output := range parentOutputs {
		if output == nil || strings.TrimSpace(output.ID) == "" {
			continue
		}
		outputByID[output.ID] = output
	}
	selected := make([]*types.TaskOutput, 0, len(sourceOutputIDs))
	for _, outputID := range sourceOutputIDs {
		if output := outputByID[outputID]; output != nil {
			selected = append(selected, output)
		}
	}
	return dedupeOutputs(selected)
}

func boundSubtasks(sourceOutputIDs []string, bound boundDetailContext) []*types.AgentTask {
	if len(sourceOutputIDs) == 0 || len(bound.DetailTaskBySourceOutput) == 0 || len(bound.TasksByID) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(sourceOutputIDs))
	subtasks := make([]*types.AgentTask, 0, len(sourceOutputIDs))
	for _, outputID := range sourceOutputIDs {
		taskID := strings.TrimSpace(bound.DetailTaskBySourceOutput[outputID])
		if taskID == "" {
			continue
		}
		if _, ok := seen[taskID]; ok {
			continue
		}
		task := bound.TasksByID[taskID]
		if task == nil {
			continue
		}
		seen[taskID] = struct{}{}
		subtasks = append(subtasks, task)
	}
	sort.SliceStable(subtasks, func(i, j int) bool {
		return subtasks[i].CreatedAt.After(subtasks[j].CreatedAt)
	})
	return subtasks
}
