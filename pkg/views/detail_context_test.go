package views

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type detailContextTestBackend struct {
	tasks    map[string]*types.AgentTask
	outputs  map[string][]*types.TaskOutput
	subtasks map[string][]*types.AgentTask
	bindings []repository.SpawnBinding
}

func (b *detailContextTestBackend) GetTaskByID(_ context.Context, taskID string) (*types.AgentTask, error) {
	return b.tasks[taskID], nil
}

func (b *detailContextTestBackend) ListTaskOutputs(_ context.Context, _ uint, taskID string) ([]*types.TaskOutput, error) {
	return b.outputs[taskID], nil
}

func (b *detailContextTestBackend) ListSubtasks(_ context.Context, parentTaskID string) ([]*types.AgentTask, error) {
	return b.subtasks[parentTaskID], nil
}

func (b *detailContextTestBackend) ListSpawnBindingsForOutputs(_ context.Context, outputIDs []string) ([]repository.SpawnBinding, error) {
	allowed := make(map[string]struct{}, len(outputIDs))
	for _, outputID := range outputIDs {
		allowed[outputID] = struct{}{}
	}
	filtered := make([]repository.SpawnBinding, 0, len(b.bindings))
	for _, binding := range b.bindings {
		if _, ok := allowed[binding.SourceOutputID]; ok {
			filtered = append(filtered, binding)
		}
	}
	return filtered, nil
}

func TestResolveRowDetailContextSelectsCanonicalBoundTaskAndMergedOutputs(t *testing.T) {
	now := time.Now().UTC()
	parentTask := &types.AgentTask{ID: "task-parent", CreatedAt: now.Add(-3 * time.Hour)}
	olderTask := &types.AgentTask{ID: "task-old", CreatedAt: now.Add(-2 * time.Hour)}
	latestTask := &types.AgentTask{
		ID:        "task-latest",
		CreatedAt: now.Add(-1 * time.Hour),
		CurrentBlocker: &types.TaskBlocker{
			ID:        "blocker-1",
			Kind:      types.TaskBlockerKindApproval,
			InputKind: types.InputKindApproveReject,
			Status:    types.TaskBlockerStatusOpen,
			OutputIDs: []string{"sub-output-1"},
		},
	}
	secondaryTask := &types.AgentTask{ID: "task-secondary", CreatedAt: now.Add(-90 * time.Minute)}

	backend := &detailContextTestBackend{
		tasks: map[string]*types.AgentTask{
			parentTask.ID:    parentTask,
			olderTask.ID:     olderTask,
			latestTask.ID:    latestTask,
			secondaryTask.ID: secondaryTask,
		},
		outputs: map[string][]*types.TaskOutput{
			latestTask.ID: {
				{ID: "sub-output-1", TaskID: latestTask.ID, OutputType: types.TaskOutputTypeEmail, Status: types.TaskOutputStatusPending, CreatedAt: now.Add(-55 * time.Minute)},
			},
			secondaryTask.ID: {
				{ID: "sub-output-2", TaskID: secondaryTask.ID, OutputType: "text", Status: types.TaskOutputStatusActive, CreatedAt: now.Add(-85 * time.Minute)},
			},
		},
		subtasks: map[string][]*types.AgentTask{
			parentTask.ID: {
				{ID: "fallback-subtask", CreatedAt: now.Add(-4 * time.Hour)},
			},
		},
		bindings: []repository.SpawnBinding{
			{TaskID: olderTask.ID, SourceOutputID: "parent-output-1", CreatedAt: now.Add(-2 * time.Hour)},
			{TaskID: latestTask.ID, SourceOutputID: "parent-output-1", CreatedAt: now.Add(-45 * time.Minute)},
			{TaskID: secondaryTask.ID, SourceOutputID: "parent-output-2", CreatedAt: now.Add(-80 * time.Minute)},
		},
	}

	parentOutputs := []*types.TaskOutput{
		{ID: "parent-output-1", TaskID: parentTask.ID, OutputType: types.TaskOutputTypeEmail, Status: types.TaskOutputStatusActive, CreatedAt: now.Add(-4 * time.Hour)},
		{ID: "parent-output-2", TaskID: parentTask.ID, OutputType: "text", Status: types.TaskOutputStatusActive, CreatedAt: now.Add(-3 * time.Hour)},
	}

	context, err := ResolveRowDetailContext(context.Background(), backend, 1, parentTask, parentOutputs, &ViewRow{
		ID:              "row-1",
		TaskID:          parentTask.ID,
		SourceOutputIDs: []string{"parent-output-1", "parent-output-2"},
	})
	if err != nil {
		t.Fatalf("ResolveRowDetailContext returned error: %v", err)
	}

	if got, want := context.DetailTaskID, latestTask.ID; got != want {
		t.Fatalf("detail task id = %q, want %q", got, want)
	}
	if context.Task == nil || context.Task.ID != latestTask.ID {
		t.Fatalf("detail task = %#v, want %q", context.Task, latestTask.ID)
	}
	if got, want := len(context.Outputs), 3; got != want {
		t.Fatalf("output count = %d, want %d", got, want)
	}
	if got, want := sortedOutputIDs(context.Outputs), []string{"parent-output-1", "parent-output-2", "sub-output-1"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("merged outputs = %#v, want %#v", got, want)
	}
	if got, want := len(context.Subtasks), 2; got != want {
		t.Fatalf("subtask count = %d, want %d", got, want)
	}
	if context.Subtasks[0].ID != latestTask.ID || context.Subtasks[1].ID != secondaryTask.ID {
		t.Fatalf("bound subtasks = %#v, want [%q %q]", []string{context.Subtasks[0].ID, context.Subtasks[1].ID}, latestTask.ID, secondaryTask.ID)
	}
}

func TestResolveRowDetailContextFallsBackToParentTaskWithoutBindings(t *testing.T) {
	now := time.Now().UTC()
	parentTask := &types.AgentTask{ID: "task-parent", CreatedAt: now.Add(-2 * time.Hour)}
	fallbackSubtask := &types.AgentTask{ID: "subtask-fallback", CreatedAt: now.Add(-1 * time.Hour)}
	parentOutputs := []*types.TaskOutput{
		{ID: "parent-output-1", TaskID: parentTask.ID, OutputType: "text", Status: types.TaskOutputStatusActive, CreatedAt: now.Add(-90 * time.Minute)},
	}
	backend := &detailContextTestBackend{
		tasks: map[string]*types.AgentTask{
			parentTask.ID: parentTask,
		},
		subtasks: map[string][]*types.AgentTask{
			parentTask.ID: {fallbackSubtask},
		},
	}

	context, err := ResolveRowDetailContext(context.Background(), backend, 1, parentTask, parentOutputs, &ViewRow{
		ID:              "row-1",
		TaskID:          parentTask.ID,
		SourceOutputIDs: []string{"parent-output-1"},
	})
	if err != nil {
		t.Fatalf("ResolveRowDetailContext returned error: %v", err)
	}

	if got, want := context.DetailTaskID, parentTask.ID; got != want {
		t.Fatalf("detail task id = %q, want %q", got, want)
	}
	if context.Task == nil || context.Task.ID != parentTask.ID {
		t.Fatalf("detail task = %#v, want %q", context.Task, parentTask.ID)
	}
	if got, want := len(context.Outputs), 1; got != want {
		t.Fatalf("output count = %d, want %d", got, want)
	}
	if context.Outputs[0].ID != parentOutputs[0].ID {
		t.Fatalf("output id = %q, want %q", context.Outputs[0].ID, parentOutputs[0].ID)
	}
	if got, want := len(context.Subtasks), 1; got != want {
		t.Fatalf("subtask count = %d, want %d", got, want)
	}
	if context.Subtasks[0].ID != fallbackSubtask.ID {
		t.Fatalf("fallback subtask id = %q, want %q", context.Subtasks[0].ID, fallbackSubtask.ID)
	}
}

func TestEnrichRowsWithOutputStateUsesCanonicalBoundTaskContext(t *testing.T) {
	now := time.Now().UTC()
	parentTask := &types.AgentTask{ID: "task-parent", CreatedAt: now.Add(-2 * time.Hour)}
	detailTask := &types.AgentTask{
		ID:        "task-detail",
		CreatedAt: now.Add(-1 * time.Hour),
		CurrentBlocker: &types.TaskBlocker{
			ID:        "blocker-1",
			Kind:      types.TaskBlockerKindApproval,
			InputKind: types.InputKindApproveReject,
			Status:    types.TaskBlockerStatusOpen,
			OutputIDs: []string{"detail-output"},
		},
	}
	rows := []resolvedSheetRow{{
		TaskID:          parentTask.ID,
		DetailTaskID:    parentTask.ID,
		RowID:           "row-1",
		SourceOutputIDs: "parent-output",
	}}
	outputs := []*types.TaskOutput{
		{ID: "parent-output", TaskID: parentTask.ID, OutputType: types.TaskOutputTypeEmail, Status: types.TaskOutputStatusActive, CreatedAt: now.Add(-90 * time.Minute)},
	}
	taskMeta := map[string]*types.AgentTask{
		parentTask.ID: parentTask,
	}
	bound := boundDetailContext{
		DetailTaskBySourceOutput: map[string]string{"parent-output": detailTask.ID},
		TasksByID:                map[string]*types.AgentTask{detailTask.ID: detailTask},
		OutputsByTask: map[string][]*types.TaskOutput{
			detailTask.ID: {
				{ID: "detail-output", TaskID: detailTask.ID, OutputType: types.TaskOutputTypeEmail, Status: types.TaskOutputStatusPending, CreatedAt: now.Add(-30 * time.Minute)},
			},
		},
	}

	enrichRowsWithOutputState(rows, outputs, bound, taskMeta)

	if got, want := rows[0].DetailTaskID, detailTask.ID; got != want {
		t.Fatalf("detail task id = %q, want %q", got, want)
	}
	if got, want := rows[0].BlockerKind, string(types.TaskBlockerKindApproval); got != want {
		t.Fatalf("blocker kind = %q, want %q", got, want)
	}
	if got, want := rows[0].BlockerInputKind, string(types.InputKindApproveReject); got != want {
		t.Fatalf("blocker input kind = %q, want %q", got, want)
	}
	if got, want := rows[0].OutputID, "detail-output"; got != want {
		t.Fatalf("output id = %q, want %q", got, want)
	}
	if got, want := rows[0].ApprovalSurface, "true"; got != want {
		t.Fatalf("approval surface = %q, want %q", got, want)
	}
	if taskMeta[detailTask.ID] == nil {
		t.Fatal("expected enrichRowsWithOutputState to cache canonical detail task metadata")
	}
}

func TestSelectedDetailTaskIDPrefersOpenAndNewestBoundTask(t *testing.T) {
	now := time.Now().UTC()
	openTask := &types.AgentTask{
		ID:        "task-open",
		CreatedAt: now.Add(-time.Minute),
		CurrentBlocker: &types.TaskBlocker{
			ID:     "blocker-1",
			Status: types.TaskBlockerStatusOpen,
		},
	}
	olderTask := &types.AgentTask{
		ID:        "task-old",
		CreatedAt: now.Add(-2 * time.Hour),
	}
	bound := boundDetailContext{
		DetailTaskBySourceOutput: map[string]string{
			"source-1": olderTask.ID,
			"source-2": openTask.ID,
		},
		TasksByID: map[string]*types.AgentTask{
			openTask.ID:  openTask,
			olderTask.ID: olderTask,
		},
	}

	if got, want := selectedDetailTaskID([]string{"source-1", "source-2"}, bound), openTask.ID; got != want {
		t.Fatalf("selected detail task = %q, want %q", got, want)
	}
}
