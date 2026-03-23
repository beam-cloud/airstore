package orchestration

import (
	"context"
	"testing"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type workspaceLiveBackend struct {
	repository.BackendRepository
	lastTaskFilter   types.AgentTaskListFilter
	lastOutputFilter types.TaskOutputListFilter
	outputs          []*types.TaskOutput
}

func (b *workspaceLiveBackend) ListTasksFiltered(_ context.Context, _ uint, filter types.AgentTaskListFilter) ([]*types.AgentTask, error) {
	b.lastTaskFilter = filter
	return []*types.AgentTask{}, nil
}

func (b *workspaceLiveBackend) ListWorkspaceTaskOutputs(_ context.Context, _ uint, filter types.TaskOutputListFilter) ([]*types.TaskOutput, error) {
	b.lastOutputFilter = filter
	return b.outputs, nil
}

func TestWorkspaceLiveBatchExcludesArchivedOutputs(t *testing.T) {
	backend := &workspaceLiveBackend{}
	api := NewAgentAPI(backend, nil)

	batch, err := api.WorkspaceLiveBatch(context.Background(), 7)
	if err != nil {
		t.Fatalf("WorkspaceLiveBatch returned error: %v", err)
	}
	if batch == nil {
		t.Fatal("expected workspace batch")
	}
	if !backend.lastOutputFilter.ExcludeArchived {
		t.Fatal("expected workspace live batch to exclude archived outputs")
	}
	if !backend.lastTaskFilter.IncludeArchived {
		t.Fatal("expected workspace live batch to include archived tasks")
	}
}

func TestWorkspaceLiveBatchHidesDraftEmailArtifacts(t *testing.T) {
	backend := &workspaceLiveBackend{
		outputs: []*types.TaskOutput{
			{
				ID:         "out-approval-email",
				OutputType: types.TaskOutputTypeEmail,
				Status:     types.TaskOutputStatusPending,
				Metadata: map[string]any{
					types.TaskOutputMetadataApprovalUI: true,
				},
			},
			{
				ID:         "out-draft-email",
				OutputType: types.TaskOutputTypeEmail,
				Status:     types.TaskOutputStatusActive,
				Data: map[string]any{
					"draft_id": "draft-123",
				},
			},
			{
				ID:         "out-sent-email",
				OutputType: types.TaskOutputTypeEmail,
				Status:     types.TaskOutputStatusActive,
				Data: map[string]any{
					"message_id": "msg-123",
				},
			},
			{
				ID:         "out-text",
				OutputType: "text",
				Status:     types.TaskOutputStatusActive,
			},
		},
	}
	api := NewAgentAPI(backend, nil)

	batch, err := api.WorkspaceLiveBatch(context.Background(), 7)
	if err != nil {
		t.Fatalf("WorkspaceLiveBatch returned error: %v", err)
	}
	if batch == nil {
		t.Fatal("expected workspace batch")
	}
	if got := len(batch.Outputs); got != 2 {
		t.Fatalf("workspace output count = %d, want 2", got)
	}
	if batch.Outputs[0].ID != "out-sent-email" || batch.Outputs[1].ID != "out-text" {
		t.Fatalf("workspace outputs = %#v, want sent email and text only", []string{batch.Outputs[0].ID, batch.Outputs[1].ID})
	}
}

type cancelTaskBackend struct {
	repository.BackendRepository
	task    *types.AgentTask
	outputs map[string]*types.TaskOutput
}

func (b *cancelTaskBackend) GetTask(_ context.Context, _ uint, _ string) (*types.AgentTask, error) {
	return b.task, nil
}

func (b *cancelTaskBackend) ListActiveChildTaskIDs(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func (b *cancelTaskBackend) UpdateTaskState(_ context.Context, update types.TaskStateUpdate) error {
	if b.task != nil && b.task.ID == update.TaskID {
		b.task.State = update.State
	}
	return nil
}

func (b *cancelTaskBackend) CancelPendingOutboxEventsForTask(_ context.Context, _ string) error {
	return nil
}

func (b *cancelTaskBackend) ListTaskOutputs(_ context.Context, _ uint, taskID string) ([]*types.TaskOutput, error) {
	var outputs []*types.TaskOutput
	for _, output := range b.outputs {
		if output != nil && output.TaskID == taskID {
			outputs = append(outputs, output)
		}
	}
	return outputs, nil
}

func (b *cancelTaskBackend) UpdateTaskOutputStatus(_ context.Context, _ uint, outputID string, status string) error {
	if output, ok := b.outputs[outputID]; ok {
		output.Status = status
	}
	return nil
}

type cancelCleanupRegistrar struct {
	calls []*types.AgentTask
}

func (r *cancelCleanupRegistrar) RegisterTaskSourceWatches(context.Context, *types.AgentTask, *types.RunExecutionWakeSignal, []*types.SourceWatchRequest) (*types.TaskBlockerSpec, error) {
	return nil, nil
}

func (r *cancelCleanupRegistrar) CleanupTaskSourceWatches(_ context.Context, task *types.AgentTask) error {
	r.calls = append(r.calls, task)
	return nil
}

func TestCancelTaskSupersedesPendingOutputs(t *testing.T) {
	task := &types.AgentTask{
		ID:          "task-1",
		WorkspaceID: 7,
		State:       types.AgentTaskStateWaiting,
	}
	backend := &cancelTaskBackend{
		task: task,
		outputs: map[string]*types.TaskOutput{
			"out-pending": {ID: "out-pending", TaskID: task.ID, Status: types.TaskOutputStatusPending},
			"out-active":  {ID: "out-active", TaskID: task.ID, Status: types.TaskOutputStatusActive},
		},
	}
	api := NewAgentAPI(backend, nil)

	if err := api.CancelTask(context.Background(), task.WorkspaceID, task.ID); err != nil {
		t.Fatalf("CancelTask returned error: %v", err)
	}
	if task.State != types.AgentTaskStateCancelled {
		t.Fatalf("expected task state cancelled, got %s", task.State)
	}
	if got := backend.outputs["out-pending"].Status; got != types.TaskOutputStatusCancelled {
		t.Fatalf("expected pending output to be cancelled, got %s", got)
	}
	if got := backend.outputs["out-active"].Status; got != types.TaskOutputStatusActive {
		t.Fatalf("expected active output unchanged, got %s", got)
	}
}

func TestCancelTaskCleansUpSourceWatches(t *testing.T) {
	task := &types.AgentTask{
		ID:          "task-1",
		WorkspaceID: 7,
		State:       types.AgentTaskStateWaiting,
	}
	backend := &cancelTaskBackend{
		task:    task,
		outputs: map[string]*types.TaskOutput{},
	}
	registrar := &cancelCleanupRegistrar{}
	runtime := &AgentService{
		backend: backend,
		runtimeLoops: &RuntimeLoops{
			backend:              backend,
			sourceWatchRegistrar: registrar,
		},
	}
	api := NewAgentAPI(backend, runtime)

	if err := api.CancelTask(context.Background(), task.WorkspaceID, task.ID); err != nil {
		t.Fatalf("CancelTask returned error: %v", err)
	}
	if len(registrar.calls) != 1 {
		t.Fatalf("cleanup calls = %d, want 1", len(registrar.calls))
	}
	if registrar.calls[0] == nil || registrar.calls[0].WorkspaceID != task.WorkspaceID || registrar.calls[0].ID != task.ID {
		t.Fatalf("cleanup called with %#v, want task %q in workspace %d", registrar.calls[0], task.ID, task.WorkspaceID)
	}
}
