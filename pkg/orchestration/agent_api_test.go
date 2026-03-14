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
}

func (b *workspaceLiveBackend) ListTasksFiltered(_ context.Context, _ uint, filter types.AgentTaskListFilter) ([]*types.AgentTask, error) {
	b.lastTaskFilter = filter
	return []*types.AgentTask{}, nil
}

func (b *workspaceLiveBackend) ListWorkspaceTaskOutputs(_ context.Context, _ uint, filter types.TaskOutputListFilter) ([]*types.TaskOutput, error) {
	b.lastOutputFilter = filter
	return []*types.TaskOutput{}, nil
}

func TestWorkspaceLiveBatchIncludesArchivedOutputs(t *testing.T) {
	backend := &workspaceLiveBackend{}
	api := NewAgentAPI(backend, nil)

	batch, err := api.WorkspaceLiveBatch(context.Background(), 7)
	if err != nil {
		t.Fatalf("WorkspaceLiveBatch returned error: %v", err)
	}
	if batch == nil {
		t.Fatal("expected workspace batch")
	}
	if backend.lastOutputFilter.ExcludeArchived {
		t.Fatal("expected workspace live batch to include archived outputs")
	}
	if !backend.lastTaskFilter.IncludeArchived {
		t.Fatal("expected workspace live batch to include archived tasks")
	}
}
