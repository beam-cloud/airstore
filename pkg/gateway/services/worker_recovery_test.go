package services

import (
	"context"
	"testing"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type recoveryTestBackend struct {
	repository.BackendRepository
	exec *types.RunExecution
}

func (b *recoveryTestBackend) GetRunExecution(_ context.Context, externalID string) (*types.RunExecution, error) {
	if b.exec == nil || b.exec.ExternalId != externalID {
		return nil, nil
	}
	return b.exec, nil
}

type recoveryTestQueue struct {
	repository.TaskQueue
	state    *types.RunExecutionState
	requeued *types.RunExecution
}

func (q *recoveryTestQueue) GetState(_ context.Context, _ string) (*types.RunExecutionState, error) {
	return q.state, nil
}

func (q *recoveryTestQueue) Requeue(_ context.Context, task *types.RunExecution) error {
	q.requeued = task
	return nil
}

func TestProcessStaleUnclaimedRunRequeuesLostExecution(t *testing.T) {
	queue := &recoveryTestQueue{
		state: &types.RunExecutionState{
			ID:     "run-123",
			Status: types.RunExecutionStatusRunning,
		},
	}
	svc := &WorkerService{
		backend: &recoveryTestBackend{
			exec: &types.RunExecution{
				ExternalId: "run-123",
				Status:     types.RunExecutionStatusPending,
			},
		},
		taskQueue: queue,
	}

	detected, recovered := svc.processStaleUnclaimedRun(context.Background(), &types.AgentRun{
		ID:     "run-123",
		Status: types.AgentRunStatusAccepted,
	})
	if !detected || !recovered {
		t.Fatalf("unexpected recovery outcome: detected=%v recovered=%v", detected, recovered)
	}
	if queue.requeued == nil || queue.requeued.ExternalId != "run-123" {
		t.Fatalf("expected run to be requeued, got %#v", queue.requeued)
	}
}

func TestProcessStaleUnclaimedRunSkipsAlreadyPendingQueueState(t *testing.T) {
	queue := &recoveryTestQueue{
		state: &types.RunExecutionState{
			ID:     "run-123",
			Status: types.RunExecutionStatusPending,
		},
	}
	svc := &WorkerService{
		backend: &recoveryTestBackend{
			exec: &types.RunExecution{
				ExternalId: "run-123",
				Status:     types.RunExecutionStatusPending,
			},
		},
		taskQueue: queue,
	}

	detected, recovered := svc.processStaleUnclaimedRun(context.Background(), &types.AgentRun{
		ID:     "run-123",
		Status: types.AgentRunStatusAccepted,
	})
	if detected || recovered {
		t.Fatalf("expected already-pending run to be ignored: detected=%v recovered=%v", detected, recovered)
	}
	if queue.requeued != nil {
		t.Fatalf("did not expect requeue, got %#v", queue.requeued)
	}
}
