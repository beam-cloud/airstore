package services

import (
	"context"
	"testing"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stubTaskOutputBackend struct {
	repository.BackendRepository
	outputs []*types.TaskOutput
	err     error
}

func (s *stubTaskOutputBackend) CreateTaskOutput(_ context.Context, output *types.TaskOutput) error {
	if output != nil {
		cloned := *output
		if output.Data != nil {
			cloned.Data = make(map[string]any, len(output.Data))
			for k, v := range output.Data {
				cloned.Data[k] = v
			}
		}
		if output.Metadata != nil {
			cloned.Metadata = make(map[string]any, len(output.Metadata))
			for k, v := range output.Metadata {
				cloned.Metadata[k] = v
			}
		}
		s.outputs = append(s.outputs, &cloned)
	}
	return s.err
}

func TestWorkerCreateTaskOutputScopesIdempotentIDsByTask(t *testing.T) {
	backend := &stubTaskOutputBackend{}
	svc := &WorkerService{backend: backend}

	reqA := &pb.CreateTaskOutputRequest{
		WorkspaceId:  7,
		TaskId:       "task-a",
		OutputType:   "report",
		Title:        "Title",
		MetadataJson: `{"_idempotent_output_id":"approval-item-1","approval_batch_id":"batch-a"}`,
	}
	reqB := &pb.CreateTaskOutputRequest{
		WorkspaceId:  7,
		TaskId:       "task-b",
		OutputType:   "report",
		Title:        "Title",
		MetadataJson: `{"_idempotent_output_id":"approval-item-1","approval_batch_id":"batch-b"}`,
	}

	respA, err := svc.CreateTaskOutput(context.Background(), reqA)
	if err != nil {
		t.Fatalf("CreateTaskOutput(task-a) returned error: %v", err)
	}
	respB, err := svc.CreateTaskOutput(context.Background(), reqB)
	if err != nil {
		t.Fatalf("CreateTaskOutput(task-b) returned error: %v", err)
	}

	if respA.Id == "" || respB.Id == "" {
		t.Fatalf("expected non-empty response IDs, got %q and %q", respA.Id, respB.Id)
	}
	if respA.Id == respB.Id {
		t.Fatalf("expected task-scoped ids to differ, got %q", respA.Id)
	}
	if len(backend.outputs) != 2 {
		t.Fatalf("expected 2 backend writes, got %d", len(backend.outputs))
	}
	if _, ok := backend.outputs[0].Metadata["_idempotent_output_id"]; ok {
		t.Fatalf("expected backend metadata to strip _idempotent_output_id, got %+v", backend.outputs[0].Metadata)
	}
}

func TestWorkerCreateTaskOutputKeepsScopedIdempotentIDStableWithinTask(t *testing.T) {
	backend := &stubTaskOutputBackend{}
	svc := &WorkerService{backend: backend}

	req := &pb.CreateTaskOutputRequest{
		WorkspaceId:  7,
		TaskId:       "task-a",
		OutputType:   "report",
		Title:        "Title",
		MetadataJson: `{"_idempotent_output_id":"approval-item-1"}`,
	}

	respA, err := svc.CreateTaskOutput(context.Background(), req)
	if err != nil {
		t.Fatalf("first CreateTaskOutput returned error: %v", err)
	}
	respB, err := svc.CreateTaskOutput(context.Background(), req)
	if err != nil {
		t.Fatalf("second CreateTaskOutput returned error: %v", err)
	}

	if respA.Id != respB.Id {
		t.Fatalf("expected stable scoped ID, got %q and %q", respA.Id, respB.Id)
	}
}

func TestWorkerCreateTaskOutputMapsConflictToAlreadyExists(t *testing.T) {
	backend := &stubTaskOutputBackend{
		err: &types.ErrTaskOutputConflict{
			ID:                  "output-1",
			WorkspaceID:         7,
			TaskID:              "task-a",
			ExistingWorkspaceID: 9,
			ExistingTaskID:      "task-b",
		},
	}
	svc := &WorkerService{backend: backend}

	_, err := svc.CreateTaskOutput(context.Background(), &pb.CreateTaskOutputRequest{
		WorkspaceId: 7,
		TaskId:      "task-a",
		OutputType:  "report",
		Title:       "Title",
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v (%v)", status.Code(err), err)
	}
}
