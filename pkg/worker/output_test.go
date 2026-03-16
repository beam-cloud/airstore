package worker

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

type stubOutputClient struct {
	createStarted chan struct{}
	releaseCreate chan struct{}
	appendCalled  chan struct{}
	finalCalled   chan struct{}
	calls         []string
}

func (s *stubOutputClient) CreateTaskOutput(_ context.Context, _ *pb.CreateTaskOutputRequest) (string, error) {
	s.calls = append(s.calls, "create")
	close(s.createStarted)
	<-s.releaseCreate
	return "server-output-id", nil
}

func (s *stubOutputClient) AppendTaskOutputRows(_ context.Context, _ *pb.AppendTaskOutputRowsRequest) error {
	s.calls = append(s.calls, "append")
	close(s.appendCalled)
	return nil
}

func (s *stubOutputClient) FinalizeTaskOutput(_ context.Context, _ *pb.FinalizeTaskOutputRequest) error {
	s.calls = append(s.calls, "finalize")
	close(s.finalCalled)
	return nil
}

func TestOutputWriterSerializesCreateAppendFinalize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &stubOutputClient{
		createStarted: make(chan struct{}),
		releaseCreate: make(chan struct{}),
		appendCalled:  make(chan struct{}),
		finalCalled:   make(chan struct{}),
	}

	task := types.RunExecution{
		WorkspaceId: 7,
		ExecutionPolicy: map[string]any{
			types.AgentExecutionMetaKeyOriginTaskID: "task-1",
			types.AgentExecutionMetaKeyRunID:        "run-1",
			types.AgentExecutionMetaKeyAgentID:      "agent-1",
		},
	}
	writer := newOutputWriter(ctx, client, task, nil)

	writer.Write([]byte(`{"type":"output","output_id":"local-1","output_type":"json","title":"Report"}`))
	writer.Write([]byte(`{"type":"output_append","output_id":"local-1","rows":[{"value":"row-1"}]}`))
	writer.Write([]byte(`{"type":"output_done","output_id":"local-1","summary":"done"}`))

	select {
	case <-client.createStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for create call")
	}

	select {
	case <-client.appendCalled:
		t.Fatal("append ran before create completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(client.releaseCreate)

	select {
	case <-client.appendCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for append call")
	}

	select {
	case <-client.finalCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for finalize call")
	}

	expected := []string{"create", "append", "finalize"}
	if len(client.calls) != len(expected) {
		t.Fatalf("expected %d calls, got %v", len(expected), client.calls)
	}
	for i, call := range expected {
		if client.calls[i] != call {
			t.Fatalf("expected call order %v, got %v", expected, client.calls)
		}
	}

	writer.Wait()
}
