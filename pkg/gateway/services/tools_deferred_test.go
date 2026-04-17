package services

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/tools"
	"github.com/beam-cloud/airstore/pkg/types"
)

type deferredToolOutputBackend struct {
	repository.BackendRepository
	outputs []*types.TaskOutput
}

func (b *deferredToolOutputBackend) GetConnection(context.Context, uint, uint, string) (*types.IntegrationConnection, error) {
	return nil, fmt.Errorf("connection not configured")
}

func (b *deferredToolOutputBackend) CreateTaskOutput(_ context.Context, output *types.TaskOutput) error {
	if output == nil {
		return nil
	}
	cloned := *output
	cloned.Data = cloneMap(output.Data)
	cloned.Metadata = cloneMap(output.Metadata)
	b.outputs = append(b.outputs, &cloned)
	return nil
}

type fakeDeferredToolProvider struct {
	name   string
	stdout string
}

func (p *fakeDeferredToolProvider) Name() string { return p.name }
func (p *fakeDeferredToolProvider) Help() string { return "test tool" }

func (p *fakeDeferredToolProvider) Execute(context.Context, []string, io.Writer, io.Writer) error {
	return nil
}

func (p *fakeDeferredToolProvider) ExecuteWithContext(_ context.Context, _ *tools.ExecutionContext, _ []string, stdout, _ io.Writer) error {
	if _, err := io.WriteString(stdout, p.stdout); err != nil {
		return err
	}
	return nil
}

func cloneMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return map[string]any{}
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func registerDeferredEmailSchema(registry *tools.Registry, name string) {
	registry.RegisterSchema(name, &tools.ToolSchema{
		Name:        name,
		Description: "test schema",
		Commands: map[string]*tools.CommandSchema{
			"send-email": {
				Description: "send email",
				Write:       true,
				OutputType:  types.TaskOutputTypeEmail,
			},
		},
	})
}

func TestExecuteDeferredPersistsGmailEmailOutput(t *testing.T) {
	registry := tools.NewRegistry()
	registerDeferredEmailSchema(registry, "gmail")
	registry.Register(&fakeDeferredToolProvider{
		name:   "gmail",
		stdout: `{"thread_id":"thread-123","message_id":"msg-123","to":"luke@example.com","subject":"Beam sandboxes","url":"https://mail.google.com/mail/u/0/#inbox/thread-123"}`,
	})

	backend := &deferredToolOutputBackend{}
	svc := &ToolService{registry: registry, backend: backend}

	runID := "run-1"
	agentID := "agent-1"
	blockerID := "blocker-1"
	_, _, exitCode, err := svc.ExecuteDeferred(context.Background(), types.DeferredToolExecutionRequest{
		Task: &types.AgentTask{
			ID:               "task-1",
			WorkspaceID:      7,
			AgentID:          &agentID,
			TargetRunID:      &runID,
			CurrentBlockerID: &blockerID,
		},
		WorkspaceID: 7,
		MemberID:    42,
		ToolName:    "gmail",
		Args:        []string{"send-email", "luke@example.com", "Beam sandboxes", "Hello Luke"},
	})
	if err != nil {
		t.Fatalf("ExecuteDeferred returned error: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
	if got := len(backend.outputs); got != 1 {
		t.Fatalf("persisted outputs = %d, want 1", got)
	}

	output := backend.outputs[0]
	if got := output.OutputType; got != types.TaskOutputTypeEmail {
		t.Fatalf("output type = %q, want email", got)
	}
	if got := output.TaskID; got != "task-1" {
		t.Fatalf("task id = %q, want task-1", got)
	}
	if output.RunID == nil || *output.RunID != "run-1" {
		t.Fatalf("run id = %#v, want run-1", output.RunID)
	}
	if got := output.Data["thread_id"]; got != "thread-123" {
		t.Fatalf("thread_id = %#v, want thread-123", got)
	}
	if got := output.Data["status"]; got != "sent" {
		t.Fatalf("status = %#v, want sent", got)
	}
	if got := output.Metadata["_tool"]; got != "gmail" {
		t.Fatalf("_tool metadata = %#v, want gmail", got)
	}
	if got := output.Metadata["integration"]; got != "gmail" {
		t.Fatalf("integration metadata = %#v, want gmail", got)
	}
}

func TestExecuteDeferredPersistsOutlookEmailOutput(t *testing.T) {
	registry := tools.NewRegistry()
	registerDeferredEmailSchema(registry, "outlook")
	registry.Register(&fakeDeferredToolProvider{
		name:   "outlook",
		stdout: `{"conversation_id":"conv-123","thread_id":"conv-123","message_id":"msg-123","to":"luke@example.com","subject":"Beam sandboxes","status":"sent","url":"https://outlook.office.com/mail/id/msg-123"}`,
	})

	backend := &deferredToolOutputBackend{}
	svc := &ToolService{registry: registry, backend: backend}

	_, _, exitCode, err := svc.ExecuteDeferred(context.Background(), types.DeferredToolExecutionRequest{
		Task: &types.AgentTask{
			ID:          "task-1",
			WorkspaceID: 7,
		},
		WorkspaceID: 7,
		MemberID:    42,
		ToolName:    "outlook",
		Args:        []string{"send-email", "--to", "luke@example.com", "--subject", "Beam sandboxes", "--body", "Hello Luke"},
	})
	if err != nil {
		t.Fatalf("ExecuteDeferred returned error: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
	if got := len(backend.outputs); got != 1 {
		t.Fatalf("persisted outputs = %d, want 1", got)
	}

	output := backend.outputs[0]
	if got := output.Data["conversation_id"]; got != "conv-123" {
		t.Fatalf("conversation_id = %#v, want conv-123", got)
	}
	if got := output.Data["thread_id"]; got != "conv-123" {
		t.Fatalf("thread_id = %#v, want conv-123", got)
	}
	if got := output.Metadata["_tool"]; got != "outlook" {
		t.Fatalf("_tool metadata = %#v, want outlook", got)
	}
	if got := output.Metadata["integration"]; got != "outlook" {
		t.Fatalf("integration metadata = %#v, want outlook", got)
	}
}
