package worker

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
	pb "github.com/beam-cloud/airstore/proto"
)

type captureOutputClient struct {
	createReqs   []*pb.CreateTaskOutputRequest
	finalizeReqs []*pb.FinalizeTaskOutputRequest
}

func (c *captureOutputClient) CreateTaskOutput(_ context.Context, req *pb.CreateTaskOutputRequest) (string, error) {
	c.createReqs = append(c.createReqs, req)
	return "output-1", nil
}

func (c *captureOutputClient) AppendTaskOutputRows(_ context.Context, _ *pb.AppendTaskOutputRowsRequest) error {
	return nil
}

func (c *captureOutputClient) FinalizeTaskOutput(_ context.Context, req *pb.FinalizeTaskOutputRequest) error {
	c.finalizeReqs = append(c.finalizeReqs, req)
	return nil
}

func (c *captureOutputClient) UpdateTaskOutputStatus(_ context.Context, _ *pb.UpdateTaskOutputStatusRequest) error {
	return nil
}

func testRunExecution() types.RunExecution {
	return types.RunExecution{
		WorkspaceId: 7,
		ExecutionPolicy: map[string]any{
			types.AgentExecutionMetaKeyOriginTaskID: "task-1",
			types.AgentExecutionMetaKeyRunID:        "run-1",
			types.AgentExecutionMetaKeyAgentID:      "agent-1",
		},
	}
}

func TestPersistAssistantResponseOutputsPersistsPendingApprovalArtifact(t *testing.T) {
	client := &captureOutputClient{}
	task := testRunExecution()

	extract := func(_ context.Context, _ *string, _ string, _ map[string]string) ([]signaltypes.ExtractedOutput, error) {
		summary := "Draft sales outreach email awaiting approval."
		content := "Hi Luke,\n\nI wanted to reach out about Airstore.\n"
		artifactKey := "sales-email"
		artifactLabel := "Sales Emails"
		artifactKind := "email"
		return []signaltypes.ExtractedOutput{{
			Kind:           signaltypes.OutputKindEMAIL_DRAFT,
			Title:          "Sales outreach email to Luke",
			Summary:        &summary,
			Content:        &content,
			Artifact_key:   &artifactKey,
			Artifact_label: &artifactLabel,
			Artifact_kind:  &artifactKind,
		}}, nil
	}

	err := persistAssistantResponseOutputs(
		context.Background(),
		client,
		task,
		nil,
		nil,
		"Please approve this outreach draft.",
		nil,
		assistantResponsePersistOptions{
			Extract: extract,
			MinLen:  1,
			Status:  types.TaskOutputStatusPending,
			Blocking: &blockingOutputMetadata{
				Kind:            types.TaskOutputBlockingKindApproval,
				InputKind:       types.InputKindApproveReject,
				WaitGroupID:     "wait-1",
				ApprovalSurface: true,
			},
		},
	)
	if err != nil {
		t.Fatalf("persistAssistantResponseOutputs returned error: %v", err)
	}
	if got := len(client.createReqs); got != 1 {
		t.Fatalf("create req count = %d, want 1", got)
	}

	req := client.createReqs[0]
	if got := req.OutputType; got != "email" {
		t.Fatalf("output type = %q, want email", got)
	}
	if got := req.Status; got != types.TaskOutputStatusPending {
		t.Fatalf("status = %q, want pending", got)
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(req.DataJson), &data); err != nil {
		t.Fatalf("unmarshal data json: %v", err)
	}
	if got := data[keyContent]; got != "Hi Luke,\n\nI wanted to reach out about Airstore." {
		t.Fatalf("content = %#v, want persisted draft body", got)
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(req.MetadataJson), &metadata); err != nil {
		t.Fatalf("unmarshal metadata json: %v", err)
	}
	if got := metadata[types.TaskOutputMetadataArtifactKey]; got != "sales-email" {
		t.Fatalf("artifact_key = %#v, want sales-email", got)
	}
	if got := metadata[types.TaskOutputMetadataBlockingKind]; got != types.TaskOutputBlockingKindApproval {
		t.Fatalf("blocking_kind = %#v, want %q", got, types.TaskOutputBlockingKindApproval)
	}
	if got := metadata[types.TaskOutputMetadataInputKind]; got != string(types.InputKindApproveReject) {
		t.Fatalf("input_kind = %#v, want %q", got, types.InputKindApproveReject)
	}
	if got := metadata[types.TaskOutputMetadataWaitGroupID]; got != "wait-1" {
		t.Fatalf("wait_group_id = %#v, want wait-1", got)
	}
	if got := metadata[types.TaskOutputMetadataApprovalUI]; got != true {
		t.Fatalf("approval_surface = %#v, want true", got)
	}
	if got := metadata[keySource]; got != sourceAssistantResponse {
		t.Fatalf("source metadata = %#v, want %q", got, sourceAssistantResponse)
	}
	if got := len(client.finalizeReqs); got != 1 {
		t.Fatalf("finalize req count = %d, want 1", got)
	}
}

func TestPersistApprovalResponseOutputPublishesWhenTrackerAlreadyCreatedOutput(t *testing.T) {
	client := &captureOutputClient{}
	task := testRunExecution()
	tracker := &taskOutputTracker{}
	tracker.MarkCreated()

	extract := func(_ context.Context, _ *string, _ string, _ map[string]string) ([]signaltypes.ExtractedOutput, error) {
		content := "Please review this approval copy before sending."
		return []signaltypes.ExtractedOutput{{
			Kind:    signaltypes.OutputKindREPORT,
			Title:   "Approval artifact",
			Content: &content,
		}}, nil
	}

	err := persistAssistantResponseOutputs(
		context.Background(),
		client,
		task,
		tracker,
		nil,
		"Please approve this draft response.",
		nil,
		assistantResponsePersistOptions{
			Extract: extract,
			MinLen:  1,
			Status:  types.TaskOutputStatusPending,
			Blocking: &blockingOutputMetadata{
				Kind:            types.TaskOutputBlockingKindApproval,
				InputKind:       types.InputKindApproveReject,
				WaitGroupID:     "wait-existing-output",
				ApprovalSurface: true,
			},
			FallbackTitle: "Approval Required",
		},
	)
	if err != nil {
		t.Fatalf("persistAssistantResponseOutputs returned error: %v", err)
	}
	if got := len(client.createReqs); got != 1 {
		t.Fatalf("create req count = %d, want 1", got)
	}
}

func TestPersistAssistantResponseOutputsFallsBackWhenApprovalExtractsNothing(t *testing.T) {
	client := &captureOutputClient{}
	task := testRunExecution()
	assistantMessage := "I am waiting for your approval before I proceed with the requested action."

	extract := func(_ context.Context, _ *string, _ string, _ map[string]string) ([]signaltypes.ExtractedOutput, error) {
		return nil, nil
	}

	err := persistAssistantResponseOutputs(
		context.Background(),
		client,
		task,
		nil,
		nil,
		assistantMessage,
		nil,
		assistantResponsePersistOptions{
			Extract: extract,
			MinLen:  1,
			Status:  types.TaskOutputStatusPending,
			Blocking: &blockingOutputMetadata{
				Kind:            types.TaskOutputBlockingKindApproval,
				InputKind:       types.InputKindApproveReject,
				WaitGroupID:     "wait-fallback",
				ApprovalSurface: true,
			},
			FallbackTitle: "Approval Required",
		},
	)
	if err != nil {
		t.Fatalf("persistAssistantResponseOutputs returned error: %v", err)
	}
	if got := len(client.createReqs); got != 1 {
		t.Fatalf("create req count = %d, want 1", got)
	}

	req := client.createReqs[0]
	if got := req.OutputType; got != "text" {
		t.Fatalf("output type = %q, want text", got)
	}
	if got := req.Title; got != "Approval Required" {
		t.Fatalf("title = %q, want Approval Required", got)
	}
	if got := req.Status; got != types.TaskOutputStatusPending {
		t.Fatalf("status = %q, want pending", got)
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(req.DataJson), &data); err != nil {
		t.Fatalf("unmarshal data json: %v", err)
	}
	if got := data[keyContent]; got != assistantMessage {
		t.Fatalf("content = %#v, want assistant message", got)
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(req.MetadataJson), &metadata); err != nil {
		t.Fatalf("unmarshal metadata json: %v", err)
	}
	if got := metadata[types.TaskOutputMetadataBlockingKind]; got != types.TaskOutputBlockingKindApproval {
		t.Fatalf("blocking_kind = %#v, want %q", got, types.TaskOutputBlockingKindApproval)
	}
	if got := metadata[types.TaskOutputMetadataInputKind]; got != string(types.InputKindApproveReject) {
		t.Fatalf("input_kind = %#v, want %q", got, types.InputKindApproveReject)
	}
	if got := metadata[types.TaskOutputMetadataWaitGroupID]; got != "wait-fallback" {
		t.Fatalf("wait_group_id = %#v, want wait-fallback", got)
	}
	if got := metadata[types.TaskOutputMetadataApprovalUI]; got != true {
		t.Fatalf("approval_surface = %#v, want true", got)
	}
	if got := metadata[keySource]; got != sourceAssistantResponse {
		t.Fatalf("source metadata = %#v, want %q", got, sourceAssistantResponse)
	}
}
