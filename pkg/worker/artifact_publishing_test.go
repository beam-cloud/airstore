package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
	pb "github.com/beam-cloud/airstore/proto"
)

type captureOutputClient struct {
	createReqs   []*pb.CreateTaskOutputRequest
	finalizeReqs []*pb.FinalizeTaskOutputRequest
	updateReqs   []*pb.UpdateTaskOutputStatusRequest
}

func (c *captureOutputClient) CreateTaskOutput(_ context.Context, req *pb.CreateTaskOutputRequest) (string, error) {
	c.createReqs = append(c.createReqs, req)
	return fmt.Sprintf("output-%d", len(c.createReqs)), nil
}

func (c *captureOutputClient) AppendTaskOutputRows(_ context.Context, _ *pb.AppendTaskOutputRowsRequest) error {
	return nil
}

func (c *captureOutputClient) FinalizeTaskOutput(_ context.Context, req *pb.FinalizeTaskOutputRequest) error {
	c.finalizeReqs = append(c.finalizeReqs, req)
	return nil
}

func (c *captureOutputClient) UpdateTaskOutputStatus(_ context.Context, req *pb.UpdateTaskOutputStatusRequest) error {
	c.updateReqs = append(c.updateReqs, req)
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
		artifactKind := types.TaskOutputTypeEmail
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

	created, err := persistAssistantResponseOutputs(
		context.Background(),
		client,
		task,
		nil,
		nil,
		"Please approve this outreach draft.",
		nil,
		responseArtifactPlan{
			Extract: extract,
			MinLen:  1,
			Status:  types.TaskOutputStatusPending,
			Blocking: &types.TaskOutputBlockingMetadata{
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
	if !created {
		t.Fatal("expected approval artifact to be created")
	}
	if got := len(client.createReqs); got != 1 {
		t.Fatalf("create req count = %d, want 1", got)
	}

	req := client.createReqs[0]
	if got := req.OutputType; got != types.TaskOutputTypeEmail {
		t.Fatalf("output type = %q, want %q", got, types.TaskOutputTypeEmail)
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

func TestPersistApprovalResponseOutputPublishesWithTrackedState(t *testing.T) {
	client := &captureOutputClient{}
	task := testRunExecution()
	tracker := &taskOutputTracker{}

	extract := func(_ context.Context, _ *string, _ string, _ map[string]string) ([]signaltypes.ExtractedOutput, error) {
		content := "Please review this approval copy before sending."
		return []signaltypes.ExtractedOutput{{
			Kind:    signaltypes.OutputKindREPORT,
			Title:   "Approval artifact",
			Content: &content,
		}}, nil
	}

	created, err := persistAssistantResponseOutputs(
		context.Background(),
		client,
		task,
		tracker,
		nil,
		"Please approve this draft response.",
		nil,
		responseArtifactPlan{
			Extract: extract,
			MinLen:  1,
			Status:  types.TaskOutputStatusPending,
			Blocking: &types.TaskOutputBlockingMetadata{
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
	if !created {
		t.Fatal("expected approval artifact to be created")
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

	created, err := persistAssistantResponseOutputs(
		context.Background(),
		client,
		task,
		nil,
		nil,
		assistantMessage,
		nil,
		responseArtifactPlan{
			Extract: extract,
			MinLen:  1,
			Status:  types.TaskOutputStatusPending,
			Blocking: &types.TaskOutputBlockingMetadata{
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
	if !created {
		t.Fatal("expected fallback approval artifact to be created")
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

func TestPersistApprovalOutputBeforeWaitingCreatesBlockingOutput(t *testing.T) {
	client := &captureOutputClient{}
	task := testRunExecution()
	tracker := &taskOutputTracker{}
	assistantMessage := "Here is the draft email for approval.\n\n**To:** Mike <luke@slai.io>\n**Subject:** Cleaner dev environments for your team\n\nHello Mike."

	outputIDs, created := persistApprovalOutputBeforeWaitingWithFunc(
		context.Background(),
		client,
		task,
		tracker,
		"Draft a cold outreach email to Mike.",
		assistantMessage,
		nil,
		func(
			ctx context.Context,
			client taskOutputClient,
			task types.RunExecution,
			tracker *taskOutputTracker,
			userMessage *string,
			assistantMessage string,
			bamlEnv map[string]string,
		) ([]string, bool, error) {
			content := assistantMessage
			result, err := persistAssistantResponseOutputsDetailed(
				ctx,
				client,
				task,
				tracker,
				userMessage,
				assistantMessage,
				bamlEnv,
				responseArtifactPlan{
					Extract: func(_ context.Context, _ *string, _ string, _ map[string]string) ([]signaltypes.ExtractedOutput, error) {
						return []signaltypes.ExtractedOutput{{
							Kind:    signaltypes.OutputKindEMAIL_DRAFT,
							Title:   "Draft outreach email",
							Content: &content,
						}}, nil
					},
					MinLen: 1,
					Status: types.TaskOutputStatusPending,
					Blocking: &types.TaskOutputBlockingMetadata{
						Kind:            types.TaskOutputBlockingKindApproval,
						InputKind:       types.InputKindApproveReject,
						WaitGroupID:     "wait-now",
						ApprovalSurface: true,
					},
					FallbackTitle: "Approval Required",
				},
			)
			return result.OutputIDs, result.Handled, err
		},
	)
	if !created {
		t.Fatal("expected approval output to be persisted before waiting")
	}
	if len(outputIDs) != 1 {
		t.Fatalf("output id count = %d, want 1", len(outputIDs))
	}
	if got := len(client.createReqs); got != 1 {
		t.Fatalf("create req count = %d, want 1", got)
	}

	req := client.createReqs[0]
	if got := req.Status; got != types.TaskOutputStatusPending {
		t.Fatalf("status = %q, want pending", got)
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(req.MetadataJson), &metadata); err != nil {
		t.Fatalf("unmarshal metadata json: %v", err)
	}
	if got := metadata[types.TaskOutputMetadataBlockingKind]; got != types.TaskOutputBlockingKindApproval {
		t.Fatalf("blocking_kind = %#v, want %q", got, types.TaskOutputBlockingKindApproval)
	}
	if got := metadata[types.TaskOutputMetadataApprovalUI]; got != true {
		t.Fatalf("approval_surface = %#v, want true", got)
	}
}

func TestPersistApprovalOutputBeforeWaitingReusesEquivalentBlockingOutput(t *testing.T) {
	client := &captureOutputClient{}
	task := testRunExecution()
	tracker := &taskOutputTracker{}
	assistantMessage := "Please review this draft before sending.\n\nTo: luke@slai.io\nSubject: Beam sandboxes\n\nHi Luke,\n\nDraft body."

	persist := func(
		ctx context.Context,
		client taskOutputClient,
		task types.RunExecution,
		tracker *taskOutputTracker,
		userMessage *string,
		assistantMessage string,
		bamlEnv map[string]string,
	) ([]string, bool, error) {
		content := assistantMessage
		result, err := persistAssistantResponseOutputsDetailed(
			ctx,
			client,
			task,
			tracker,
			userMessage,
			assistantMessage,
			bamlEnv,
			responseArtifactPlan{
				Extract: func(_ context.Context, _ *string, _ string, _ map[string]string) ([]signaltypes.ExtractedOutput, error) {
					return []signaltypes.ExtractedOutput{{
						Kind:    signaltypes.OutputKindEMAIL_DRAFT,
						Title:   "Draft outreach email",
						Content: &content,
					}}, nil
				},
				MinLen: 1,
				Status: types.TaskOutputStatusPending,
				Blocking: &types.TaskOutputBlockingMetadata{
					Kind:            types.TaskOutputBlockingKindApproval,
					InputKind:       types.InputKindApproveReject,
					WaitGroupID:     "wait-stable",
					ApprovalSurface: true,
				},
				FallbackTitle: "Approval Required",
			},
		)
		return result.OutputIDs, result.Handled, err
	}

	firstIDs, firstHandled := persistApprovalOutputBeforeWaitingWithFunc(
		context.Background(),
		client,
		task,
		tracker,
		"Draft the reply.",
		assistantMessage,
		nil,
		persist,
	)
	secondIDs, secondHandled := persistApprovalOutputBeforeWaitingWithFunc(
		context.Background(),
		client,
		task,
		tracker,
		"Draft the reply.",
		assistantMessage,
		nil,
		persist,
	)

	if !firstHandled || !secondHandled {
		t.Fatal("expected approval output handling on both passes")
	}
	if got := len(client.createReqs); got != 1 {
		t.Fatalf("create req count = %d, want 1", got)
	}
	if got := len(firstIDs); got != 1 {
		t.Fatalf("first output id count = %d, want 1", got)
	}
	if got := len(secondIDs); got != 1 {
		t.Fatalf("second output id count = %d, want 1", got)
	}
	if firstIDs[0] != secondIDs[0] {
		t.Fatalf("expected stable output id reuse, got %q then %q", firstIDs[0], secondIDs[0])
	}
	if got := len(client.updateReqs); got != 0 {
		t.Fatalf("update req count = %d, want 0 for equivalent approval output", got)
	}
}

func TestPublishOutputCandidateSupersedesChangedArtifact(t *testing.T) {
	client := &captureOutputClient{}
	task := testRunExecution()
	tracker := &taskOutputTracker{}
	ids := outputIDsFromTask(task)

	base := outputCandidate{
		OutputType: types.TaskOutputTypeEmail,
		Title:      "Draft outreach email",
		Summary:    "Drafted outreach email for approval.",
		Data: map[string]any{
			"to":       "luke@slai.io",
			"subject":  "Beam sandboxes",
			keyContent: "Hi Luke,\n\nFirst draft.\n",
		},
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey:   "email-draft",
			types.TaskOutputMetadataArtifactKind:  "email",
			types.TaskOutputMetadataArtifactLabel: "Email Drafts",
			types.TaskOutputMetadataArtifactRole:  types.TaskOutputArtifactRolePrimary,
		},
		Status: types.TaskOutputStatusPending,
	}

	firstID, err := publishOutputCandidate(context.Background(), client, ids, tracker, base)
	if err != nil {
		t.Fatalf("publishOutputCandidate first: %v", err)
	}

	updated := base
	updated.Data = cloneAnyMap(base.Data)
	updated.Data[keyContent] = "Hi Luke,\n\nUpdated draft.\n"
	secondID, err := publishOutputCandidate(context.Background(), client, ids, tracker, updated)
	if err != nil {
		t.Fatalf("publishOutputCandidate second: %v", err)
	}

	if got := len(client.createReqs); got != 2 {
		t.Fatalf("create req count = %d, want 2", got)
	}
	if firstID == secondID {
		t.Fatalf("expected changed artifact to supersede with new id, got %q", firstID)
	}
	if got := len(client.updateReqs); got != 1 {
		t.Fatalf("update req count = %d, want 1", got)
	}
	if got := client.updateReqs[0].OutputId; got != firstID {
		t.Fatalf("updated predecessor = %q, want %q", got, firstID)
	}
	if got := client.updateReqs[0].Status; got != types.TaskOutputStatusCancelled {
		t.Fatalf("updated status = %q, want %q", got, types.TaskOutputStatusCancelled)
	}
}

func TestPersistFinalResponseOutputSkipsEmailArtifacts(t *testing.T) {
	client := &captureOutputClient{}
	task := testRunExecution()
	assistantMessage := strings.Repeat("A", minResponseOutputLen)

	extract := func(_ context.Context, _ *string, _ string, _ map[string]string) ([]signaltypes.ExtractedOutput, error) {
		draftSummary := "Draft outreach email."
		draftContent := "Draft body"
		sentSummary := "Sent outreach email."
		sentContent := "Sent body"
		return []signaltypes.ExtractedOutput{
			{
				Kind:    signaltypes.OutputKindEMAIL_DRAFT,
				Title:   "Draft outreach email",
				Summary: &draftSummary,
				Content: &draftContent,
			},
			{
				Kind:    signaltypes.OutputKindEMAIL_SENT,
				Title:   "Sent outreach email",
				Summary: &sentSummary,
				Content: &sentContent,
			},
		}, nil
	}

	created, err := persistFinalResponseOutput(
		context.Background(),
		client,
		task,
		nil,
		nil,
		assistantMessage,
		nil,
		extract,
	)
	if err != nil {
		t.Fatalf("persistFinalResponseOutput returned error: %v", err)
	}
	if created {
		t.Fatal("expected final response persistence to skip email artifacts")
	}
	if got := len(client.createReqs); got != 0 {
		t.Fatalf("create req count = %d, want 0", got)
	}
}

func TestDefaultArtifactMetadataPreservesBAMLValues(t *testing.T) {
	metadata := map[string]any{
		types.TaskOutputMetadataArtifactKey:   "extracted-recipes",
		types.TaskOutputMetadataArtifactLabel: "Extracted Recipes",
		types.TaskOutputMetadataArtifactKind:  "recipe",
	}

	result := defaultArtifactMetadata(metadata, types.TaskOutputArtifactRolePrimary)

	if got := result[types.TaskOutputMetadataArtifactKey]; got != "extracted-recipes" {
		t.Fatalf("artifact key = %#v, want %q", got, "extracted-recipes")
	}
	if got := result[types.TaskOutputMetadataArtifactLabel]; got != "Extracted Recipes" {
		t.Fatalf("artifact label = %#v, want %q", got, "Extracted Recipes")
	}
	if got := result[types.TaskOutputMetadataArtifactKind]; got != "recipe" {
		t.Fatalf("artifact kind = %#v, want %q", got, "recipe")
	}
	if got := result[types.TaskOutputMetadataArtifactRole]; got != types.TaskOutputArtifactRolePrimary {
		t.Fatalf("role = %#v, want %q", got, types.TaskOutputArtifactRolePrimary)
	}
}

func TestDefaultArtifactMetadataNormalizesTokens(t *testing.T) {
	metadata := map[string]any{
		types.TaskOutputMetadataArtifactKey:  "Extracted Recipes",
		types.TaskOutputMetadataArtifactKind: "RECIPE",
	}

	result := defaultArtifactMetadata(metadata, "")

	if got := result[types.TaskOutputMetadataArtifactKey]; got != "extracted-recipes" {
		t.Fatalf("artifact key = %#v, want %q", got, "extracted-recipes")
	}
	if got := result[types.TaskOutputMetadataArtifactKind]; got != "recipe" {
		t.Fatalf("artifact kind = %#v, want %q", got, "recipe")
	}
}

func TestDefaultArtifactMetadataSetsRoleDefault(t *testing.T) {
	result := defaultArtifactMetadata(nil, "")

	if got := result[types.TaskOutputMetadataArtifactRole]; got != types.TaskOutputArtifactRoleSupporting {
		t.Fatalf("role should default to supporting, got %#v", got)
	}
}
