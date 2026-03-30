package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

func TestResolveRunAgentConfigStrengthensSkillDirectives(t *testing.T) {
	service := &AgentService{}
	originalPrompt := strings.Join([]string{
		"You are a helpful agent.",
		"",
		"## Active Skills",
		"- /workspace/skills/mystery-shopper -- audit storefronts",
		"",
		"## Working Style",
		"Be concise.",
	}, "\n")
	payloadConfig := map[string]any{
		agentConfigKeySystemPrompt: originalPrompt,
	}

	got := service.resolveRunAgentConfig(context.Background(), &types.AgentRun{WorkspaceID: 42}, map[string]any{
		agentPayloadKeyAgentConfig: payloadConfig,
	})

	prompt := stringFromPayload(got, agentConfigKeySystemPrompt)
	if !strings.HasPrefix(prompt, "## MANDATORY - Active Skills") {
		t.Fatalf("expected strengthened skills section to move to top, got prompt:\n%s", prompt)
	}
	if !strings.Contains(prompt, "These are the skills explicitly associated with this agent. You MUST read them before starting any work.") {
		t.Fatalf("expected mandatory assigned-skills guidance, got prompt:\n%s", prompt)
	}
	if !strings.Contains(prompt, "1. cat /workspace/skills/mystery-shopper/SKILL.md -- audit storefronts") {
		t.Fatalf("expected explicit cat directive, got prompt:\n%s", prompt)
	}
	if !strings.Contains(prompt, "These are the skills explicitly associated with this agent.") {
		t.Fatalf("expected assigned-skill priority guidance, got prompt:\n%s", prompt)
	}
	if !strings.Contains(prompt, "These assigned skills take priority over broader workspace-wide skills under /workspace/skills") {
		t.Fatalf("expected broader workspace skills to be fallback context, got prompt:\n%s", prompt)
	}
	if !strings.Contains(prompt, "## Working Style\nBe concise.") {
		t.Fatalf("expected remaining prompt content after strengthened section, got prompt:\n%s", prompt)
	}
	if strings.Contains(prompt, "should be loaded") {
		t.Fatalf("expected weak advisory language to be removed, got prompt:\n%s", prompt)
	}
	if gotOriginal := stringFromPayload(payloadConfig, agentConfigKeySystemPrompt); gotOriginal != originalPrompt {
		t.Fatalf("resolveRunAgentConfig mutated original payload config:\n%s", gotOriginal)
	}
}

func TestDefaultAgentConfigPrioritizesAssignedSkills(t *testing.T) {
	cfg := DefaultAgentConfig("mystery-shopper")
	prompt := stringFromPayload(cfg, agentConfigKeySystemPrompt)

	if !strings.Contains(prompt, "If this agent has explicitly assigned skills, read those assigned skills first.") {
		t.Fatalf("expected default prompt to prioritize assigned skills, got prompt:\n%s", prompt)
	}
	if !strings.Contains(prompt, "Explicitly assigned agent skills take priority over broader workspace-wide skills.") {
		t.Fatalf("expected default prompt to treat broader skills as fallback context, got prompt:\n%s", prompt)
	}
	if !strings.Contains(prompt, runtimeSchedulingGuidanceHeader) {
		t.Fatalf("expected default prompt to include runtime scheduling guidance, got prompt:\n%s", prompt)
	}
}

func TestEnsureRuntimeSchedulingGuidanceIsIdempotent(t *testing.T) {
	base := "You are a helpful agent."
	got := ensureRuntimeSchedulingGuidance(base)
	if !strings.Contains(got, runtimeSchedulingGuidanceHeader) {
		t.Fatalf("expected scheduling guidance to be appended, got:\n%s", got)
	}
	if again := ensureRuntimeSchedulingGuidance(got); again != got {
		t.Fatalf("expected scheduling guidance helper to be idempotent:\n%s", again)
	}
}

func TestEnsureRuntimeSchedulingGuidanceReplacesLegacyBlock(t *testing.T) {
	legacy := strings.Join([]string{
		"You are a helpful agent.",
		"",
		runtimeSchedulingGuidanceHeader,
		"- Airstore handles timers, sleeps, and future wakes outside your internal loop.",
		"- If work should resume later, say so explicitly in your final response, including the desired delay and what should happen on wake. The worker and BAML will classify that response and schedule the wake for you.",
		"",
		runtimeViewSchemaGuidanceHeader,
		"- view=Deals; columns=monthly_rent",
	}, "\n")

	got := ensureRuntimeSchedulingGuidance(legacy)

	if strings.Contains(got, "what should happen on wake") {
		t.Fatalf("expected legacy wake-request copy to be removed, got:\n%s", got)
	}
	if !strings.Contains(got, "NEVER create your own timers") {
		t.Fatalf("expected new declarative scheduling guidance, got:\n%s", got)
	}
	if strings.Count(got, runtimeSchedulingGuidanceHeader) != 1 {
		t.Fatalf("expected exactly one scheduling guidance block, got:\n%s", got)
	}
	if !strings.Contains(got, runtimeViewSchemaGuidanceHeader) {
		t.Fatalf("expected view schema context to be preserved, got:\n%s", got)
	}
}

type viewSchemaBackend struct {
	repository.BackendRepository
	profile *types.AgentProfile
	views   []*types.View
}

func (b *viewSchemaBackend) GetAgentProfile(_ context.Context, _ uint, _ string) (*types.AgentProfile, error) {
	return b.profile, nil
}

func (b *viewSchemaBackend) ListViews(_ context.Context, _ uint) ([]*types.View, error) {
	return b.views, nil
}

func TestLoadViewOutputSchemaContextFindsAgentBoundTables(t *testing.T) {
	backend := &viewSchemaBackend{
		profile: &types.AgentProfile{
			ID:       "agent-1",
			AgentKey: "sales-agent",
			Name:     "Sales Agent",
		},
		views: []*types.View{{
			ID:   "view-1",
			Name: "Sales Dashboard",
			Definition: types.ViewDefinition{
				Agents: []string{"other-agent"},
				Sheets: []types.SheetSpec{{
					ID:   "sheet-1",
					Name: "Pipeline",
					Components: []types.ComponentSpec{
						{
							ID:    "table-1",
							Type:  types.ComponentTypeTable,
							Title: "Outbound Emails",
							DataSource: &types.DataSource{
								AgentIDs:    []string{"sales-agent"},
								ArtifactKey: "sales-email",
								OutputType:  types.TaskOutputTypeEmail,
								Transform: []types.TransformRule{
									{Column: "recipient", Source: "data.to", Type: "email"},
									{Column: "status", Source: "metadata.status", Type: "status"},
								},
							},
							Config: map[string]any{
								"columns": []map[string]any{
									{"key": "recipient", "label": "Recipient", "type": "email"},
									{"key": "status", "label": "Status", "type": "status", "options": []map[string]any{
										{"value": "draft", "color": "yellow"},
										{"value": "sent", "color": "green"},
									}},
								},
							},
						},
						{
							ID:    "table-2",
							Type:  types.ComponentTypeTable,
							Title: "Ignore Me",
							DataSource: &types.DataSource{
								AgentIDs: []string{"another-agent"},
							},
							Config: map[string]any{
								"columns": []map[string]any{
									{"key": "title", "label": "Title", "type": "text"},
								},
							},
						},
					},
				}},
			},
		}},
	}
	agentID := "agent-1"
	contexts, err := types.LoadViewOutputSchemaContexts(context.Background(), backend, 7, agentID)
	if err != nil {
		t.Fatalf("LoadViewOutputSchemaContexts: %v", err)
	}
	if got, want := len(contexts), 1; got != want {
		t.Fatalf("schema context count = %d, want %d", got, want)
	}
	if got, want := contexts[0].ComponentID, "table-1"; got != want {
		t.Fatalf("component id = %q, want %q", got, want)
	}
	if got, want := contexts[0].ArtifactKey, "sales-email"; got != want {
		t.Fatalf("artifact key = %q, want %q", got, want)
	}
	if got, want := contexts[0].OutputType, types.TaskOutputTypeEmail; got != want {
		t.Fatalf("output type = %q, want %q", got, want)
	}
	if got, want := len(contexts[0].Columns), 2; got != want {
		t.Fatalf("column count = %d, want %d", got, want)
	}
	if got, want := contexts[0].Columns[0].Key, "recipient"; got != want {
		t.Fatalf("first column key = %q, want %q", got, want)
	}
	if got, want := contexts[0].Columns[1].Options[0].Value, "draft"; got != want {
		t.Fatalf("status option value = %q, want %q", got, want)
	}
	if got, want := contexts[0].TransformHints, []string{"data.to", "metadata.status"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("transform hints = %#v, want %#v", got, want)
	}
}

func TestAppendSchemaGuidanceAddsSchemaBlock(t *testing.T) {
	contexts := []types.ViewOutputSchemaContext{{
		ViewID:         "view-1",
		ViewName:       "Sales Dashboard",
		SheetID:        "sheet-1",
		SheetName:      "Pipeline",
		ComponentID:    "table-1",
		ComponentTitle: "Outbound Emails",
		ArtifactKey:    "sales-email",
		OutputType:     types.TaskOutputTypeEmail,
		Columns: []types.ViewOutputSchemaColumn{
			{Key: "recipient", Label: "Recipient", Type: "email"},
			{Key: "status", Label: "Status", Type: "status"},
		},
	}}

	prompt := appendSchemaGuidance("You are a helpful agent.", contexts)

	if !strings.Contains(prompt, runtimeViewSchemaGuidanceHeader) {
		t.Fatalf("expected schema guidance header in prompt, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "artifact_key=sales-email") {
		t.Fatalf("expected artifact key guidance in prompt, got:\n%s", prompt)
	}

	// Verify contexts round-trip through JSON (execution policy path).
	body, err := json.Marshal(contexts)
	if err != nil {
		t.Fatalf("marshal contexts: %v", err)
	}
	var decoded []types.ViewOutputSchemaContext
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal contexts: %v", err)
	}
	if got, want := len(decoded), 1; got != want {
		t.Fatalf("decoded schema context count = %d, want %d", got, want)
	}
	if got, want := decoded[0].ComponentID, "table-1"; got != want {
		t.Fatalf("decoded component id = %q, want %q", got, want)
	}
}

func TestApplyDispatchPayloadIncludesResumeMetadata(t *testing.T) {
	task := &types.AgentTask{
		PayloadJSON: map[string]any{
			"message": "original prompt",
		},
	}

	applyDispatchPayload(task, map[string]any{
		types.OrchestrationOutboxPayloadDispatchPrompt:        "wake prompt",
		types.OrchestrationOutboxPayloadResumeSession:         true,
		types.OrchestrationOutboxPayloadResumeExcludeRunID:    "run-prev",
		types.OrchestrationOutboxPayloadResumeCheckpointRunID: "run-prev",
	}, 2)

	if got := task.PayloadJSON["message"]; got != "wake prompt" {
		t.Fatalf("message override = %#v, want wake prompt", got)
	}
	if got := task.PayloadJSON["prompt"]; got != "wake prompt" {
		t.Fatalf("prompt override = %#v, want wake prompt", got)
	}
	if got := task.PayloadJSON[types.OrchestrationOutboxPayloadResumeSession]; got != true {
		t.Fatalf("resume_session = %#v, want true", got)
	}
	if got := task.PayloadJSON[types.OrchestrationOutboxPayloadResumeExcludeRunID]; got != "run-prev" {
		t.Fatalf("resume_exclude_run_id = %#v, want run-prev", got)
	}
	if got := task.PayloadJSON[types.OrchestrationOutboxPayloadResumeCheckpointRunID]; got != "run-prev" {
		t.Fatalf("resume_checkpoint_run_id = %#v, want run-prev", got)
	}
	if got := task.PayloadJSON[types.OrchestrationOutboxPayloadDispatchAttempt]; got != 2 {
		t.Fatalf("dispatch_attempt = %#v, want 2", got)
	}
}

func TestRunInputPromptPrefersDispatchPrompt(t *testing.T) {
	payload := map[string]any{
		"message": "original task instruction",
		"prompt":  "wake-specific follow-up prompt",
	}

	if got := runInputPrompt(payload); got != "wake-specific follow-up prompt" {
		t.Fatalf("runInputPrompt = %q, want wake-specific follow-up prompt", got)
	}
}

func TestDispatchPromptFromValuesWrapsWakeFollowUpPrompt(t *testing.T) {
	got := dispatchPromptFromValues(map[string]any{
		types.OrchestrationOutboxPayloadDispatchPrompt:     "plain dispatch prompt",
		types.OrchestrationOutboxPayloadWakeFollowUpPrompt: "Check Gmail thread 123 for replies and report back.",
	})

	if !strings.Contains(got, wakeDispatchReminder) {
		t.Fatalf("expected wake dispatch reminder in prompt, got:\n%s", got)
	}
	if !strings.Contains(got, "Check Gmail thread 123 for replies and report back.") {
		t.Fatalf("expected original follow-up prompt in wrapped prompt, got:\n%s", got)
	}
}

func TestWakeBackoffDelayHonorsRequestedDelay(t *testing.T) {
	if got := wakeBackoffDelay(0, 2880); got != 2880 {
		t.Fatalf("wakeBackoffDelay = %d, want 2880", got)
	}
	if got := wakeBackoffDelay(3, 0); got != 5 {
		t.Fatalf("wakeBackoffDelay with invalid delay = %d, want 5", got)
	}
}

func TestIsRetryableError(t *testing.T) {
	retryable := []string{
		"session abc checkpoint for run xyz not durable yet",
		"session ID abc is already in use",
		"session still held by worker",
		"TOOMANYREQUESTS: Rate exceeded",
		"failed to fetch image: GET https://public.ecr.aws/v2/foo: TOOMANYREQUESTS",
		"failed to prepare rootfs from image: mount archive: failed to fetch image",
		"connection refused",
		"dial tcp 10.0.0.1:443: i/o timeout",
		"TLS handshake timeout",
		"503 Service Unavailable",
		"rate limit hit, please retry",
		"request throttled by upstream",
		"failed to pull image: temporary failure in name resolution",
	}
	for _, msg := range retryable {
		if !isRetryableError(msg) {
			t.Fatalf("expected retryable: %s", msg)
		}
	}

	permanent := []string{
		"agent not found",
		"invalid configuration",
		"permission denied",
		"syntax error in prompt",
	}
	for _, msg := range permanent {
		if isRetryableError(msg) {
			t.Fatalf("expected permanent: %s", msg)
		}
	}
}

func TestSkillNamesFromConfigHandlesStringSlices(t *testing.T) {
	config := map[string]any{
		agentConfigKeySkills: []string{"triage", " review ", "triage", ""},
	}

	if got, want := skillNamesFromConfig(config), []string{"triage", "review"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("skillNamesFromConfig = %#v, want %#v", got, want)
	}
}

func TestSkillNamesFromConfigHandlesInterfaceSlices(t *testing.T) {
	config := map[string]any{
		agentConfigKeySkills: []any{"triage", " review ", "triage", "", 42},
	}

	if got, want := skillNamesFromConfig(config), []string{"triage", "review"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("skillNamesFromConfig = %#v, want %#v", got, want)
	}
}

type acceptTaskInputBackend struct {
	repository.BackendRepository
	task                *types.AgentTask
	outputs             map[string]*types.TaskOutput
	appendedInputs      []*types.TaskInput
	resolvedBlocker     *types.TaskBlockerResolution
	statusUpdateErrByID map[string]error
}

func (b *acceptTaskInputBackend) GetTask(_ context.Context, _ uint, taskID string) (*types.AgentTask, error) {
	if b.task != nil && b.task.ID == taskID {
		return b.task, nil
	}
	return nil, &types.ErrAgentTaskNotFound{ID: taskID}
}

func (b *acceptTaskInputBackend) GetTaskOutput(_ context.Context, _ uint, outputID string) (*types.TaskOutput, error) {
	if output, ok := b.outputs[outputID]; ok {
		return output, nil
	}
	return nil, &types.ErrTaskOutputNotFound{ID: outputID}
}

func (b *acceptTaskInputBackend) ListTaskOutputs(_ context.Context, _ uint, taskID string) ([]*types.TaskOutput, error) {
	var outputs []*types.TaskOutput
	for _, output := range b.outputs {
		if output != nil && output.TaskID == taskID {
			outputs = append(outputs, output)
		}
	}
	return outputs, nil
}

func (b *acceptTaskInputBackend) UpdateTaskOutputStatus(_ context.Context, _ uint, outputID string, status string) error {
	if err := b.statusUpdateErrByID[outputID]; err != nil {
		return err
	}
	output, ok := b.outputs[outputID]
	if !ok {
		return &types.ErrTaskOutputNotFound{ID: outputID}
	}
	output.Status = status
	return nil
}

func (b *acceptTaskInputBackend) AppendTaskInput(_ context.Context, input *types.TaskInput) error {
	copied := *input
	b.appendedInputs = append(b.appendedInputs, &copied)
	return nil
}

func (b *acceptTaskInputBackend) ResolveCurrentTaskBlocker(_ context.Context, _ uint, taskID string, resolution *types.TaskBlockerResolution) (*types.TaskBlocker, error) {
	if b.task == nil || b.task.ID != taskID {
		return nil, &types.ErrAgentTaskNotFound{ID: taskID}
	}
	if resolution != nil {
		copied := *resolution
		if len(resolution.ResolutionJSON) > 0 {
			copied.ResolutionJSON = make(map[string]any, len(resolution.ResolutionJSON))
			for key, value := range resolution.ResolutionJSON {
				copied.ResolutionJSON[key] = value
			}
		}
		b.resolvedBlocker = &copied
	}
	b.task.CurrentBlocker = nil
	b.task.CurrentBlockerID = nil
	b.task.InputKind = ""
	b.task.WaitingSummary = nil
	return &types.TaskBlocker{ID: "blocker-1", Status: types.TaskBlockerStatusResolved}, nil
}

func TestAcceptTaskInputRejectsCrossTaskItemDecisions(t *testing.T) {
	backend := &acceptTaskInputBackend{
		task: &types.AgentTask{
			ID:          "task-1",
			WorkspaceID: 7,
		},
		outputs: map[string]*types.TaskOutput{
			"out-foreign": {
				ID:     "out-foreign",
				TaskID: "task-2",
				Title:  "Foreign approval item",
				Status: types.TaskOutputStatusPending,
			},
		},
	}
	service := &AgentService{backend: backend}

	_, err := service.AcceptTaskInput(
		context.Background(),
		7,
		"task-1",
		types.InputKindApproveReject,
		nil,
		"",
		"idem-1",
		[]types.ItemDecision{{
			OutputID: "out-foreign",
			Action:   types.TaskInputActionApprove,
		}},
	)
	if err == nil {
		t.Fatal("expected invalid task input error")
	}
	var invalidInputErr *types.ErrInvalidTaskInput
	if !errors.As(err, &invalidInputErr) {
		t.Fatalf("expected invalid task input error, got %T: %v", err, err)
	}
	if want := "does not belong to task task-1"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want substring %q", err.Error(), want)
	}
	if got := len(backend.appendedInputs); got != 0 {
		t.Fatalf("append count = %d, want 0", got)
	}
	if got := backend.outputs["out-foreign"].Status; got != types.TaskOutputStatusPending {
		t.Fatalf("foreign output status = %q, want pending", got)
	}
}

func TestAcceptTaskInputRollsBackItemStatusesOnUpdateError(t *testing.T) {
	backend := &acceptTaskInputBackend{
		task: &types.AgentTask{
			ID:          "task-1",
			WorkspaceID: 7,
		},
		outputs: map[string]*types.TaskOutput{
			"out-1": {
				ID:     "out-1",
				TaskID: "task-1",
				Title:  "First item",
				Status: types.TaskOutputStatusPending,
			},
			"out-2": {
				ID:     "out-2",
				TaskID: "task-1",
				Title:  "Second item",
				Status: types.TaskOutputStatusPending,
			},
		},
		statusUpdateErrByID: map[string]error{
			"out-2": fmt.Errorf("boom"),
		},
	}
	service := &AgentService{backend: backend}

	_, err := service.AcceptTaskInput(
		context.Background(),
		7,
		"task-1",
		types.InputKindApproveReject,
		nil,
		"",
		"idem-2",
		[]types.ItemDecision{
			{OutputID: "out-1", Action: types.TaskInputActionApprove},
			{OutputID: "out-2", Action: types.TaskInputActionApprove},
		},
	)
	if err == nil {
		t.Fatal("expected status update error")
	}
	if got := backend.outputs["out-1"].Status; got != types.TaskOutputStatusPending {
		t.Fatalf("out-1 status = %q, want pending after rollback", got)
	}
	if got := backend.outputs["out-2"].Status; got != types.TaskOutputStatusPending {
		t.Fatalf("out-2 status = %q, want pending after failure", got)
	}
	if got := len(backend.appendedInputs); got != 0 {
		t.Fatalf("append count = %d, want 0", got)
	}
}

func TestAcceptTaskInputAutoAppliesPendingOutputsForApproveReject(t *testing.T) {
	backend := &acceptTaskInputBackend{
		task: &types.AgentTask{
			ID:          "task-1",
			WorkspaceID: 7,
		},
		outputs: map[string]*types.TaskOutput{
			"out-old": {
				ID:        "out-old",
				TaskID:    "task-1",
				Title:     "Older approval item",
				Status:    types.TaskOutputStatusPending,
				CreatedAt: time.Unix(10, 0),
				Metadata: map[string]any{
					types.TaskOutputMetadataBlockingKind: types.TaskOutputBlockingKindApproval,
					types.TaskOutputMetadataInputKind:    string(types.InputKindApproveReject),
					types.TaskOutputMetadataWaitGroupID:  "wait-1",
					types.TaskOutputMetadataApprovalUI:   true,
				},
			},
			"out-pending": {
				ID:        "out-pending",
				TaskID:    "task-1",
				Title:     "Draft outreach email",
				Status:    types.TaskOutputStatusPending,
				CreatedAt: time.Unix(20, 0),
				Metadata: map[string]any{
					types.TaskOutputMetadataBlockingKind: types.TaskOutputBlockingKindApproval,
					types.TaskOutputMetadataInputKind:    string(types.InputKindApproveReject),
					types.TaskOutputMetadataWaitGroupID:  "wait-2",
					types.TaskOutputMetadataApprovalUI:   true,
				},
			},
			"out-active": {
				ID:     "out-active",
				TaskID: "task-1",
				Title:  "Sent outreach email",
				Status: types.TaskOutputStatusActive,
			},
		},
	}
	service := &AgentService{backend: backend}
	approve := types.TaskInputActionApprove

	_, err := service.AcceptTaskInput(
		context.Background(),
		7,
		"task-1",
		types.InputKindApproveReject,
		&approve,
		"",
		"idem-3",
		nil,
	)
	if err != nil {
		t.Fatalf("AcceptTaskInput returned error: %v", err)
	}
	if got := backend.outputs["out-pending"].Status; got != types.TaskOutputStatusApproved {
		t.Fatalf("pending output status = %q, want approved", got)
	}
	if got := backend.outputs["out-old"].Status; got != types.TaskOutputStatusPending {
		t.Fatalf("older wait-group output status = %q, want pending", got)
	}
	if got := backend.outputs["out-active"].Status; got != types.TaskOutputStatusActive {
		t.Fatalf("active output status = %q, want active", got)
	}
	if got := len(backend.appendedInputs); got != 1 {
		t.Fatalf("append count = %d, want 1", got)
	}
	if msg := backend.appendedInputs[0].Message; !strings.Contains(msg, "Approved:") || !strings.Contains(msg, "Draft outreach email") {
		t.Fatalf("appended input message = %q, want approval summary for pending output", msg)
	}
}

func TestAcceptTaskInputFreeTextSupersedesPendingApprovalOutputs(t *testing.T) {
	backend := &acceptTaskInputBackend{
		task: &types.AgentTask{
			ID:          "task-1",
			WorkspaceID: 7,
			State:       types.AgentTaskStateWaiting,
			InputKind:   types.InputKindApproveReject,
		},
		outputs: map[string]*types.TaskOutput{
			"out-old": {
				ID:        "out-old",
				TaskID:    "task-1",
				Title:     "Older approval draft",
				Status:    types.TaskOutputStatusPending,
				CreatedAt: time.Unix(10, 0),
				Metadata: map[string]any{
					types.TaskOutputMetadataBlockingKind: types.TaskOutputBlockingKindApproval,
					types.TaskOutputMetadataInputKind:    string(types.InputKindApproveReject),
					types.TaskOutputMetadataWaitGroupID:  "wait-1",
					types.TaskOutputMetadataApprovalUI:   true,
				},
			},
			"out-current": {
				ID:        "out-current",
				TaskID:    "task-1",
				Title:     "Current approval draft",
				Status:    types.TaskOutputStatusPending,
				CreatedAt: time.Unix(20, 0),
				Metadata: map[string]any{
					types.TaskOutputMetadataBlockingKind: types.TaskOutputBlockingKindApproval,
					types.TaskOutputMetadataInputKind:    string(types.InputKindApproveReject),
					types.TaskOutputMetadataWaitGroupID:  "wait-2",
					types.TaskOutputMetadataApprovalUI:   true,
				},
			},
			"out-unrelated": {
				ID:        "out-unrelated",
				TaskID:    "task-1",
				Title:     "Pending document upload",
				Status:    types.TaskOutputStatusPending,
				CreatedAt: time.Unix(30, 0),
			},
		},
	}
	service := &AgentService{backend: backend}

	_, err := service.AcceptTaskInput(
		context.Background(),
		7,
		"task-1",
		"",
		nil,
		"Please revise the tone and tighten the CTA.",
		"idem-4",
		nil,
	)
	if err != nil {
		t.Fatalf("AcceptTaskInput returned error: %v", err)
	}
	if got := backend.outputs["out-old"].Status; got != types.TaskOutputStatusRejected {
		t.Fatalf("older approval status = %q, want rejected", got)
	}
	if got := backend.outputs["out-current"].Status; got != types.TaskOutputStatusRejected {
		t.Fatalf("current approval status = %q, want rejected", got)
	}
	if got := backend.outputs["out-unrelated"].Status; got != types.TaskOutputStatusPending {
		t.Fatalf("unrelated pending status = %q, want pending", got)
	}
	if got := len(backend.appendedInputs); got != 1 {
		t.Fatalf("append count = %d, want 1", got)
	}
	if got := backend.appendedInputs[0].Kind; got != types.InputKindFreeText {
		t.Fatalf("input kind = %q, want free_text", got)
	}
	if msg := backend.appendedInputs[0].Message; !strings.Contains(msg, "Return an updated version for approval unless the user explicitly approves proceeding.") {
		t.Fatalf("appended input message = %q, want approval revision guardrail", msg)
	}
}

func TestAcceptTaskInputUsesCurrentBlockerArtifactsForAutoApproval(t *testing.T) {
	backend := &acceptTaskInputBackend{
		task: &types.AgentTask{
			ID:               "task-1",
			WorkspaceID:      7,
			State:            types.AgentTaskStateWaiting,
			InputKind:        types.InputKindApproveReject,
			CurrentBlockerID: stringPtr("blocker-1"),
			CurrentBlocker: &types.TaskBlocker{
				ID:        "blocker-1",
				Kind:      types.TaskBlockerKindApproval,
				InputKind: types.InputKindApproveReject,
				Status:    types.TaskBlockerStatusOpen,
				OutputIDs: []string{"out-current"},
			},
		},
		outputs: map[string]*types.TaskOutput{
			"out-old": {
				ID:        "out-old",
				TaskID:    "task-1",
				Title:     "Older approval draft",
				Status:    types.TaskOutputStatusPending,
				CreatedAt: time.Unix(10, 0),
				Metadata: map[string]any{
					types.TaskOutputMetadataBlockerID:    "blocker-old",
					types.TaskOutputMetadataBlockingKind: types.TaskOutputBlockingKindApproval,
					types.TaskOutputMetadataInputKind:    string(types.InputKindApproveReject),
				},
			},
			"out-current": {
				ID:        "out-current",
				TaskID:    "task-1",
				Title:     "Current approval draft",
				Status:    types.TaskOutputStatusPending,
				CreatedAt: time.Unix(20, 0),
				Metadata: map[string]any{
					types.TaskOutputMetadataBlockerID:    "blocker-1",
					types.TaskOutputMetadataBlockingKind: types.TaskOutputBlockingKindApproval,
					types.TaskOutputMetadataInputKind:    string(types.InputKindApproveReject),
				},
			},
		},
	}
	service := &AgentService{backend: backend}
	approve := types.TaskInputActionApprove

	_, err := service.AcceptTaskInput(
		context.Background(),
		7,
		"task-1",
		types.InputKindApproveReject,
		&approve,
		"",
		"idem-blocker-approve",
		nil,
	)
	if err != nil {
		t.Fatalf("AcceptTaskInput returned error: %v", err)
	}
	if got := backend.outputs["out-current"].Status; got != types.TaskOutputStatusApproved {
		t.Fatalf("current blocker output status = %q, want approved", got)
	}
	if got := backend.outputs["out-old"].Status; got != types.TaskOutputStatusPending {
		t.Fatalf("older blocker output status = %q, want pending", got)
	}
	if backend.resolvedBlocker == nil {
		t.Fatal("expected current blocker to be resolved")
	}
	if got := backend.resolvedBlocker.Status; got != types.TaskBlockerStatusResolved {
		t.Fatalf("blocker status = %q, want resolved", got)
	}
}

func TestAcceptTaskInputSupersedesOnlyCurrentBlockerArtifacts(t *testing.T) {
	backend := &acceptTaskInputBackend{
		task: &types.AgentTask{
			ID:               "task-1",
			WorkspaceID:      7,
			State:            types.AgentTaskStateWaiting,
			InputKind:        types.InputKindApproveReject,
			CurrentBlockerID: stringPtr("blocker-1"),
			CurrentBlocker: &types.TaskBlocker{
				ID:        "blocker-1",
				Kind:      types.TaskBlockerKindApproval,
				InputKind: types.InputKindApproveReject,
				Status:    types.TaskBlockerStatusOpen,
				OutputIDs: []string{"out-current"},
			},
		},
		outputs: map[string]*types.TaskOutput{
			"out-old": {
				ID:        "out-old",
				TaskID:    "task-1",
				Title:     "Older approval draft",
				Status:    types.TaskOutputStatusPending,
				CreatedAt: time.Unix(10, 0),
				Metadata: map[string]any{
					types.TaskOutputMetadataBlockerID:    "blocker-old",
					types.TaskOutputMetadataBlockingKind: types.TaskOutputBlockingKindApproval,
					types.TaskOutputMetadataInputKind:    string(types.InputKindApproveReject),
				},
			},
			"out-current": {
				ID:        "out-current",
				TaskID:    "task-1",
				Title:     "Current approval draft",
				Status:    types.TaskOutputStatusPending,
				CreatedAt: time.Unix(20, 0),
				Metadata: map[string]any{
					types.TaskOutputMetadataBlockerID:    "blocker-1",
					types.TaskOutputMetadataBlockingKind: types.TaskOutputBlockingKindApproval,
					types.TaskOutputMetadataInputKind:    string(types.InputKindApproveReject),
				},
			},
		},
	}
	service := &AgentService{backend: backend}

	_, err := service.AcceptTaskInput(
		context.Background(),
		7,
		"task-1",
		"",
		nil,
		"Please revise the CTA.",
		"idem-blocker-supersede",
		nil,
	)
	if err != nil {
		t.Fatalf("AcceptTaskInput returned error: %v", err)
	}
	if got := backend.outputs["out-current"].Status; got != types.TaskOutputStatusRejected {
		t.Fatalf("current blocker output status = %q, want rejected", got)
	}
	if got := backend.outputs["out-old"].Status; got != types.TaskOutputStatusPending {
		t.Fatalf("older blocker output status = %q, want pending", got)
	}
	if backend.resolvedBlocker == nil {
		t.Fatal("expected current blocker to be superseded")
	}
	if got := backend.resolvedBlocker.Status; got != types.TaskBlockerStatusSuperseded {
		t.Fatalf("blocker status = %q, want superseded", got)
	}
}

func stringPtr(value string) *string {
	return &value
}

func TestNamespaceWorkspaceDirByViewSetsSharedProjectRoot(t *testing.T) {
	tests := []struct {
		name     string
		config   map[string]any
		payload  map[string]any
		expected string
	}{
		{
			name:     "sets shared project dir when view present",
			config:   map[string]any{agentConfigKeyWorkspaceDir: "/workspace/agents/my-agent"},
			payload:  map[string]any{"source_view_id": "view-123"},
			expected: "/workspace/projects/view-123",
		},
		{
			name:     "no-op when no source_view_id",
			config:   map[string]any{agentConfigKeyWorkspaceDir: "/workspace/agents/my-agent"},
			payload:  map[string]any{},
			expected: "/workspace/agents/my-agent",
		},
		{
			name:     "idempotent if already set",
			config:   map[string]any{agentConfigKeyWorkspaceDir: "/workspace/projects/view-123"},
			payload:  map[string]any{"source_view_id": "view-123"},
			expected: "/workspace/projects/view-123",
		},
		{
			name:     "overrides agent dir with project dir",
			config:   map[string]any{agentConfigKeyWorkspaceDir: "/workspace/agents/other-agent"},
			payload:  map[string]any{"source_view_id": "view-abc"},
			expected: "/workspace/projects/view-abc",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			namespaceWorkspaceDirByView(tt.config, tt.payload)
			got := tt.config[agentConfigKeyWorkspaceDir]
			if got != tt.expected {
				t.Fatalf("workspace_dir = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestFormatViewContextRendersEntries(t *testing.T) {
	entries := []types.ViewContextEntry{
		{EntryType: types.ViewContextEntryCompaction, Content: "- Always use formal tone"},
		{EntryType: types.ViewContextEntryFeedback, Content: "Add phone numbers to outreach"},
	}
	result := formatViewContext(entries)
	if !strings.Contains(result, viewContextPromptHeader) {
		t.Fatalf("expected header in output, got:\n%s", result)
	}
	if !strings.Contains(result, "Always use formal tone") {
		t.Fatalf("expected compaction content, got:\n%s", result)
	}
	if !strings.Contains(result, "- Add phone numbers") {
		t.Fatalf("expected feedback entry, got:\n%s", result)
	}
}

func TestFormatViewContextEmptyReturnsEmpty(t *testing.T) {
	if got := formatViewContext(nil); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}
