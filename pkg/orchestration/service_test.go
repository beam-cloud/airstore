package orchestration

import (
	"context"
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
	if got := backend.outputs["out-old"].Status; got != types.TaskOutputStatusCancelled {
		t.Fatalf("older approval status = %q, want cancelled", got)
	}
	if got := backend.outputs["out-current"].Status; got != types.TaskOutputStatusCancelled {
		t.Fatalf("current approval status = %q, want cancelled", got)
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
	if got := backend.outputs["out-current"].Status; got != types.TaskOutputStatusCancelled {
		t.Fatalf("current blocker output status = %q, want cancelled", got)
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
