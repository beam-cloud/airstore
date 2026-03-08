package orchestration

import (
	"context"
	"errors"
	"strings"
	"testing"

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

func TestBuildRunInputPayloadCarriesResumeCheckpoint(t *testing.T) {
	run := &types.AgentRun{
		ID:        "run-123",
		SessionID: "session-123",
		TimeoutMs: 60000,
	}

	payload := buildRunInputPayload(run, "continue please")

	if got := payload["message"]; got != "continue please" {
		t.Fatalf("message = %#v, want continue please", got)
	}
	if got := payload[types.OrchestrationOutboxPayloadResumeSession]; got != true {
		t.Fatalf("resume_session = %#v, want true", got)
	}
	if got := payload[types.OrchestrationOutboxPayloadResumeExcludeRunID]; got != "run-123" {
		t.Fatalf("resume_exclude_run_id = %#v, want run-123", got)
	}
	if got := payload[types.OrchestrationOutboxPayloadResumeCheckpointRunID]; got != "run-123" {
		t.Fatalf("resume_checkpoint_run_id = %#v, want run-123", got)
	}
}

func TestIsSessionBusyErrorMatchesCheckpointBarrier(t *testing.T) {
	if !isSessionBusyError(errors.New("session abc checkpoint for run xyz not durable yet")) {
		t.Fatal("expected checkpoint barrier error to be retryable")
	}
	if !isSessionBusyError(errors.New("session ID abc is already in use")) {
		t.Fatal("expected already-in-use error to be retryable")
	}
	if isSessionBusyError(errors.New("totally unrelated failure")) {
		t.Fatal("expected unrelated error not to be retryable")
	}
}
