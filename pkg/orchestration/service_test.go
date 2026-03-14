package orchestration

import (
	"context"
	"reflect"
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
