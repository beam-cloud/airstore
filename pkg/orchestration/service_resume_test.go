package orchestration

import (
	"errors"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

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
