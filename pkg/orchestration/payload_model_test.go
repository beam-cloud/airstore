package orchestration

import (
	"reflect"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestTaskCommandPayloadRoundTrip(t *testing.T) {
	deliver := true
	payload := TaskCommandPayload{
		Message:         "Draft a reply",
		Prompt:          "Follow up with the customer",
		OriginalMessage: "Draft a reply",
		SessionID:       "session-1",
		SessionKey:      strPtr("session-key"),
		AgentID:         strPtr("agent-1"),
		TimeoutMs:       1234,
		Policy:          DefaultRunExecutionPolicy(),
		Deliver:         &deliver,
		InstanceKey:     "instance-1",
		Label:           strPtr("child"),
		SpawnedBy:       strPtr("planner"),
		Priority:        "high",
		Provider:        strPtr("anthropic"),
		Model:           strPtr("claude"),
		AgentConfig:     map[string]any{"runner": "claude"},
		Resume:          ResumeDirective{Enabled: true, ExcludeRunID: "run-old", CheckpointRunID: "run-old"},
	}

	decoded := parseTaskCommandPayload(payload.ToMap())
	if decoded.Message != payload.Message {
		t.Fatalf("message = %q, want %q", decoded.Message, payload.Message)
	}
	if decoded.Prompt != payload.Prompt {
		t.Fatalf("prompt = %q, want %q", decoded.Prompt, payload.Prompt)
	}
	if decoded.SessionID != payload.SessionID {
		t.Fatalf("session_id = %q, want %q", decoded.SessionID, payload.SessionID)
	}
	if decoded.InstanceKey != payload.InstanceKey {
		t.Fatalf("instance_key = %q, want %q", decoded.InstanceKey, payload.InstanceKey)
	}
	if decoded.Resume != payload.Resume {
		t.Fatalf("resume = %#v, want %#v", decoded.Resume, payload.Resume)
	}
	if decoded.Provider == nil || *decoded.Provider != "anthropic" {
		t.Fatalf("provider = %#v, want anthropic", decoded.Provider)
	}
	if decoded.Model == nil || *decoded.Model != "claude" {
		t.Fatalf("model = %#v, want claude", decoded.Model)
	}
	if !reflect.DeepEqual(decoded.AgentConfig, payload.AgentConfig) {
		t.Fatalf("agent config = %#v, want %#v", decoded.AgentConfig, payload.AgentConfig)
	}
}

func TestDispatchEnvelopeRoundTrip(t *testing.T) {
	envelope := DispatchEnvelope{
		TaskID:       "task-1",
		Prompt:       "Check the thread for replies.",
		RetryAttempt: 2,
		Resume: ResumeDirective{
			Enabled:         true,
			ExcludeRunID:    "run-prev",
			CheckpointRunID: "run-prev",
		},
	}

	decoded := parseDispatchEnvelope(envelope.ToMap())
	if decoded.TaskID != envelope.TaskID {
		t.Fatalf("task_id = %q, want %q", decoded.TaskID, envelope.TaskID)
	}
	if decoded.RetryAttempt != envelope.RetryAttempt {
		t.Fatalf("retry_attempt = %d, want %d", decoded.RetryAttempt, envelope.RetryAttempt)
	}
	if decoded.Resume != envelope.Resume {
		t.Fatalf("resume = %#v, want %#v", decoded.Resume, envelope.Resume)
	}
	if decoded.Prompt != envelope.Prompt {
		t.Fatalf("prompt = %q, want %q", decoded.Prompt, envelope.Prompt)
	}
}

func TestRunResultEnvelopeRoundTrip(t *testing.T) {
	envelope := RunResultEnvelope{
		TaskID:          "exec-1",
		AttemptID:       "attempt-1",
		ExitCode:        0,
		ResultKey:       "result-1",
		WaitingForInput: true,
		Wake: WakeDirective{
			DelayMinutes:   15,
			Reason:         "Wait for a reply",
			FollowUpPrompt: "Check the thread again.",
			Agenda: []*types.TaskWakeAgendaItem{
				{Seq: 1, Title: "Check thread", Reason: "Need the user's reply"},
			},
		},
		SubtaskRequests: []*types.SubtaskRequest{
			{SourceOutputID: "out-1", EntityLabel: "customer", Prompt: "Reach out", WakeDelayMinutes: 5},
		},
	}

	decoded := parseRunResultEnvelope(envelope.ToMap())
	if decoded.TaskID != envelope.TaskID {
		t.Fatalf("task_id = %q, want %q", decoded.TaskID, envelope.TaskID)
	}
	if decoded.AttemptID != envelope.AttemptID {
		t.Fatalf("attempt_id = %q, want %q", decoded.AttemptID, envelope.AttemptID)
	}
	if decoded.Wake.DelayMinutes != envelope.Wake.DelayMinutes {
		t.Fatalf("wake delay = %d, want %d", decoded.Wake.DelayMinutes, envelope.Wake.DelayMinutes)
	}
	if len(decoded.SubtaskRequests) != 1 || decoded.SubtaskRequests[0].Prompt != "Reach out" {
		t.Fatalf("subtask requests = %#v, want one preserved request", decoded.SubtaskRequests)
	}
}
