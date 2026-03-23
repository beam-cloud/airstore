package worker

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
)

func TestNormalizeSourceWatchRequestsFallsBackToTrackedOutputs(t *testing.T) {
	reason := "Check for replies to the outreach email."
	tracker := trackedEmailOutput(
		"out-1",
		"thread-1",
		"msg-1",
		"luke@beam.cloud",
		"Quick question about your dev environments",
	)

	got := normalizeSourceWatchRequests(nil, tracker, &reason)
	if got == nil || len(got) != 1 {
		t.Fatalf("normalizeSourceWatchRequests() len = %d, want 1", len(got))
	}
	if got[0].Integration != string(types.SourceGmail) {
		t.Fatalf("integration = %q, want %q", got[0].Integration, types.SourceGmail)
	}
	if got[0].ThreadID != "thread-1" {
		t.Fatalf("thread_id = %q, want %q", got[0].ThreadID, "thread-1")
	}
	if got[0].MessageID != "msg-1" {
		t.Fatalf("message_id = %q, want %q", got[0].MessageID, "msg-1")
	}
	if got[0].SourceOutputID != "out-1" {
		t.Fatalf("source_output_id = %q, want %q", got[0].SourceOutputID, "out-1")
	}
	if got[0].EntityLabel != "Quick question about your dev environments" {
		t.Fatalf("entity_label = %q, want subject-derived label", got[0].EntityLabel)
	}
}

func TestNormalizeSourceWatchRequestsPrefersClassifierRequestsOverTrackedFallbacks(t *testing.T) {
	reason := "Check for replies to cold outreach email sent to luke@beam.cloud"
	entityLabel := "Reply from luke@beam.cloud to Beam outreach"
	threadID := "thread-classifier"

	tracker := trackedEmailOutput(
		"out-1",
		"thread-tracker",
		"msg-1",
		"luke@beam.cloud",
		"Quick question about your dev environments",
	)

	got := normalizeSourceWatchRequests([]signaltypes.SourceWatchRequest{{
		Integration:  string(types.SourceGmail),
		Reason:       &reason,
		Entity_label: &entityLabel,
		Thread_id:    &threadID,
	}}, tracker, &reason)
	if got == nil || len(got) != 1 {
		t.Fatalf("normalizeSourceWatchRequests() len = %d, want 1", len(got))
	}
	if got[0].ThreadID != threadID {
		t.Fatalf("thread_id = %q, want %q", got[0].ThreadID, threadID)
	}
	if got[0].EntityLabel != entityLabel {
		t.Fatalf("entity_label = %q, want %q", got[0].EntityLabel, entityLabel)
	}
}

func TestNormalizeSourceWatchRequestsMergesTrackedEmailIdentity(t *testing.T) {
	reason := "Check for replies to cold outreach email sent to luke@beam.cloud"
	entityKey := "luke@beam.cloud"
	entityLabel := "Reply from luke@beam.cloud to Beam outreach"
	query := "from:luke@beam.cloud"

	tracker := trackedEmailOutput(
		"out-1",
		"thread-tracker",
		"msg-tracker",
		"luke@beam.cloud",
		"Quick question about your dev environments",
	)

	got := normalizeSourceWatchRequests([]signaltypes.SourceWatchRequest{{
		Integration:          string(types.SourceGmail),
		Reason:               &reason,
		Query:                &query,
		Entity_key:           &entityKey,
		Entity_label:         &entityLabel,
		Include_message_body: true,
	}}, tracker, &reason)
	if got == nil || len(got) != 1 {
		t.Fatalf("normalizeSourceWatchRequests() len = %d, want 1", len(got))
	}
	if got[0].ThreadID != "thread-tracker" {
		t.Fatalf("thread_id = %q, want %q", got[0].ThreadID, "thread-tracker")
	}
	if got[0].MessageID != "msg-tracker" {
		t.Fatalf("message_id = %q, want %q", got[0].MessageID, "msg-tracker")
	}
	if got[0].SourceOutputID != "out-1" {
		t.Fatalf("source_output_id = %q, want %q", got[0].SourceOutputID, "out-1")
	}
	if got[0].Query != query {
		t.Fatalf("query = %q, want %q", got[0].Query, query)
	}
	if got[0].EntityLabel != entityLabel {
		t.Fatalf("entity_label = %q, want %q", got[0].EntityLabel, entityLabel)
	}
}

func TestNormalizeSourceWatchRequestsSkipsAmbiguousTrackedMatches(t *testing.T) {
	reason := "Check for replies to cold outreach email sent to luke@beam.cloud"
	entityKey := "luke@beam.cloud"
	query := "from:luke@beam.cloud"

	tracker := &taskOutputTracker{}
	rememberTrackedEmailOutput(tracker, "out-1", "thread-1", "msg-1", "luke@beam.cloud", "First subject")
	rememberTrackedEmailOutput(tracker, "out-2", "thread-2", "msg-2", "luke@beam.cloud", "Second subject")

	got := normalizeSourceWatchRequests([]signaltypes.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		Reason:      &reason,
		Query:       &query,
		Entity_key:  &entityKey,
	}}, tracker, &reason)
	if got == nil || len(got) != 1 {
		t.Fatalf("normalizeSourceWatchRequests() len = %d, want 1", len(got))
	}
	if got[0].ThreadID != "" {
		t.Fatalf("thread_id = %q, want empty for ambiguous match", got[0].ThreadID)
	}
	if got[0].MessageID != "" {
		t.Fatalf("message_id = %q, want empty for ambiguous match", got[0].MessageID)
	}
	if got[0].SourceOutputID != "" {
		t.Fatalf("source_output_id = %q, want empty for ambiguous match", got[0].SourceOutputID)
	}
}

func TestNormalizeSourceWatchRequestsAcceptsDuplicateTrackedSameThread(t *testing.T) {
	reason := "Check for replies to cold outreach email sent to luke@beam.cloud"
	entityKey := "luke@beam.cloud"
	query := "from:luke@beam.cloud"

	tracker := &taskOutputTracker{}
	rememberTrackedEmailOutput(tracker, "out-1", "thread-1", "msg-1", "luke@beam.cloud", "Quick question about your dev environments")
	rememberTrackedEmailOutput(tracker, "out-2", "thread-1", "msg-1", "luke@beam.cloud", "Quick question about your dev environments")

	got := normalizeSourceWatchRequests([]signaltypes.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		Reason:      &reason,
		Query:       &query,
		Entity_key:  &entityKey,
	}}, tracker, &reason)
	if got == nil || len(got) != 1 {
		t.Fatalf("normalizeSourceWatchRequests() len = %d, want 1", len(got))
	}
	if got[0].ThreadID != "thread-1" {
		t.Fatalf("thread_id = %q, want %q", got[0].ThreadID, "thread-1")
	}
	if got[0].MessageID != "msg-1" {
		t.Fatalf("message_id = %q, want %q", got[0].MessageID, "msg-1")
	}
}

func trackedEmailOutput(
	outputID, threadID, messageID, recipient, subject string,
) *taskOutputTracker {
	tracker := &taskOutputTracker{}
	rememberTrackedEmailOutput(tracker, outputID, threadID, messageID, recipient, subject)
	return tracker
}

func rememberTrackedEmailOutput(
	tracker *taskOutputTracker,
	outputID, threadID, messageID, recipient, subject string,
) {
	tracker.RememberWithID(outputCandidate{
		OutputType: types.TaskOutputTypeEmail,
		Title:      subject,
		Data: map[string]any{
			"thread_id":  threadID,
			"message_id": messageID,
			"recipient":  recipient,
			"subject":    subject,
		},
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey: "email-sent",
		},
	}, outputID)
}
