package worker

import (
	"strings"
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

func TestNormalizeSourceWatchRequestsMergesSameThreadTrackedMessages(t *testing.T) {
	reason := "Check for replies to cold outreach email sent to luke@beam.cloud"
	threadID := "thread-1"

	tracker := &taskOutputTracker{}
	rememberTrackedEmailOutputWithArtifactKey(tracker, "out-1", "email-sent", threadID, "msg-1", "luke@beam.cloud", "Quick question about your dev environments")
	rememberTrackedEmailOutputWithArtifactKey(tracker, "out-2", "gmail-reply", threadID, "msg-2", "luke@beam.cloud", "Quick question about your dev environments")

	got := normalizeSourceWatchRequests([]signaltypes.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		Reason:      &reason,
		Thread_id:   &threadID,
	}}, tracker, &reason)
	if got == nil || len(got) != 1 {
		t.Fatalf("normalizeSourceWatchRequests() len = %d, want 1", len(got))
	}
	if got[0].ThreadID != threadID {
		t.Fatalf("thread_id = %q, want %q", got[0].ThreadID, threadID)
	}
	if got[0].MessageID != "" {
		t.Fatalf("message_id = %q, want empty for thread-level merge", got[0].MessageID)
	}
	if got[0].SourceOutputID != "" {
		t.Fatalf("source_output_id = %q, want empty for thread-level merge", got[0].SourceOutputID)
	}
	if got[0].Query == "" {
		t.Fatal("expected merged thread watch to retain fallback query")
	}
	if !got[0].IncludeAttachments {
		t.Fatal("expected merged thread watch to retain attachment reads")
	}
}

func TestNormalizeSourceWatchRequestsBackfillsThreadIDFromTrackedOutput(t *testing.T) {
	reason := "Check for replies to opening offer email"
	wrongQuery := `subject:"Opening offer email to Oswaldo for RH Loveseat"`

	tracker := trackedEmailOutput(
		"out-1",
		"thread-real",
		"msg-real",
		"oswaldo@example.com",
		"Restoration Hardware Loveseat - Interested Buyer",
	)

	got := normalizeSourceWatchRequests([]signaltypes.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		Reason:      &reason,
		Query:       &wrongQuery,
	}}, tracker, &reason)
	if got == nil || len(got) != 1 {
		t.Fatalf("normalizeSourceWatchRequests() len = %d, want 1", len(got))
	}
	if got[0].ThreadID != "thread-real" {
		t.Fatalf("thread_id = %q, want %q (backfilled from tracked output)", got[0].ThreadID, "thread-real")
	}
	if got[0].Query != wrongQuery {
		t.Fatalf("classifier query should be preserved when non-empty, got %q", got[0].Query)
	}
	if got[0].SourceOutputID != "out-1" {
		t.Fatalf("source_output_id = %q, want %q (backfilled from tracked output)", got[0].SourceOutputID, "out-1")
	}
	if !got[0].IncludeAttachments {
		t.Fatal("expected IncludeAttachments=true after backfill set ThreadID on Gmail watch")
	}
	if !got[0].IncludeMessageBody {
		t.Fatal("expected IncludeMessageBody=true after backfill set ThreadID on Gmail watch")
	}
}

func TestNormalizeSourceWatchRequestsSkipsBackfillWhenMultipleTrackedOutputs(t *testing.T) {
	reason := "Check for replies"
	wrongQuery := `subject:"Opening offer email"`

	tracker := &taskOutputTracker{}
	rememberTrackedEmailOutput(tracker, "out-1", "thread-1", "msg-1", "alice@example.com", "Subject A")
	rememberTrackedEmailOutput(tracker, "out-2", "thread-2", "msg-2", "bob@example.com", "Subject B")

	got := normalizeSourceWatchRequests([]signaltypes.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		Reason:      &reason,
		Query:       &wrongQuery,
	}}, tracker, &reason)
	if got == nil || len(got) != 1 {
		t.Fatalf("normalizeSourceWatchRequests() len = %d, want 1", len(got))
	}
	if got[0].ThreadID != "" {
		t.Fatalf("thread_id = %q, want empty (ambiguous tracked outputs)", got[0].ThreadID)
	}
}

func TestNormalizeSourceWatchRequestsSkipsBackfillWhenQueryUnrelated(t *testing.T) {
	reason := "Check for new messages"
	unrelatedQuery := `label:inbox is:unread`

	tracker := trackedEmailOutput(
		"out-1",
		"thread-real",
		"msg-real",
		"alice@example.com",
		"Restoration Hardware Loveseat - Interested Buyer",
	)

	got := normalizeSourceWatchRequests([]signaltypes.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		Reason:      &reason,
		Query:       &unrelatedQuery,
	}}, tracker, &reason)
	if got == nil || len(got) != 1 {
		t.Fatalf("normalizeSourceWatchRequests() len = %d, want 1", len(got))
	}
	if got[0].ThreadID != "" {
		t.Fatalf("thread_id = %q, want empty (unrelated query should skip backfill)", got[0].ThreadID)
	}
	if got[0].Query != unrelatedQuery {
		t.Fatalf("query = %q, want original %q preserved", got[0].Query, unrelatedQuery)
	}
	if got[0].SourceOutputID != "" {
		t.Fatalf("source_output_id = %q, want empty (backfill should be skipped)", got[0].SourceOutputID)
	}
}

func TestNormalizeSourceWatchRequestsSkipsBackfillWhenEmailMismatch(t *testing.T) {
	reason := "Check for replies"
	mismatchQuery := `from:bob@other.com`

	tracker := trackedEmailOutput(
		"out-1",
		"thread-real",
		"msg-real",
		"alice@example.com",
		"Restoration Hardware Loveseat - Interested Buyer",
	)

	got := normalizeSourceWatchRequests([]signaltypes.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		Reason:      &reason,
		Query:       &mismatchQuery,
	}}, tracker, &reason)
	if got == nil || len(got) != 1 {
		t.Fatalf("normalizeSourceWatchRequests() len = %d, want 1", len(got))
	}
	if got[0].ThreadID != "" {
		t.Fatalf("thread_id = %q, want empty (email mismatch should skip backfill)", got[0].ThreadID)
	}
	if got[0].Query != mismatchQuery {
		t.Fatalf("query = %q, want original %q preserved", got[0].Query, mismatchQuery)
	}
}

func TestFollowUpPlanningMessageAppendsTrackedEmailMetadata(t *testing.T) {
	tracker := trackedEmailOutput(
		"out-1",
		"thread-abc",
		"msg-xyz",
		"user@example.com",
		"Restoration Hardware Loveseat",
	)

	agentMsg := "I sent the opening offer email. I'll check back for replies."
	got := followUpPlanningMessage(agentMsg, tracker)

	if !strings.Contains(got, agentMsg) {
		t.Fatal("expected result to contain the original agent message")
	}
	if !strings.Contains(got, "[thread_id=thread-abc]") {
		t.Fatal("expected result to contain tracked thread_id")
	}
	if !strings.Contains(got, "[subject=Restoration Hardware Loveseat]") {
		t.Fatal("expected result to contain tracked subject")
	}
	if !strings.Contains(got, "[recipient=user@example.com]") {
		t.Fatal("expected result to contain tracked recipient")
	}
}

func TestFollowUpPlanningMessageNoSuffixWithoutEmailOutputs(t *testing.T) {
	tracker := &taskOutputTracker{}

	agentMsg := "I completed the task successfully."
	got := followUpPlanningMessage(agentMsg, tracker)

	if got != agentMsg {
		t.Fatalf("expected unmodified message %q, got %q", agentMsg, got)
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
	rememberTrackedEmailOutputWithArtifactKey(tracker, outputID, "email-sent", threadID, messageID, recipient, subject)
}

func rememberTrackedEmailOutputWithArtifactKey(
	tracker *taskOutputTracker,
	outputID, artifactKey, threadID, messageID, recipient, subject string,
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
			types.TaskOutputMetadataArtifactKey: artifactKey,
		},
	}, outputID)
}
