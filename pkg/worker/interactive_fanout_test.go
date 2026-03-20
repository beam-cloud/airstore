package worker

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestShouldAttemptFanOutSkipsSingleEmailEntity(t *testing.T) {
	tracker := &taskOutputTracker{}

	tracker.RememberWithID(outputCandidate{
		OutputType: "email",
		Title:      "Cold outreach email to Luke at Beam",
		Data: map[string]any{
			"recipient": "luke@beam.cloud",
			"subject":   "Airstore scheduling test March 20",
		},
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey:  "outreach-email",
			types.TaskOutputMetadataArtifactKind: "email",
			types.TaskOutputMetadataArtifactRole: types.TaskOutputArtifactRolePrimary,
		},
	}, "draft-output")
	tracker.RememberWithID(outputCandidate{
		OutputType: "email",
		Title:      "Sent email to luke@beam.cloud",
		URI:        "https://mail.google.com/mail/u/0/#inbox/sent-message",
		Data: map[string]any{
			"to":      "luke@beam.cloud",
			"subject": "Airstore scheduling test March 20",
		},
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey:  "email-sent",
			types.TaskOutputMetadataArtifactKind: "email",
		},
	}, "sent-output")
	tracker.RememberWithID(outputCandidate{
		OutputType: "email",
		Title:      "Drafted follow-up email to Luke",
		URI:        "https://mail.google.com/mail/u/0/#inbox/followup-draft",
		Data: map[string]any{
			"to":      "luke@beam.cloud",
			"subject": "Re: Airstore scheduling test March 20",
		},
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey:  "gmail-draft",
			types.TaskOutputMetadataArtifactKind: "email",
		},
	}, "followup-output")

	summaries := tracker.TrackedOutputSummaries()
	if got := distinctFanOutEntityCount(summaries); got != 1 {
		t.Fatalf("distinctFanOutEntityCount = %d, want 1", got)
	}
	if shouldAttemptFanOut(summaries) {
		t.Fatal("shouldAttemptFanOut = true, want false for a single email thread")
	}
	for _, summary := range summaries {
		if got := summary.EntityKey; got != "email:luke@beam.cloud" {
			t.Fatalf("EntityKey = %q, want %q", got, "email:luke@beam.cloud")
		}
	}
}

func TestShouldAttemptFanOutAllowsDistinctEmailEntities(t *testing.T) {
	tracker := &taskOutputTracker{}

	tracker.RememberWithID(outputCandidate{
		OutputType: "email",
		Title:      "Sent email to Luke",
		URI:        "https://mail.google.com/mail/u/0/#inbox/luke-thread",
		Data: map[string]any{
			"to":      "luke@beam.cloud",
			"subject": "Hello Luke",
		},
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey:  "email-sent",
			types.TaskOutputMetadataArtifactKind: "email",
		},
	}, "luke-output")
	tracker.RememberWithID(outputCandidate{
		OutputType: "email",
		Title:      "Sent email to Jill",
		URI:        "https://mail.google.com/mail/u/0/#inbox/jill-thread",
		Data: map[string]any{
			"to":      "jill@example.com",
			"subject": "Hello Jill",
		},
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey:  "email-sent",
			types.TaskOutputMetadataArtifactKind: "email",
		},
	}, "jill-output")

	summaries := tracker.TrackedOutputSummaries()
	if got := distinctFanOutEntityCount(summaries); got != 2 {
		t.Fatalf("distinctFanOutEntityCount = %d, want 2", got)
	}
	if !shouldAttemptFanOut(summaries) {
		t.Fatal("shouldAttemptFanOut = false, want true for distinct recipients")
	}
}

func TestShouldAttemptFanOutIgnoresWorkspaceDraftFileAlongsideSentEmail(t *testing.T) {
	tracker := &taskOutputTracker{}

	tracker.RememberWithID(outputCandidate{
		OutputType: types.TaskOutputTypeEmail,
		Title:      "Sent email to luke@slai.io",
		URI:        "https://mail.google.com/mail/u/0/#inbox/thread-1",
		Data: map[string]any{
			"to":      "luke@slai.io",
			"subject": "Faster dev environments for your ML workflows?",
		},
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey:  "outreach-email",
			types.TaskOutputMetadataArtifactKind: "email",
		},
	}, "sent-output")

	tracker.RememberWithID(outputCandidate{
		OutputType: "file",
		Title:      "Marked email draft as sent",
		Path:       "/workspace/agents/email-outreach/draft-mike-slai.md",
		Data: map[string]any{
			"company":   "Mike (Slai)",
			"recipient": "Mike (Slai)",
			"status":    "sent",
		},
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey:  "email-outreach",
			types.TaskOutputMetadataArtifactKind: "md",
		},
	}, "workspace-draft-output")

	summaries := tracker.TrackedOutputSummaries()
	if got := distinctFanOutEntityCount(summaries); got != 1 {
		t.Fatalf("distinctFanOutEntityCount = %d, want 1", got)
	}
	if shouldAttemptFanOut(summaries) {
		t.Fatal("shouldAttemptFanOut = true, want false when only one real email thread exists")
	}
}
