package views

import (
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestFormatForPromptWithCompaction(t *testing.T) {
	entries := []types.ViewContextEntry{
		{
			EntryType: types.ViewContextEntryCompaction,
			Content:   "- Use formal tone\n- Primary contact is jane@acme.com",
		},
		{
			EntryType: types.ViewContextEntryFeedback,
			Content:   "Include company size in outreach",
		},
	}
	result := FormatForPrompt(entries)
	if !strings.Contains(result, "standing instructions") {
		t.Fatalf("expected header, got:\n%s", result)
	}
	if !strings.Contains(result, "formal tone") {
		t.Fatalf("expected compaction content, got:\n%s", result)
	}
	if !strings.Contains(result, "- Include company size") {
		t.Fatalf("expected feedback bullet, got:\n%s", result)
	}
}

func TestFormatForPromptEmpty(t *testing.T) {
	if got := FormatForPrompt(nil); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestContextCompactorShouldCompact(t *testing.T) {
	cc := &ContextCompactor{}

	under := make([]types.ViewContextEntry, 19)
	for i := range under {
		under[i] = types.ViewContextEntry{EntryType: types.ViewContextEntryFeedback}
	}
	if cc.ShouldCompact(under) {
		t.Fatal("expected false for < threshold entries")
	}

	at := make([]types.ViewContextEntry, 20)
	for i := range at {
		at[i] = types.ViewContextEntry{EntryType: types.ViewContextEntryNote}
	}
	if !cc.ShouldCompact(at) {
		t.Fatal("expected true for = threshold entries")
	}

	withCompaction := make([]types.ViewContextEntry, 25)
	for i := range withCompaction {
		withCompaction[i] = types.ViewContextEntry{EntryType: types.ViewContextEntryFeedback}
	}
	withCompaction[0] = types.ViewContextEntry{EntryType: types.ViewContextEntryCompaction}

	// 24 raw entries (25 total - 1 compaction)
	if !cc.ShouldCompact(withCompaction) {
		t.Fatal("expected true: 24 raw entries exceed threshold")
	}
}
