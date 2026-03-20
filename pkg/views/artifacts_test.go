package views

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestMatchesKeyFuzzy(t *testing.T) {
	mk := func(key string) Artifact {
		return ArtifactOf(&types.TaskOutput{
			Metadata: map[string]any{string(types.TaskOutputMetadataArtifactKey): key},
		})
	}
	cases := []struct {
		outputKey, filterKey string
		want                 bool
	}{
		{"real-estate-agent-contacts", "real-estate-contacts", true},
		{"real-estate-contacts", "real-estate-agent-contacts", true},
		{"email", "email", true},
		{"approval-email", "sales-email", true},
		{"blocked-email", "sales-email", true},
		{"sales-email", "approval-email", true},
		{"recipe", "contacts", false},
		{"csv", "real-estate-contacts", false},
		{"", "contacts", false},
		{"contacts", "", false},
	}
	for _, tc := range cases {
		if got := mk(tc.outputKey).MatchesKey(tc.filterKey); got != tc.want {
			t.Errorf("MatchesKey(%q, %q) = %v, want %v", tc.outputKey, tc.filterKey, got, tc.want)
		}
	}
}

func TestArtifactLinkKindUsesRegistrableDomain(t *testing.T) {
	nyt := "https://www.nytimes.com/2026/03/15/example"
	google := "https://www.google.com/search?q=test"

	if got := ArtifactOf(&types.TaskOutput{URI: &nyt}).Kind(); got != "nytimes-link" {
		t.Fatalf("nytimes link kind = %q, want nytimes-link", got)
	}
	if got := ArtifactOf(&types.TaskOutput{URI: &google}).Kind(); got != "google-link" {
		t.Fatalf("google link kind = %q, want google-link", got)
	}
}
