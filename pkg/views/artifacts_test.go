package views

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

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
