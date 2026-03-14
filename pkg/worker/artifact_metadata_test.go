package worker

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestDefaultArtifactMetadataPreservesBAMLValues(t *testing.T) {
	metadata := map[string]any{
		types.TaskOutputMetadataArtifactKey:   "extracted-recipes",
		types.TaskOutputMetadataArtifactLabel: "Extracted Recipes",
		types.TaskOutputMetadataArtifactKind:  "recipe",
	}

	result := defaultArtifactMetadata(metadata, types.TaskOutputArtifactRolePrimary)

	if got := result[types.TaskOutputMetadataArtifactKey]; got != "extracted-recipes" {
		t.Fatalf("artifact key = %#v, want %q", got, "extracted-recipes")
	}
	if got := result[types.TaskOutputMetadataArtifactLabel]; got != "Extracted Recipes" {
		t.Fatalf("artifact label = %#v, want %q", got, "Extracted Recipes")
	}
	if got := result[types.TaskOutputMetadataArtifactKind]; got != "recipe" {
		t.Fatalf("artifact kind = %#v, want %q", got, "recipe")
	}
	if got := result[types.TaskOutputMetadataArtifactRole]; got != types.TaskOutputArtifactRolePrimary {
		t.Fatalf("role = %#v, want %q", got, types.TaskOutputArtifactRolePrimary)
	}
}

func TestDefaultArtifactMetadataNormalizesTokens(t *testing.T) {
	metadata := map[string]any{
		types.TaskOutputMetadataArtifactKey:  "Extracted Recipes",
		types.TaskOutputMetadataArtifactKind: "RECIPE",
	}

	result := defaultArtifactMetadata(metadata, "")

	if got := result[types.TaskOutputMetadataArtifactKey]; got != "extracted-recipes" {
		t.Fatalf("artifact key = %#v, want %q", got, "extracted-recipes")
	}
	if got := result[types.TaskOutputMetadataArtifactKind]; got != "recipe" {
		t.Fatalf("artifact kind = %#v, want %q", got, "recipe")
	}
}

func TestDefaultArtifactMetadataSetsRoleDefault(t *testing.T) {
	result := defaultArtifactMetadata(nil, "")

	if got := result[types.TaskOutputMetadataArtifactRole]; got != types.TaskOutputArtifactRoleSupporting {
		t.Fatalf("role should default to supporting, got %#v", got)
	}
}
