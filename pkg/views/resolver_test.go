package views

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestDotGetSupportsImplicitArrayTraversal(t *testing.T) {
	data := map[string]any{
		"videos": []any{
			map[string]any{
				"metadata": map[string]any{
					"title": "Actual video title",
				},
			},
		},
	}

	got := dotGet(data, "videos.metadata.title")
	if got != "Actual video title" {
		t.Fatalf("expected first array match, got %#v", got)
	}
}

func TestDotGetSupportsIndexedArrayTraversal(t *testing.T) {
	data := map[string]any{
		"videos": []any{
			map[string]any{"title": "First"},
			map[string]any{"title": "Second"},
		},
	}

	got := dotGet(data, "videos[1].title")
	if got != "Second" {
		t.Fatalf("expected indexed array value, got %#v", got)
	}
}

func TestFilterOutputsByTimeRange(t *testing.T) {
	now := time.Now()
	outputs := []*types.TaskOutput{
		{ID: "fresh", CreatedAt: now.Add(-2 * time.Hour)},
		{ID: "old", CreatedAt: now.Add(-48 * time.Hour)},
	}

	filtered := filterOutputsByTimeRange(outputs, "24h")
	if len(filtered) != 1 || filtered[0].ID != "fresh" {
		t.Fatalf("expected only recent outputs, got %#v", filtered)
	}
}

type fakeResolverBackend struct {
	profilesByKey    map[string]*types.AgentProfile
	profiles         []*types.AgentProfile
	outputsByAgent   map[string][]*types.TaskOutput
	workspaceOutputs []*types.TaskOutput
	queriedAgentIDs  []string
	filters          []types.TaskOutputListFilter
}

func (b *fakeResolverBackend) GetAgentProfileByKey(_ context.Context, _ uint, agentKey string) (*types.AgentProfile, error) {
	if profile, ok := b.profilesByKey[agentKey]; ok {
		return profile, nil
	}
	return nil, fmt.Errorf("agent not found")
}

func (b *fakeResolverBackend) ListAgentProfiles(_ context.Context, _ uint) ([]*types.AgentProfile, error) {
	return b.profiles, nil
}

func (b *fakeResolverBackend) ListWorkspaceTaskOutputs(_ context.Context, _ uint, filter types.TaskOutputListFilter) ([]*types.TaskOutput, error) {
	b.filters = append(b.filters, filter)
	if filter.AgentID != nil {
		b.queriedAgentIDs = append(b.queriedAgentIDs, *filter.AgentID)
		return b.outputsByAgent[*filter.AgentID], nil
	}
	return b.workspaceOutputs, nil
}

func TestFetchOutputsSkipsUnresolvedAgentRefs(t *testing.T) {
	backend := &fakeResolverBackend{
		profilesByKey: map[string]*types.AgentProfile{
			"chef-agent": {ID: "0c34c8c3-0af0-4e21-a553-5d3f7c88a4e2"},
		},
		profiles: []*types.AgentProfile{
			{ID: "0c34c8c3-0af0-4e21-a553-5d3f7c88a4e2", Name: "Chef Agent"},
		},
		outputsByAgent: map[string][]*types.TaskOutput{
			"0c34c8c3-0af0-4e21-a553-5d3f7c88a4e2": {
				{ID: "out-1", Title: "Recipe output"},
			},
		},
	}
	resolver := &DataResolver{backend: backend, cache: newMappingCache(nil)}

	outputs, err := resolver.fetchOutputs(context.Background(), 7, &types.DataSource{
		AgentIDs: []string{"deleted-agent-key", "chef-agent"},
	})
	if err != nil {
		t.Fatalf("fetchOutputs returned error: %v", err)
	}
	if got, want := len(outputs), 1; got != want {
		t.Fatalf("output count = %d, want %d", got, want)
	}
	if got, want := len(backend.queriedAgentIDs), 1; got != want {
		t.Fatalf("query count = %d, want %d", got, want)
	}
	if got, want := backend.queriedAgentIDs[0], "0c34c8c3-0af0-4e21-a553-5d3f7c88a4e2"; got != want {
		t.Fatalf("queried agent id = %q, want %q", got, want)
	}
	if got := backend.filters[0].ExcludeArchived; got {
		t.Fatal("expected views resolver to include archived outputs")
	}
}

func TestResolveReturnsBindingErrorWhenArtifactKeyMatchesNothing(t *testing.T) {
	backend := &fakeResolverBackend{
		workspaceOutputs: []*types.TaskOutput{newRecipeOutput("recipe-1")},
	}
	resolver := &DataResolver{backend: backend, cache: newMappingCache(nil)}

	result, err := resolver.Resolve(context.Background(), 7, types.ComponentSpec{
		ID:    "recipe-table",
		Type:  "table",
		Title: "Recipe Table",
		DataSource: &types.DataSource{
			OutputType:  "text",
			ArtifactKey: "wrong-key",
			Transform: []types.TransformRule{
				{Column: "recipe_name", Source: "data.recipe_name", Type: "text"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if got, want := result.Status, types.ResolvedDataStatusBindingError; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
}

func TestResolveUsesExplicitTransforms(t *testing.T) {
	backend := &fakeResolverBackend{
		workspaceOutputs: []*types.TaskOutput{newRecipeOutput("recipe-1")},
	}
	resolver := &DataResolver{backend: backend, cache: newMappingCache(nil)}

	result, err := resolver.Resolve(context.Background(), 7, types.ComponentSpec{
		ID:    "recipe-table",
		Type:  "table",
		Title: "Recipe Table",
		DataSource: &types.DataSource{
			ArtifactKey: "recipes",
			Transform: []types.TransformRule{
				{Column: "recipe_name", Source: "data.recipe_name", Type: "text"},
				{Column: "video_url", Source: "data.video_url", Type: "link"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if got, want := result.Status, types.ResolvedDataStatusOK; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got := result.Rows[0][0]; got != "Spaghetti in Tomato Water" {
		t.Fatalf("recipe_name = %#v, want recipe title", got)
	}
	if got := result.Rows[0][1]; got != "https://example.com/video" {
		t.Fatalf("video_url = %#v, want url", got)
	}
}

func TestResolveInfersRulesWhenNoTransforms(t *testing.T) {
	backend := &fakeResolverBackend{
		workspaceOutputs: []*types.TaskOutput{newRecipeOutput("recipe-1")},
	}
	resolver := &DataResolver{backend: backend, cache: newMappingCache(nil)}

	result, err := resolver.Resolve(context.Background(), 7, types.ComponentSpec{
		ID:    "recipe-table",
		Type:  "table",
		Title: "Recipe Table",
		DataSource: &types.DataSource{
			ArtifactKey: "recipes",
		},
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if got, want := result.Status, types.ResolvedDataStatusOK; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if len(result.Columns) < 3 {
		t.Fatalf("expected inferred columns, got %v", result.Columns)
	}
	if result.Columns[0] != "title" {
		t.Fatalf("first column should be title, got %q", result.Columns[0])
	}
	if result.Columns[len(result.Columns)-3] != "created_at" {
		t.Fatalf("expected created_at before task_id/output_id, got %v", result.Columns)
	}
}

func newRecipeOutput(id string) *types.TaskOutput {
	return &types.TaskOutput{
		ID:         id,
		TaskID:     "task-1",
		OutputType: "text",
		Title:      "Created recipe report",
		Data: map[string]any{
			"recipe_name": "Spaghetti in Tomato Water",
			"video_url":   "https://example.com/video",
		},
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey:   "recipes",
			types.TaskOutputMetadataArtifactLabel: "Recipes",
			types.TaskOutputMetadataArtifactKind:  "recipe",
			types.TaskOutputMetadataArtifactRole:  types.TaskOutputArtifactRolePrimary,
		},
		CreatedAt: time.Now(),
	}
}
