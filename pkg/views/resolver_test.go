package views

import (
	"context"
	"encoding/json"
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
	tasks            map[string]*types.AgentTask
	queriedAgentIDs  []string
	filters          []types.TaskOutputListFilter
}

func (b *fakeResolverBackend) GetTaskByID(_ context.Context, taskID string) (*types.AgentTask, error) {
	if b.tasks != nil {
		if t, ok := b.tasks[taskID]; ok {
			return t, nil
		}
	}
	return nil, fmt.Errorf("task not found")
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

func TestResolveMetricReturnsOutputCount(t *testing.T) {
	backend := &fakeResolverBackend{
		workspaceOutputs: []*types.TaskOutput{
			newRecipeOutput("recipe-1"),
			newRecipeOutput("recipe-2"),
			newRecipeOutput("recipe-3"),
		},
	}
	resolver := &DataResolver{backend: backend, cache: newMappingCache(nil)}

	result, err := resolver.Resolve(context.Background(), 7, "test-view", types.ComponentSpec{
		ID:    "recipe-count",
		Type:  "metric",
		Title: "Recipes Extracted",
		Config: map[string]any{
			"label":  "Total Recipes",
			"suffix": " recipes",
		},
		DataSource: &types.DataSource{
			OutputType: "text",
		},
	})
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if got, want := result.Status, types.ResolvedDataStatusOK; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := result.Total, 3; got != want {
		t.Fatalf("total = %d, want %d", got, want)
	}
}

func TestBuildColumnSchemas(t *testing.T) {
	comp := types.ComponentSpec{
		DataSource: &types.DataSource{
			Transform: []types.TransformRule{
				{Column: "name", Source: "data.recipe_name", Type: "text"},
				{Column: "url", Source: "data.video_url", Type: "link"},
			},
		},
		Config: map[string]any{
			"columns": []any{
				map[string]any{"key": "name", "label": "Recipe Name"},
			},
		},
	}

	schemas := buildColumnSchemas(comp)
	if len(schemas) != 2 {
		t.Fatalf("expected 2 schemas, got %d", len(schemas))
	}
	if schemas[0].Key != "name" {
		t.Fatalf("first schema key = %q, want 'name'", schemas[0].Key)
	}
	if schemas[0].Description != "Recipe Name (hint: data.recipe_name)" {
		t.Fatalf("first schema desc = %q, unexpected", schemas[0].Description)
	}
}

func TestCacheRoundTrip(t *testing.T) {
	cached := &cachedMapping{
		SchemaHash: "testhash123",
		OutputIDs:  []string{"out-1", "out-2"},
		Columns:    []string{"recipe_name", "video_url", "task_id"},
		ColumnMeta: []types.ColumnMeta{
			{Key: "recipe_name", Type: "text", Label: "Recipe Name"},
			{Key: "video_url", Type: "link", Label: "Video"},
			{Key: "task_id", Type: "text", Hidden: true},
		},
		Rows: [][]any{
			{"Spaghetti", "https://yt.com/1", "task-1"},
			{"Brownies", nil, "task-2"},
		},
		CachedAt: time.Now(),
	}

	raw, err := json.Marshal(cached)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var restored cachedMapping
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if len(restored.Rows) != 2 {
		t.Fatalf("round-trip: expected 2 rows, got %d", len(restored.Rows))
	}
	if restored.SchemaHash != "testhash123" {
		t.Fatalf("round-trip: schema hash = %q, want testhash123", restored.SchemaHash)
	}
	if !slicesMatch(restored.OutputIDs, []string{"out-1", "out-2"}) {
		t.Fatalf("round-trip: output IDs mismatch: %v", restored.OutputIDs)
	}
	if restored.Rows[0][0] != "Spaghetti" {
		t.Fatalf("row 0 col 0 = %v, want Spaghetti", restored.Rows[0][0])
	}
	if restored.Rows[1][1] != nil {
		t.Fatalf("row 1 col 1 = %v, want nil", restored.Rows[1][1])
	}
}

func TestSchemaHashStable(t *testing.T) {
	comp := types.ComponentSpec{
		ID:    "recipe-table",
		Title: "Recipe PDFs in Google Drive",
		Type:  "data-table",
		DataSource: &types.DataSource{
			AgentIDs:    []string{"youtube-recipe-extractor"},
			ArtifactKey: "recipes",
			Transform: []types.TransformRule{
				{Column: "file_name", Source: "data.file_name", Type: "text"},
				{Column: "video_url", Source: "data.video_url", Type: "link"},
			},
		},
		Config: map[string]any{
			"columns": []any{
				map[string]any{"key": "file_name", "label": "File Name"},
				map[string]any{"key": "video_url", "label": "Video"},
			},
		},
	}

	h1 := schemaHash(comp)
	h2 := schemaHash(comp)
	h3 := schemaHash(comp)
	if h1 != h2 || h2 != h3 {
		t.Fatalf("schemaHash not stable: %q, %q, %q", h1, h2, h3)
	}

	compJSON, _ := json.Marshal(comp)
	var comp2 types.ComponentSpec
	json.Unmarshal(compJSON, &comp2)
	h4 := schemaHash(comp2)
	if h1 != h4 {
		t.Fatalf("schemaHash not stable across JSON round-trip: %q vs %q", h1, h4)
	}
}

func TestSortedOutputIDsAndSlicesMatch(t *testing.T) {
	outputs := []*types.TaskOutput{
		{ID: "out-3", TaskID: "task-b"},
		{ID: "out-1", TaskID: "task-a"},
		{ID: "out-2", TaskID: "task-b"},
	}

	oids := sortedOutputIDs(outputs)
	expected := []string{"out-1", "out-2", "out-3"}
	if !slicesMatch(oids, expected) {
		t.Fatalf("sortedOutputIDs = %v, want %v", oids, expected)
	}

	tids := sortedTaskIDs(outputs)
	expectedTasks := []string{"task-a", "task-b"}
	if !slicesMatch(tids, expectedTasks) {
		t.Fatalf("sortedTaskIDs = %v, want %v", tids, expectedTasks)
	}

	if slicesMatch(oids, []string{"out-1", "out-2"}) {
		t.Fatal("should not match shorter list")
	}
	if slicesMatch(oids, []string{"out-1", "out-2", "out-4"}) {
		t.Fatal("should not match different list")
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
