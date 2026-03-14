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

func TestTaskMatchesDataSource(t *testing.T) {
	agentID := "agent-1"
	mkOutput := func(id, artifactKey string) *types.TaskOutput {
		o := newRecipeOutput(id)
		o.AgentID = &agentID
		o.Metadata = map[string]any{"artifact_key": artifactKey}
		return o
	}

	outputs := []*types.TaskOutput{
		mkOutput("out-1", "extracted-recipes"),
		mkOutput("out-2", "drive-recipe-pdf"),
	}

	if !taskMatchesDataSource(outputs, &types.DataSource{}, []string{agentID}) {
		t.Fatal("expected match for correct agent")
	}
	if taskMatchesDataSource(outputs, &types.DataSource{}, []string{"other-agent"}) {
		t.Fatal("expected no match for wrong agent")
	}
	if !taskMatchesDataSource(outputs, nil, nil) {
		t.Fatal("nil data source should match everything")
	}
}

func TestBuildUnifiedSchema(t *testing.T) {
	comps := []types.ComponentSpec{
		{
			Type: types.ComponentTypeTable,
			DataSource: &types.DataSource{
				Transform: []types.TransformRule{
					{Column: "name", Source: "data.name", Type: "text"},
					{Column: "url", Source: "data.url", Type: "link"},
				},
			},
		},
		{
			Type: types.ComponentTypeTable,
			DataSource: &types.DataSource{
				Transform: []types.TransformRule{
					{Column: "name", Source: "data.name", Type: "text"},
					{Column: "price", Source: "data.price", Type: "currency"},
				},
			},
		},
		{Type: types.ComponentTypeMetric, DataSource: &types.DataSource{ArtifactKey: "recipes"}},
	}

	cols := buildUnifiedSchema(comps)
	if len(cols) != 3 {
		t.Fatalf("expected 3 unified columns (name deduped), got %d", len(cols))
	}
	keys := make(map[string]bool)
	for _, c := range cols {
		keys[c.Key] = true
	}
	for _, want := range []string{"name", "url", "price"} {
		if !keys[want] {
			t.Fatalf("missing unified column %q", want)
		}
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

func TestTaskCacheRoundTrip(t *testing.T) {
	cached := &cachedTaskMapping{
		SchemaHash: "testhash123",
		OutputIDs:  []string{"out-1", "out-2"},
		Cells: map[string]string{
			"recipe_name": "Spaghetti",
			"video_url":   "https://yt.com/1",
		},
		CachedAt: time.Now(),
	}

	raw, err := json.Marshal(cached)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var restored cachedTaskMapping
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if restored.SchemaHash != "testhash123" {
		t.Fatalf("round-trip: schema hash = %q, want testhash123", restored.SchemaHash)
	}
	if !slicesMatch(restored.OutputIDs, []string{"out-1", "out-2"}) {
		t.Fatalf("round-trip: output IDs mismatch: %v", restored.OutputIDs)
	}
	if restored.Cells["recipe_name"] != "Spaghetti" {
		t.Fatalf("round-trip: recipe_name = %q, want Spaghetti", restored.Cells["recipe_name"])
	}
	if restored.Cells["video_url"] != "https://yt.com/1" {
		t.Fatalf("round-trip: video_url = %q, want https://yt.com/1", restored.Cells["video_url"])
	}
}

func TestHashColumnsStable(t *testing.T) {
	comps := []types.ComponentSpec{
		{
			Type: types.ComponentTypeTable,
			DataSource: &types.DataSource{
				Transform: []types.TransformRule{
					{Column: "file_name", Source: "data.file_name", Type: "text"},
					{Column: "video_url", Source: "data.video_url", Type: "link"},
				},
			},
		},
	}

	cols := buildUnifiedSchema(comps)
	h1 := hashColumns(cols)
	h2 := hashColumns(cols)
	if h1 != h2 {
		t.Fatalf("hashColumns not stable: %q vs %q", h1, h2)
	}

	compsJSON, _ := json.Marshal(comps)
	var comps2 []types.ComponentSpec
	json.Unmarshal(compsJSON, &comps2)
	cols2 := buildUnifiedSchema(comps2)
	h3 := hashColumns(cols2)
	if h1 != h3 {
		t.Fatalf("hashColumns not stable across JSON round-trip: %q vs %q", h1, h3)
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
