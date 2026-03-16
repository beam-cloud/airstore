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

func TestFetchComponentOutputsSkipsUnresolvedAgentRefs(t *testing.T) {
	agentID := "0c34c8c3-0af0-4e21-a553-5d3f7c88a4e2"
	backend := &fakeResolverBackend{
		profilesByKey: map[string]*types.AgentProfile{
			"chef-agent": {ID: agentID},
		},
		profiles: []*types.AgentProfile{
			{ID: agentID, Name: "Chef Agent"},
		},
		outputsByAgent: map[string][]*types.TaskOutput{
			agentID: {
				{ID: "out-1", Title: "Recipe output", AgentID: &agentID},
			},
		},
	}
	resolver := &DataResolver{backend: backend, store: nil}

	ds := &types.DataSource{
		AgentIDs: []string{"deleted-agent-key", "chef-agent"},
	}
	outputs, err := resolver.fetchComponentOutputs(context.Background(), 7, ds, nil)
	if err != nil {
		t.Fatalf("fetchComponentOutputs returned error: %v", err)
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

func TestFetchMappingOutputsExpandsTaskContextForSelectedTasks(t *testing.T) {
	agentID := "agent-1"
	primary := newRecipeOutput("out-1")
	primary.AgentID = &agentID
	primary.TaskID = "task-1"
	primary.Metadata = map[string]any{"artifact_key": "extracted-recipes"}

	sibling := newRecipeOutput("out-2")
	sibling.AgentID = &agentID
	sibling.TaskID = "task-1"
	sibling.Metadata = map[string]any{"artifact_key": "drive-recipe-pdf"}

	otherTask := newRecipeOutput("out-3")
	otherTask.AgentID = &agentID
	otherTask.TaskID = "task-2"
	otherTask.Metadata = map[string]any{"artifact_key": "drive-recipe-pdf"}

	backend := &fakeResolverBackend{
		profilesByKey: map[string]*types.AgentProfile{
			"chef-agent": {ID: agentID},
		},
		outputsByAgent: map[string][]*types.TaskOutput{
			agentID: {primary, sibling, otherTask},
		},
	}
	resolver := &DataResolver{backend: backend, store: nil}

	outputs, err := resolver.fetchMappingOutputs(
		context.Background(),
		7,
		&types.DataSource{ArtifactKey: "extracted-recipes"},
		[]string{"chef-agent"},
	)
	if err != nil {
		t.Fatalf("fetchMappingOutputs returned error: %v", err)
	}
	if got, want := len(outputs), 2; got != want {
		t.Fatalf("output count = %d, want %d", got, want)
	}
	if got, want := sortedOutputIDs(outputs), []string{"out-1", "out-2"}; !slicesMatch(got, want) {
		t.Fatalf("output ids = %v, want %v", got, want)
	}
	if got, want := len(backend.queriedAgentIDs), 2; got != want {
		t.Fatalf("query count = %d, want %d", got, want)
	}
}

func TestFetchMappingOutputsAvoidsExtraExpansionWhenSelectionIsAlreadyFull(t *testing.T) {
	agentID := "agent-1"
	first := newRecipeOutput("out-1")
	first.AgentID = &agentID
	first.TaskID = "task-1"

	second := newRecipeOutput("out-2")
	second.AgentID = &agentID
	second.TaskID = "task-2"

	backend := &fakeResolverBackend{
		profilesByKey: map[string]*types.AgentProfile{
			"chef-agent": {ID: agentID},
		},
		outputsByAgent: map[string][]*types.TaskOutput{
			agentID: {first, second},
		},
	}
	resolver := &DataResolver{backend: backend, store: nil}

	outputs, err := resolver.fetchMappingOutputs(
		context.Background(),
		7,
		&types.DataSource{},
		[]string{"chef-agent"},
	)
	if err != nil {
		t.Fatalf("fetchMappingOutputs returned error: %v", err)
	}
	if got, want := len(outputs), 2; got != want {
		t.Fatalf("output count = %d, want %d", got, want)
	}
	if got, want := len(backend.queriedAgentIDs), 1; got != want {
		t.Fatalf("query count = %d, want %d", got, want)
	}
}

func TestFilterOutputsForDataSource(t *testing.T) {
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

	filtered := filterOutputsForDataSource(outputs, &types.DataSource{}, []string{agentID})
	if len(filtered) != 2 {
		t.Fatalf("expected both outputs for matching agent, got %d", len(filtered))
	}
	filtered = filterOutputsForDataSource(outputs, &types.DataSource{}, []string{"other-agent"})
	if len(filtered) != 0 {
		t.Fatalf("expected no outputs for wrong agent, got %d", len(filtered))
	}
	filtered = filterOutputsForDataSource(outputs, &types.DataSource{ArtifactKey: "extracted-recipes"}, []string{agentID})
	if len(filtered) != 1 || filtered[0].ID != "out-1" {
		t.Fatalf("expected artifact key filter to keep out-1, got %#v", filtered)
	}
	filtered = filterOutputsForDataSource(outputs, nil, nil)
	if len(filtered) != 2 {
		t.Fatalf("nil data source should keep everything, got %d", len(filtered))
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
		{Type: types.ComponentTypeTable, DataSource: &types.DataSource{ArtifactKey: "recipes"}},
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

func TestBuildColumnSchemasIncludesTaskMetadataColumnsFromConfig(t *testing.T) {
	comp := types.ComponentSpec{
		DataSource: &types.DataSource{
			Transform: []types.TransformRule{
				{Column: "name", Source: "data.recipe_name", Type: "text"},
			},
		},
		Config: map[string]any{
			"columns": []any{
				map[string]any{"key": "name", "label": "Recipe Name"},
				map[string]any{"key": "next_wake_at", "label": "Next wake"},
				map[string]any{"key": "next_wake_summary", "label": "Planned wake"},
			},
		},
	}

	schemas := buildColumnSchemas(comp)
	if len(schemas) != 3 {
		t.Fatalf("expected transform columns plus task metadata columns, got %d", len(schemas))
	}
	if got := schemas[1].Key; got != "next_wake_at" {
		t.Fatalf("second schema key = %q, want next_wake_at", got)
	}
	if got := schemas[1].Type; got != "date" {
		t.Fatalf("next_wake_at type = %q, want date", got)
	}
	if got := schemas[2].Key; got != "next_wake_summary" {
		t.Fatalf("third schema key = %q, want next_wake_summary", got)
	}
}

func TestAssembleTableIncludesTaskWakeMetadata(t *testing.T) {
	wakeAt := time.Date(2026, 3, 15, 18, 30, 0, 0, time.UTC)
	wakeReason := "Check inbox for new broker replies"
	comp := types.ComponentSpec{
		Config: map[string]any{
			"columns": []any{
				map[string]any{"key": "name", "label": "Name", "type": "text"},
				map[string]any{"key": "next_wake_at", "label": "Next wake"},
				map[string]any{"key": "next_wake_summary", "label": "Planned wake"},
			},
		},
		DataSource: &types.DataSource{
			Transform: []types.TransformRule{
				{Column: "name", Source: "title", Type: "text"},
			},
		},
	}

	resolved := assembleTable(
		"sheet-1",
		comp,
		[]resolvedSheetRow{
			{
				SheetID: "sheet-1",
				TaskID:  "task-1",
				RowID:   "sheet-1:task-1:task",
				RowKey:  "task",
				Cells:   map[string]string{"name": "Prospect outreach"},
			},
		},
		map[string]*types.AgentTask{
			"task-1": {
				ID:         "task-1",
				State:      types.AgentTaskStateSleeping,
				WakeAt:     &wakeAt,
				WakeReason: &wakeReason,
				WakeAgenda: []*types.TaskWakeAgendaItem{
					{Seq: 1, Type: "check_replies", Title: "Check inbox for new broker replies"},
					{Seq: 2, Type: "follow_up", Title: "Send the next follow-up if nobody replied"},
				},
			},
		},
	)

	if got, want := resolved.Status, types.ResolvedDataStatusOK; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if len(resolved.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(resolved.Rows))
	}
	row := resolved.Rows[0]
	if got := row[1]; got != wakeAt.Format(time.RFC3339) {
		t.Fatalf("next_wake_at cell = %#v, want %q", got, wakeAt.Format(time.RFC3339))
	}
	if got := row[2]; got != wakeReason {
		t.Fatalf("next_wake_summary cell = %#v, want %q", got, wakeReason)
	}
}

func TestViewRowMergedCells(t *testing.T) {
	row := &ViewRow{
		ID:         "sheet-1:task-1:task",
		SheetID:    "sheet-1",
		GroupID:    "task-1",
		TaskID:     "task-1",
		RowKey:     "task",
		SchemaHash: "testhash123",
		OutputIDs:  []string{"out-1", "out-2"},
		Cells: map[string]string{
			"recipe_name": "Spaghetti",
			"video_url":   "https://yt.com/1",
		},
		Manual: map[string]string{
			"recipe_name": "Updated Spaghetti",
		},
		UpdatedAt: time.Now(),
	}

	merged := row.MergedCells()
	if merged["recipe_name"] != "Updated Spaghetti" {
		t.Fatalf("manual edit should override: got %q", merged["recipe_name"])
	}
	if merged["video_url"] != "https://yt.com/1" {
		t.Fatalf("non-edited cell should be preserved: got %q", merged["video_url"])
	}

	rowNoManual := &ViewRow{
		Cells: map[string]string{"a": "1", "b": "2"},
	}
	m2 := rowNoManual.MergedCells()
	if m2["a"] != "1" || m2["b"] != "2" {
		t.Fatalf("no manual edits should return cells as-is: %v", m2)
	}
}

func TestCarryStoredRowAndResolvedCells(t *testing.T) {
	row := &ViewRow{
		Cells: map[string]string{
			"name":   "Base Name",
			"status": "queued",
			"extra":  "ignored",
		},
		Manual: map[string]string{
			"name": "Edited Name",
		},
	}
	keys := map[string]bool{
		"name":   true,
		"status": true,
	}

	if !canCarryStoredRow(row, keys) {
		t.Fatal("expected stored row to be reusable when missing values are satisfied by cells or manual edits")
	}

	filtered := filterStoredCells(row.Cells, keys)
	if _, ok := filtered["extra"]; ok {
		t.Fatalf("unexpected extra field in filtered cells: %v", filtered)
	}
	if filtered["name"] != "Base Name" || filtered["status"] != "queued" {
		t.Fatalf("filtered base cells incorrect: %v", filtered)
	}

	withoutManual := composeResolvedCells(filtered, row.Manual, keys, false)
	if withoutManual["name"] != "Base Name" {
		t.Fatalf("force refresh should not reapply manual value: %v", withoutManual)
	}

	merged := composeResolvedCells(filtered, row.Manual, keys, true)
	if merged["name"] != "Edited Name" {
		t.Fatalf("manual value should win in merged cells: %v", merged)
	}
	if filtered["name"] != "Base Name" {
		t.Fatalf("base cells should remain unchanged after merge: %v", filtered)
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
	h1 := hashColumns(cols, types.RowStrategy{Mode: types.RowStrategyModeTask}, "test-sheet")
	h2 := hashColumns(cols, types.RowStrategy{Mode: types.RowStrategyModeTask}, "test-sheet")
	if h1 != h2 {
		t.Fatalf("hashColumns not stable: %q vs %q", h1, h2)
	}

	compsJSON, _ := json.Marshal(comps)
	var comps2 []types.ComponentSpec
	json.Unmarshal(compsJSON, &comps2)
	cols2 := buildUnifiedSchema(comps2)
	h3 := hashColumns(cols2, types.RowStrategy{Mode: types.RowStrategyModeTask}, "test-sheet")
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
