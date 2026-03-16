package views

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	bamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
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
	profilesByKey     map[string]*types.AgentProfile
	profiles          []*types.AgentProfile
	outputsByAgent    map[string][]*types.TaskOutput
	taskOutputsByTask map[string][]*types.TaskOutput
	workspaceOutputs  []*types.TaskOutput
	tasks             map[string]*types.AgentTask
	queriedAgentIDs   []string
	queriedTaskIDs    []string
	filters           []types.TaskOutputListFilter
}

func (b *fakeResolverBackend) GetTaskByID(_ context.Context, taskID string) (*types.AgentTask, error) {
	if b.tasks != nil {
		if t, ok := b.tasks[taskID]; ok {
			return t, nil
		}
	}
	return nil, fmt.Errorf("task not found")
}

func (b *fakeResolverBackend) ListTaskOutputs(_ context.Context, _ uint, taskID string) ([]*types.TaskOutput, error) {
	b.queriedTaskIDs = append(b.queriedTaskIDs, taskID)
	if b.taskOutputsByTask != nil {
		if outputs, ok := b.taskOutputsByTask[taskID]; ok {
			return outputs, nil
		}
	}
	var outputs []*types.TaskOutput
	for _, output := range b.workspaceOutputs {
		if output != nil && output.TaskID == taskID {
			outputs = append(outputs, output)
		}
	}
	return outputs, nil
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
	outputs := b.workspaceOutputs
	if filter.AgentID != nil {
		b.queriedAgentIDs = append(b.queriedAgentIDs, *filter.AgentID)
		outputs = b.outputsByAgent[*filter.AgentID]
	}
	if filter.OutputType != nil {
		filtered := make([]*types.TaskOutput, 0, len(outputs))
		for _, output := range outputs {
			if output != nil && strings.EqualFold(strings.TrimSpace(output.OutputType), strings.TrimSpace(*filter.OutputType)) {
				filtered = append(filtered, output)
			}
		}
		return filtered, nil
	}
	return outputs, nil
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

func TestFetchMappingOutputsDoesNotFallBackToWorkspaceForUnresolvedViewAgentRefs(t *testing.T) {
	otherAgentID := "other-agent"
	other := newRecipeOutput("out-1")
	other.AgentID = &otherAgentID
	other.TaskID = "task-1"

	backend := &fakeResolverBackend{
		workspaceOutputs: []*types.TaskOutput{other},
	}
	resolver := &DataResolver{backend: backend, store: nil}

	outputs, err := resolver.fetchMappingOutputs(
		context.Background(),
		7,
		&types.DataSource{},
		[]string{"missing-agent"},
	)
	if err != nil {
		t.Fatalf("fetchMappingOutputs returned error: %v", err)
	}
	if len(outputs) != 0 {
		t.Fatalf("expected no outputs, got %#v", outputs)
	}
	if got := len(backend.filters); got != 0 {
		t.Fatalf("expected no workspace queries, got %d", got)
	}
	if got := len(backend.queriedTaskIDs); got != 0 {
		t.Fatalf("expected no task expansion, got %d task queries", got)
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
		taskOutputsByTask: map[string][]*types.TaskOutput{
			"task-1": {primary, sibling},
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
	if got, want := len(backend.queriedAgentIDs), 1; got != want {
		t.Fatalf("agent query count = %d, want %d", got, want)
	}
	if got, want := len(backend.queriedTaskIDs), 1; got != want {
		t.Fatalf("task query count = %d, want %d", got, want)
	}
	if got, want := backend.queriedTaskIDs[0], "task-1"; got != want {
		t.Fatalf("queried task id = %q, want %q", got, want)
	}
}

func TestFetchMappingOutputsExpandsTaskContextForStatusScopedTasks(t *testing.T) {
	agentID := "agent-1"
	pending := newRecipeOutput("out-1")
	pending.AgentID = &agentID
	pending.TaskID = "task-1"
	pending.Status = types.TaskOutputStatusPending

	approvedSibling := newRecipeOutput("out-2")
	approvedSibling.AgentID = &agentID
	approvedSibling.TaskID = "task-1"
	approvedSibling.Status = types.TaskOutputStatusApproved

	otherTask := newRecipeOutput("out-3")
	otherTask.AgentID = &agentID
	otherTask.TaskID = "task-2"
	otherTask.Status = types.TaskOutputStatusApproved

	backend := &fakeResolverBackend{
		profilesByKey: map[string]*types.AgentProfile{
			"chef-agent": {ID: agentID},
		},
		outputsByAgent: map[string][]*types.TaskOutput{
			agentID: {pending, approvedSibling, otherTask},
		},
		taskOutputsByTask: map[string][]*types.TaskOutput{
			"task-1": {pending, approvedSibling},
		},
	}
	resolver := &DataResolver{backend: backend, store: nil}

	outputs, err := resolver.fetchMappingOutputs(
		context.Background(),
		7,
		&types.DataSource{Statuses: []string{types.TaskOutputStatusPending}},
		[]string{"chef-agent"},
	)
	if err != nil {
		t.Fatalf("fetchMappingOutputs returned error: %v", err)
	}
	if got, want := sortedOutputIDs(outputs), []string{"out-1", "out-2"}; !slicesMatch(got, want) {
		t.Fatalf("output ids = %v, want %v", got, want)
	}
	if got, want := len(backend.queriedTaskIDs), 1; got != want {
		t.Fatalf("task query count = %d, want %d", got, want)
	}
	if got, want := backend.queriedTaskIDs[0], "task-1"; got != want {
		t.Fatalf("queried task id = %q, want %q", got, want)
	}
}

func TestFetchMappingOutputsUsesOutputTypeFallbackWhenArtifactKeyUnset(t *testing.T) {
	agentID := "agent-1"
	jsonPrimary := newRecipeOutput("out-1")
	jsonPrimary.AgentID = &agentID
	jsonPrimary.TaskID = "task-1"
	jsonPrimary.OutputType = "json"

	textSibling := newRecipeOutput("out-2")
	textSibling.AgentID = &agentID
	textSibling.TaskID = "task-1"
	textSibling.OutputType = "text"

	textOtherTask := newRecipeOutput("out-3")
	textOtherTask.AgentID = &agentID
	textOtherTask.TaskID = "task-2"
	textOtherTask.OutputType = "text"

	backend := &fakeResolverBackend{
		profilesByKey: map[string]*types.AgentProfile{
			"chef-agent": {ID: agentID},
		},
		outputsByAgent: map[string][]*types.TaskOutput{
			agentID: {jsonPrimary, textSibling, textOtherTask},
		},
		taskOutputsByTask: map[string][]*types.TaskOutput{
			"task-1": {jsonPrimary, textSibling},
		},
	}
	resolver := &DataResolver{backend: backend, store: nil}

	outputs, err := resolver.fetchMappingOutputs(
		context.Background(),
		7,
		&types.DataSource{OutputType: "json"},
		[]string{"chef-agent"},
	)
	if err != nil {
		t.Fatalf("fetchMappingOutputs returned error: %v", err)
	}
	if got, want := sortedOutputIDs(outputs), []string{"out-1", "out-2"}; !slicesMatch(got, want) {
		t.Fatalf("output ids = %v, want %v", got, want)
	}
	if got, want := len(backend.queriedTaskIDs), 1; got != want {
		t.Fatalf("task query count = %d, want %d", got, want)
	}
	if got, want := backend.queriedTaskIDs[0], "task-1"; got != want {
		t.Fatalf("queried task id = %q, want %q", got, want)
	}
	if got := backend.filters[0].OutputType; got == nil || *got != "json" {
		t.Fatalf("expected output type filter to be forwarded, got %#v", got)
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
	if got := len(backend.queriedTaskIDs); got != 0 {
		t.Fatalf("expected no task-level expansion, got %d task queries", got)
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

	// artifact_key narrows to matching outputs regardless of output_type.
	filtered = filterOutputsForDataSource(outputs, &types.DataSource{
		ArtifactKey: "extracted-recipes",
		OutputType:  "json",
	}, []string{agentID})
	if len(filtered) != 1 || filtered[0].ID != "out-1" {
		t.Fatalf("artifact_key should narrow to matching outputs, got %d results", len(filtered))
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
	if schemas[0].Name != "Recipe Name" {
		t.Fatalf("first schema name = %q, want 'Recipe Name'", schemas[0].Name)
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
	if got := schemas[1].Name; got != "Next wake" {
		t.Fatalf("second schema name = %q, want 'Next wake'", got)
	}
	if got := schemas[1].Type; got != "date" {
		t.Fatalf("next_wake_at type = %q, want date", got)
	}
	if got := schemas[2].Key; got != "next_wake_summary" {
		t.Fatalf("third schema key = %q, want next_wake_summary", got)
	}
}

func TestBuildColumnSchemasSkipsDuplicateKeys(t *testing.T) {
	comp := types.ComponentSpec{
		DataSource: &types.DataSource{
			Transform: []types.TransformRule{
				{Column: "source_url", Source: "data.primary_url", Type: "link"},
				{Column: "source_url", Source: "data.secondary_url", Type: "link"},
				{Column: "recipe_name", Source: "data.recipe_name", Type: "text"},
			},
		},
		Config: map[string]any{
			"columns": []any{
				map[string]any{"key": "source_url", "label": "Video", "type": "link"},
				map[string]any{"key": "source_url", "label": "Duplicate Video", "type": "link"},
				map[string]any{"key": "recipe_name", "label": "Recipe", "type": "text"},
			},
		},
	}

	schemas := buildColumnSchemas(comp)
	if got, want := len(schemas), 2; got != want {
		t.Fatalf("schema count = %d, want %d", got, want)
	}
	if got, want := schemas[0].Key, "source_url"; got != want {
		t.Fatalf("first schema key = %q, want %q", got, want)
	}
	if got, want := schemas[1].Key, "recipe_name"; got != want {
		t.Fatalf("second schema key = %q, want %q", got, want)
	}

	keys := schemaKeyList(schemas)
	if got, want := len(keys), 2; got != want {
		t.Fatalf("schemaKeyList length = %d, want %d", got, want)
	}
	if got, want := keys[0], "source_url"; got != want {
		t.Fatalf("first schema key list item = %q, want %q", got, want)
	}
	if got, want := keys[1], "recipe_name"; got != want {
		t.Fatalf("second schema key list item = %q, want %q", got, want)
	}
}

func TestBuildColumnSchemasCanonicalizesAliasKeys(t *testing.T) {
	compAlias := types.ComponentSpec{
		Type:  types.ComponentTypeTable,
		Title: "Recipes",
		DataSource: &types.DataSource{
			Transform: []types.TransformRule{
				{Column: "source_url", Source: "data.primary_url", Type: "link"},
			},
		},
		Config: map[string]any{
			"columns": []any{
				map[string]any{"key": "Source URL", "label": "Source URL", "type": "link"},
				map[string]any{"key": "source_url", "label": "Canonical Source URL", "type": "link"},
			},
		},
	}
	compCanonical := types.ComponentSpec{
		Type:  types.ComponentTypeTable,
		Title: "Recipes",
		DataSource: &types.DataSource{
			Transform: []types.TransformRule{
				{Column: "source_url", Source: "data.primary_url", Type: "link"},
			},
		},
		Config: map[string]any{
			"columns": []any{
				map[string]any{"key": "source_url", "label": "Source URL", "type": "link"},
			},
		},
	}

	aliasSchemas := buildColumnSchemas(compAlias)
	if got, want := len(aliasSchemas), 1; got != want {
		t.Fatalf("alias schema count = %d, want %d", got, want)
	}
	if got, want := aliasSchemas[0].Key, "source_url"; got != want {
		t.Fatalf("alias schema key = %q, want %q", got, want)
	}
	if got, want := aliasSchemas[0].Name, "Source URL"; got != want {
		t.Fatalf("alias schema name = %q, want %q", got, want)
	}

	canonicalSchemas := buildColumnSchemas(compCanonical)
	hashAlias := hashColumns(aliasSchemas, types.RowStrategy{Mode: types.RowStrategyModeTask}, "recipes", compAlias.Title, compAlias.Type)
	hashCanonical := hashColumns(canonicalSchemas, types.RowStrategy{Mode: types.RowStrategyModeTask}, "recipes", compCanonical.Title, compCanonical.Type)
	if hashAlias != hashCanonical {
		t.Fatalf("alias hash = %q, canonical hash = %q, want match", hashAlias, hashCanonical)
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
		ID:          "sheet-1:c1:task-1:task",
		SheetID:     "sheet-1",
		ComponentID: "c1",
		GroupID:     "task-1",
		TaskID:      "task-1",
		RowKey:      "task",
		SchemaHash:  "testhash123",
		OutputIDs:   []string{"out-1", "out-2"},
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

func TestHashColumnsStable(t *testing.T) {
	comp := types.ComponentSpec{
		Type:  types.ComponentTypeTable,
		Title: "Output Table",
		DataSource: &types.DataSource{
			Transform: []types.TransformRule{
				{Column: "file_name", Source: "data.file_name", Type: "text"},
				{Column: "video_url", Source: "data.video_url", Type: "link"},
			},
		},
	}

	cols := buildColumnSchemas(comp)
	h1 := hashColumns(cols, types.RowStrategy{Mode: types.RowStrategyModeTask}, "test-sheet", comp.Title, comp.Type)
	h2 := hashColumns(cols, types.RowStrategy{Mode: types.RowStrategyModeTask}, "test-sheet", comp.Title, comp.Type)
	if h1 != h2 {
		t.Fatalf("hashColumns not stable: %q vs %q", h1, h2)
	}

	compJSON, _ := json.Marshal(comp)
	var comp2 types.ComponentSpec
	json.Unmarshal(compJSON, &comp2)
	cols2 := buildColumnSchemas(comp2)
	h3 := hashColumns(cols2, types.RowStrategy{Mode: types.RowStrategyModeTask}, "test-sheet", comp2.Title, comp2.Type)
	if h1 != h3 {
		t.Fatalf("hashColumns not stable across JSON round-trip: %q vs %q", h1, h3)
	}

	h4 := hashColumns(cols, types.RowStrategy{Mode: types.RowStrategyModeTask}, "test-sheet", "Renamed Table", comp.Type)
	if h1 == h4 {
		t.Fatalf("hashColumns should change when title changes: %q", h1)
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

func TestSerializeOutputsForMappingSortsOutputsDeterministically(t *testing.T) {
	older := newRecipeOutput("out-b")
	older.TaskID = "task-1"
	older.CreatedAt = time.Date(2026, 3, 16, 10, 0, 0, 0, time.UTC)
	older.Title = "Older"

	newer := newRecipeOutput("out-a")
	newer.TaskID = "task-1"
	newer.CreatedAt = time.Date(2026, 3, 16, 11, 0, 0, 0, time.UTC)
	newer.Title = "Newer"

	raw, err := serializeOutputsForMapping([]*types.TaskOutput{newer, older}, map[string]string{"task-1": "prompt"})
	if err != nil {
		t.Fatalf("serializeOutputsForMapping returned error: %v", err)
	}
	if !strings.Contains(raw, "<<<BEGIN_TASK id=task-1>>>") {
		t.Fatalf("serialized payload missing task marker: %s", raw)
	}
	if !strings.Contains(raw, "PROMPT: prompt") {
		t.Fatalf("serialized payload missing prompt: %s", raw)
	}

	first := strings.Index(raw, "<<<BEGIN_OUTPUT id=out-b>>>")
	second := strings.Index(raw, "<<<BEGIN_OUTPUT id=out-a>>>")
	if first < 0 || second < 0 {
		t.Fatalf("serialized payload missing ordered output markers: %s", raw)
	}
	if first >= second {
		t.Fatalf("output order was not deterministic: %s", raw)
	}
}

func TestSerializeOutputsForMappingSanitizesNoisyMarkup(t *testing.T) {
	output := newRecipeOutput("out-1")
	output.TaskID = "task-1"
	output.Data = map[string]any{
		"recipe_name": "Top 4 Lemon Hacks",
	}
	output.Metadata = map[string]any{
		"source_input": map[string]any{
			"content": "<li>Cut lemon</li><li>Freeze zest</li>",
		},
		"source_excerpt": "\u001b[32m✓\u001b[0m PDF saved to /workspace/file.pdf",
		"data_fields": []any{
			map[string]any{"key": "recipe_name", "label": "Recipe Name", "type": "text"},
		},
	}

	raw, err := serializeOutputsForMapping([]*types.TaskOutput{output}, map[string]string{"task-1": "Prompt"})
	if err != nil {
		t.Fatalf("serializeOutputsForMapping returned error: %v", err)
	}
	if strings.Contains(raw, "\\u003c") {
		t.Fatalf("serialized payload should not contain escaped HTML tags: %s", raw)
	}
	if strings.Contains(raw, "<li>") {
		t.Fatalf("serialized payload should strip markup tags: %s", raw)
	}
	if strings.Contains(raw, "\u001b[32m") {
		t.Fatalf("serialized payload should strip ANSI escapes: %s", raw)
	}
	if !strings.Contains(raw, "source_input_excerpt: Cut lemon Freeze zest") {
		t.Fatalf("serialized payload missing condensed source input excerpt: %s", raw)
	}
	if !strings.Contains(raw, "data_fields: recipe_name [Recipe Name: text]") {
		t.Fatalf("serialized payload missing summarized data_fields: %s", raw)
	}
}

func TestCanonicalizeMappedRowsMergesDuplicateRowKeysDeterministically(t *testing.T) {
	rows := canonicalizeMappedRows(
		[]bamltypes.ColumnSchema{
			{Key: "name"},
			{Key: "email"},
		},
		[]bamltypes.MappedRow{
			{
				Task_id:           "task-1",
				Row_key:           "Alice Smith",
				Source_output_ids: []string{"out-2"},
				Cells: []bamltypes.MappedCell{
					{Column: "name", Value: ""},
					{Column: "email", Value: "alice@example.com"},
				},
			},
			{
				Task_id:           "task-1",
				Row_key:           "alice-smith",
				Source_output_ids: []string{"out-1"},
				Cells: []bamltypes.MappedCell{
					{Column: "name", Value: "Alice Smith"},
					{Column: "email", Value: ""},
				},
			},
		},
	)

	if got, want := len(rows), 1; got != want {
		t.Fatalf("row count = %d, want %d", got, want)
	}
	if got, want := rows[0].Row_key, "alice-smith"; got != want {
		t.Fatalf("row key = %q, want %q", got, want)
	}
	if got, want := rows[0].Source_output_ids, []string{"out-1", "out-2"}; !slicesMatch(got, want) {
		t.Fatalf("source output ids = %v, want %v", got, want)
	}
	if got, want := len(rows[0].Cells), 2; got != want {
		t.Fatalf("cell count = %d, want %d", got, want)
	}
	if got, want := rows[0].Cells[0].Column, "name"; got != want {
		t.Fatalf("first cell column = %q, want %q", got, want)
	}
	if got, want := rows[0].Cells[0].Value, "Alice Smith"; got != want {
		t.Fatalf("first cell value = %q, want %q", got, want)
	}
	if got, want := rows[0].Cells[1].Column, "email"; got != want {
		t.Fatalf("second cell column = %q, want %q", got, want)
	}
	if got, want := rows[0].Cells[1].Value, "alice@example.com"; got != want {
		t.Fatalf("second cell value = %q, want %q", got, want)
	}
}

func TestCanonicalizeMappedRowsResolvesUniqueColumnAliases(t *testing.T) {
	rows := canonicalizeMappedRows(
		[]bamltypes.ColumnSchema{
			{Key: "recipe_name", Name: "Recipe"},
			{Key: "source_url", Name: "Video"},
		},
		[]bamltypes.MappedRow{
			{
				Task_id: "task-1",
				Cells: []bamltypes.MappedCell{
					{Column: "recipe", Value: "Top 4 Lemon Hacks"},
					{Column: "video", Value: "https://example.com/video"},
				},
			},
		},
	)

	if got, want := len(rows), 1; got != want {
		t.Fatalf("row count = %d, want %d", got, want)
	}
	if got, want := rows[0].Cells[0].Column, "recipe_name"; got != want {
		t.Fatalf("first cell column = %q, want %q", got, want)
	}
	if got, want := rows[0].Cells[0].Value, "Top 4 Lemon Hacks"; got != want {
		t.Fatalf("first cell value = %q, want %q", got, want)
	}
	if got, want := rows[0].Cells[1].Column, "source_url"; got != want {
		t.Fatalf("second cell column = %q, want %q", got, want)
	}
	if got, want := rows[0].Cells[1].Value, "https://example.com/video"; got != want {
		t.Fatalf("second cell value = %q, want %q", got, want)
	}
}

func TestMappedRowToViewRowSanitizesSourceOutputIDs(t *testing.T) {
	now := time.Now()
	outputs := []*types.TaskOutput{
		{ID: "out-2"},
		{ID: "out-1"},
	}

	row := mappedRowToViewRow("sheet-1", "c1", "task-1", "schema", outputs, bamltypes.MappedRow{
		Row_key:           "task",
		Source_output_ids: []string{"out-2", "foreign-id"},
		Cells:             []bamltypes.MappedCell{{Column: "name", Value: "Alice"}},
	}, now)
	if got, want := row.SourceOutputIDs, []string{"out-2"}; !slicesMatch(got, want) {
		t.Fatalf("sanitized source ids = %v, want %v", got, want)
	}
	if got, want := row.ComponentID, "c1"; got != want {
		t.Fatalf("component id = %q, want %q", got, want)
	}

	fallback := mappedRowToViewRow("sheet-1", "c1", "task-1", "schema", outputs, bamltypes.MappedRow{
		Row_key:           "task",
		Source_output_ids: []string{"foreign-id"},
	}, now)
	if got, want := fallback.SourceOutputIDs, []string{"out-1", "out-2"}; !slicesMatch(got, want) {
		t.Fatalf("fallback source ids = %v, want %v", got, want)
	}
}

func TestStableRowIDIncludesComponentScope(t *testing.T) {
	first := stableRowID("sheet-1", "c1", "task-1", "task")
	second := stableRowID("sheet-1", "c2", "task-1", "task")
	if first == second {
		t.Fatalf("stable row ids should differ by component: %q", first)
	}
}

func TestGroupRowsFreshReportsMismatchReasons(t *testing.T) {
	row := fallbackViewRow("sheet-1", "c1", "task-1", "schema-1", []*types.TaskOutput{{ID: "out-1"}}, time.Now())

	if ok, reason := groupRowsFresh([]ViewRow{row}, "c1", "schema-1", []string{"out-1"}); !ok || reason != "" {
		t.Fatalf("expected fresh rows, got ok=%v reason=%q", ok, reason)
	}
	if ok, reason := groupRowsFresh([]ViewRow{row}, "c2", "schema-1", []string{"out-1"}); ok || reason != "component_scope_mismatch" {
		t.Fatalf("expected component mismatch, got ok=%v reason=%q", ok, reason)
	}
	if ok, reason := groupRowsFresh([]ViewRow{row}, "c1", "schema-2", []string{"out-1"}); ok || reason != "schema_hash_mismatch" {
		t.Fatalf("expected schema mismatch, got ok=%v reason=%q", ok, reason)
	}
	if ok, reason := groupRowsFresh([]ViewRow{row}, "c1", "schema-1", []string{"out-2"}); ok || reason != "output_ids_mismatch" {
		t.Fatalf("expected output mismatch, got ok=%v reason=%q", ok, reason)
	}
	if ok, reason := groupRowsFresh(nil, "c1", "schema-1", []string{"out-1"}); ok || reason != "missing_rows" {
		t.Fatalf("expected missing rows, got ok=%v reason=%q", ok, reason)
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
