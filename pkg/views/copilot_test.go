package views

import (
	"reflect"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
	bamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
)

func TestNormalizeViewDefinitionUsesReferencedAgents(t *testing.T) {
	def := types.ViewDefinition{
		Name:        "Mystery shopping",
		Description: "Track campaign progress",
		Agents:      []string{"campaign-alpha", "campaign-beta", "campaign-gamma"},
		Sheets: []types.SheetSpec{
			{
				ID:     "sheet-1",
				Name:   "Campaign summary",
				Layout: types.LayoutConfig{Columns: 12},
				Components: []types.ComponentSpec{
					{
						ID:       "table-1",
						Type:     types.ComponentTypeTable,
						Position: types.Position{Col: 0, Row: 0, ColSpan: 6, RowSpan: 1},
						DataSource: &types.DataSource{
							AgentID:  " campaign-beta ",
							AgentIDs: []string{"campaign-beta", "campaign-beta", ""},
						},
					},
					{
						ID:       "action-1",
						Type:     types.ComponentTypeAction,
						Position: types.Position{Col: 6, Row: 0, ColSpan: 6, RowSpan: 1},
						Config: map[string]any{
							"agent_id":  "campaign-beta",
							"agent_ids": []any{"campaign-beta", "campaign-beta", " "},
						},
					},
				},
			},
		},
	}

	normalizeViewDefinition(&def)

	if got, want := def.Agents, []string{"campaign-beta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Agents = %#v, want %#v", got, want)
	}
	if got, want := def.Sheets[0].Components[0].DataSource.AgentID, "campaign-beta"; got != want {
		t.Fatalf("DataSource.AgentID = %q, want %q", got, want)
	}
	if got, want := def.Sheets[0].Components[0].DataSource.AgentIDs, []string{"campaign-beta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DataSource.AgentIDs = %#v, want %#v", got, want)
	}
	if got, want := def.Sheets[0].Components[1].Config["agent_ids"], []string{"campaign-beta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Config agent_ids = %#v, want %#v", got, want)
	}
}

func TestNormalizeViewDefinitionKeepsExplicitAgentsWhenComponentsDoNotReferenceAny(t *testing.T) {
	def := types.ViewDefinition{
		Name:        "Overview",
		Description: "Workspace summary",
		Agents:      []string{"ops-agent", "ops-agent", "", "qa-agent"},
		Sheets: []types.SheetSpec{{
			ID:     "sheet-1",
			Name:   "Overview",
			Layout: types.LayoutConfig{Columns: 12},
			Components: []types.ComponentSpec{
				{
					ID:       "metric-1",
					Type:     types.ComponentTypeTable,
					Position: types.Position{Col: 0, Row: 0, ColSpan: 6, RowSpan: 1},
				},
			}}},
	}

	normalizeViewDefinition(&def)

	if got, want := def.Agents, []string{"ops-agent", "qa-agent"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Agents = %#v, want %#v", got, want)
	}
}

func TestNormalizeViewDefinitionAssignsUniqueSheetAndComponentIDs(t *testing.T) {
	def := types.ViewDefinition{
		Sheets: []types.SheetSpec{
			{
				ID:     "sheet-1",
				Name:   "One",
				Layout: types.LayoutConfig{Columns: 12},
				Components: []types.ComponentSpec{
					{ID: "c1", Type: types.ComponentTypeTable},
					{ID: "c1", Type: types.ComponentTypeTable},
				},
			},
			{
				ID:     "sheet-1",
				Name:   "Two",
				Layout: types.LayoutConfig{Columns: 12},
				Components: []types.ComponentSpec{
					{ID: "", Type: types.ComponentTypeAction},
				},
			},
			{
				ID:     "",
				Name:   "Three",
				Layout: types.LayoutConfig{Columns: 12},
			},
		},
	}

	normalizeViewDefinition(&def)

	if got, want := def.Sheets[0].ID, "sheet-1"; got != want {
		t.Fatalf("first sheet id = %q, want %q", got, want)
	}
	seenSheetIDs := map[string]bool{}
	for i, sheet := range def.Sheets {
		if sheet.ID == "" {
			t.Fatalf("sheet %d id should not be empty", i)
		}
		if seenSheetIDs[sheet.ID] {
			t.Fatalf("duplicate sheet id %q after normalization", sheet.ID)
		}
		seenSheetIDs[sheet.ID] = true
	}
	if got := def.Sheets[1].ID; got == "sheet-1" {
		t.Fatalf("duplicate sheet id was not rewritten: %q", got)
	}
	if got := def.Sheets[2].ID; got == "" {
		t.Fatal("empty sheet id was not rewritten")
	}
	if got := def.Sheets[0].Components[0].ID; got != "c1" {
		t.Fatalf("first component id = %q, want %q", got, "c1")
	}
	if got := def.Sheets[0].Components[1].ID; got == "" || got == "c1" {
		t.Fatalf("duplicate component id was not rewritten: %q", got)
	}
	if got := def.Sheets[1].Components[0].ID; got == "" {
		t.Fatal("empty component id was not rewritten")
	}
}

func TestNormalizeViewDefinitionPreservesExplicitOutputTypeFilter(t *testing.T) {
	def := types.ViewDefinition{
		Sheets: []types.SheetSpec{{
			ID:     "sheet-1",
			Name:   "Recipes",
			Layout: types.LayoutConfig{Columns: 12},
			Components: []types.ComponentSpec{{
				ID:   "table-1",
				Type: types.ComponentTypeTable,
				DataSource: &types.DataSource{
					AgentID:    "chef-agent",
					OutputType: " json ",
				},
			}},
		}},
	}

	normalizeViewDefinition(&def)

	if got, want := def.Sheets[0].Components[0].DataSource.OutputType, "json"; got != want {
		t.Fatalf("DataSource.OutputType = %q, want %q", got, want)
	}
}

func TestExtractStringSliceHandlesStringArrays(t *testing.T) {
	m := map[string]any{
		"skills": []string{"triage", " review ", "triage", ""},
	}

	if got, want := extractStringSlice(m, "skills"), []string{"triage", "review"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("extractStringSlice = %#v, want %#v", got, want)
	}
}

func TestOperationExecutionStateResolvesInstalledSkillAliases(t *testing.T) {
	state := newOperationExecutionState([]bamltypes.Operation{
		{
			Type: bamltypes.OperationTypeCREATE_SKILL,
			Payload: `{
				"name": "meeting-notes",
				"content": "---\nname: meeting-notes\ndescription: Summarize meetings.\n---\n\n# Meeting notes\n"
			}`,
		},
	})

	if got, want := state.resolveSkillAlias("Meeting Notes"), "meeting-notes"; got != want {
		t.Fatalf("resolveSkillAlias = %q, want %q", got, want)
	}
	if got, want := state.resolveSkillAlias("/skills/meeting-notes"), "meeting-notes"; got != want {
		t.Fatalf("resolveSkillAlias path = %q, want %q", got, want)
	}
}

func TestConfigFromPayloadResolvesKnownSkillAliases(t *testing.T) {
	state := &operationExecutionState{}
	state.rememberSkillAlias("meeting-notes", "meeting-notes")

	config := configFromPayload(map[string]any{
		"skills": []any{"Meeting Notes", "/skills/meeting-notes", "meeting-notes"},
	}, state)

	got, _ := config["skills"].([]string)
	if want := []string{"meeting-notes"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("config skills = %#v, want %#v", got, want)
	}
}

func TestCanonicalizeViewAgentRefsResolvesNamesAndKeys(t *testing.T) {
	def := types.ViewDefinition{
		Name:        "Recipe extraction",
		Description: "Process videos",
		Agents:      []string{"YouTube Recipe Extractor"},
		Sheets: []types.SheetSpec{{
			ID:     "sheet-1",
			Name:   "Recipe extraction",
			Layout: types.LayoutConfig{Columns: 12},
			Components: []types.ComponentSpec{
				{
					ID:       "action-1",
					Type:     types.ComponentTypeAction,
					Position: types.Position{Col: 0, Row: 0, ColSpan: 4, RowSpan: 1},
					Config: map[string]any{
						"agent_id": "YouTube Recipe Extractor",
					},
				},
				{
					ID:       "table-1",
					Type:     types.ComponentTypeTable,
					Position: types.Position{Col: 4, Row: 0, ColSpan: 8, RowSpan: 1},
					DataSource: &types.DataSource{
						AgentID: "youtube-recipe-extractor",
					},
				},
			},
		}},
	}

	canonicalizeViewAgentRefs(&def, []*types.AgentProfile{
		{
			ID:       "agent-uuid-1",
			AgentKey: "youtube-recipe-extractor",
			Name:     "YouTube Recipe Extractor",
		},
	}, nil)
	normalizeViewDefinition(&def)

	if got, want := def.Agents, []string{"agent-uuid-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Agents = %#v, want %#v", got, want)
	}
	if got, want := def.Sheets[0].Components[0].Config["agent_id"], any("agent-uuid-1"); got != want {
		t.Fatalf("Config agent_id = %#v, want %#v", got, want)
	}
	if got, want := def.Sheets[0].Components[1].DataSource.AgentID, "agent-uuid-1"; got != want {
		t.Fatalf("DataSource.AgentID = %q, want %q", got, want)
	}
}

func TestOperationPhaseSortsCreateAgentFirst(t *testing.T) {
	if got := operationPhase(bamltypes.OperationTypeCREATE_AGENT); got != 0 {
		t.Fatalf("CREATE_AGENT phase = %d, want 0", got)
	}
	if got := operationPhase(bamltypes.OperationTypeCREATE_SKILL); got != 1 {
		t.Fatalf("CREATE_SKILL phase = %d, want 1", got)
	}
	if got := operationPhase(bamltypes.OperationTypeINSTALL_SKILL); got != 1 {
		t.Fatalf("INSTALL_SKILL phase = %d, want 1", got)
	}
	if got := operationPhase(bamltypes.OperationTypeASSIGN_SKILL); got != 2 {
		t.Fatalf("ASSIGN_SKILL phase = %d, want 2", got)
	}
	if got := operationPhase(bamltypes.OperationTypeDISPATCH_TASK); got != 2 {
		t.Fatalf("DISPATCH_TASK phase = %d, want 2", got)
	}
}

func TestAgentAliasResolution(t *testing.T) {
	state := &operationExecutionState{
		agentAliases: map[string]string{},
	}
	state.rememberAgent("Email Outreach", "email-outreach", "uuid-123")

	if got, want := state.resolveAgentAlias("uuid-123"), "uuid-123"; got != want {
		t.Fatalf("resolve by ID = %q, want %q", got, want)
	}
	if got, want := state.resolveAgentAlias("email-outreach"), "uuid-123"; got != want {
		t.Fatalf("resolve by key = %q, want %q", got, want)
	}
	if got, want := state.resolveAgentAlias("Email Outreach"), "uuid-123"; got != want {
		t.Fatalf("resolve by name = %q, want %q", got, want)
	}
	if got, want := state.resolveAgentAlias("unknown-agent"), "unknown-agent"; got != want {
		t.Fatalf("resolve unknown = %q, want %q (should pass through)", got, want)
	}
	if got := state.resolveAgentAlias(""); got != "" {
		t.Fatalf("resolve empty = %q, want empty", got)
	}
}

func TestCanonicalizeViewAgentRefsUsesOperationResultsForNewAgents(t *testing.T) {
	def := types.ViewDefinition{
		Name:        "Recipe extraction",
		Description: "Process videos",
		Agents:      []string{"YouTube Recipe Extractor"},
		Sheets: []types.SheetSpec{{
			ID:     "sheet-1",
			Name:   "Recipe extraction",
			Layout: types.LayoutConfig{Columns: 12},
			Components: []types.ComponentSpec{
				{
					ID:       "action-1",
					Type:     types.ComponentTypeAction,
					Position: types.Position{Col: 0, Row: 0, ColSpan: 4, RowSpan: 1},
					Config: map[string]any{
						"agent_id": "YouTube Recipe Extractor",
					},
				},
			},
		}},
	}

	canonicalizeViewAgentRefs(&def, nil, []OperationResult{
		{
			Type:    "create_agent",
			Name:    "YouTube Recipe Extractor",
			Status:  "done",
			AgentID: "agent-uuid-2",
		},
	})
	normalizeViewDefinition(&def)

	if got, want := def.Agents, []string{"agent-uuid-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Agents = %#v, want %#v", got, want)
	}
	if got, want := def.Sheets[0].Components[0].Config["agent_id"], any("agent-uuid-2"); got != want {
		t.Fatalf("Config agent_id = %#v, want %#v", got, want)
	}
}
