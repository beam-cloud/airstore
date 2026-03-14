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
		Layout:      types.LayoutConfig{Columns: 12},
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
	}

	normalizeViewDefinition(&def)

	if got, want := def.Agents, []string{"campaign-beta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Agents = %#v, want %#v", got, want)
	}
	if got, want := def.Components[0].DataSource.AgentID, "campaign-beta"; got != want {
		t.Fatalf("DataSource.AgentID = %q, want %q", got, want)
	}
	if got, want := def.Components[0].DataSource.AgentIDs, []string{"campaign-beta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DataSource.AgentIDs = %#v, want %#v", got, want)
	}
	if got, want := def.Components[1].Config["agent_ids"], []string{"campaign-beta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Config agent_ids = %#v, want %#v", got, want)
	}
}

func TestNormalizeViewDefinitionKeepsExplicitAgentsWhenComponentsDoNotReferenceAny(t *testing.T) {
	def := types.ViewDefinition{
		Name:        "Overview",
		Description: "Workspace summary",
		Agents:      []string{"ops-agent", "ops-agent", "", "qa-agent"},
		Layout:      types.LayoutConfig{Columns: 12},
		Components: []types.ComponentSpec{
			{
				ID:       "metric-1",
				Type:     types.ComponentTypeMetric,
				Position: types.Position{Col: 0, Row: 0, ColSpan: 6, RowSpan: 1},
			},
		},
	}

	normalizeViewDefinition(&def)

	if got, want := def.Agents, []string{"ops-agent", "qa-agent"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Agents = %#v, want %#v", got, want)
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
		Layout:      types.LayoutConfig{Columns: 12},
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
	if got, want := def.Components[0].Config["agent_id"], any("agent-uuid-1"); got != want {
		t.Fatalf("Config agent_id = %#v, want %#v", got, want)
	}
	if got, want := def.Components[1].DataSource.AgentID, "agent-uuid-1"; got != want {
		t.Fatalf("DataSource.AgentID = %q, want %q", got, want)
	}
}

func TestCanonicalizeViewAgentRefsUsesOperationResultsForNewAgents(t *testing.T) {
	def := types.ViewDefinition{
		Name:        "Recipe extraction",
		Description: "Process videos",
		Agents:      []string{"YouTube Recipe Extractor"},
		Layout:      types.LayoutConfig{Columns: 12},
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
	if got, want := def.Components[0].Config["agent_id"], any("agent-uuid-2"); got != want {
		t.Fatalf("Config agent_id = %#v, want %#v", got, want)
	}
}
