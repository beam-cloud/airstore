package apiv1

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

func TestApplyColumnRenamesToDefinitionUpdatesSchemaHintsAndRelations(t *testing.T) {
	def := types.ViewDefinition{
		Sheets: []types.SheetSpec{
			{
				ID: "sheet-a",
				Components: []types.ComponentSpec{
					{
						ID: "table-a",
						DataSource: &types.DataSource{
							Transform: []types.TransformRule{
								{Column: "company", Source: "company", Type: "text"},
								{Column: "website", Source: "data.website", Type: "link"},
							},
						},
						Config: map[string]any{
							"columns": []any{
								map[string]any{"key": "company", "label": "Company", "type": "text"},
								map[string]any{"key": "website", "label": "Website", "type": "link"},
							},
						},
					},
				},
				Relations: []types.SheetRelation{
					{ID: "rel-from", ToSheetID: "sheet-b", FromColumn: "company", ToColumn: "account_name"},
				},
			},
			{
				ID: "sheet-b",
				Relations: []types.SheetRelation{
					{ID: "rel-to", ToSheetID: "sheet-a", FromColumn: "account_name", ToColumn: "company"},
				},
			},
		},
	}

	applyColumnRenamesToDefinition(&def, []columnRename{
		{SheetID: "sheet-a", OldKey: "company", NewKey: "company_name", NewLabel: "Company Name"},
	})

	rule := def.Sheets[0].Components[0].DataSource.Transform[0]
	if got := rule.Column; got != "company_name" {
		t.Fatalf("transform column = %q, want company_name", got)
	}
	if got := rule.Source; got != "company_name" {
		t.Fatalf("placeholder transform source = %q, want company_name", got)
	}
	if got := def.Sheets[0].Components[0].DataSource.Transform[1].Source; got != "data.website" {
		t.Fatalf("bound transform source should remain unchanged, got %q", got)
	}
	if got := def.Sheets[0].Relations[0].FromColumn; got != "company_name" {
		t.Fatalf("from_column = %q, want company_name", got)
	}
	if got := def.Sheets[1].Relations[0].ToColumn; got != "company_name" {
		t.Fatalf("to_column = %q, want company_name", got)
	}

	rawColumns, err := json.Marshal(def.Sheets[0].Components[0].Config["columns"])
	if err != nil {
		t.Fatalf("marshal columns: %v", err)
	}
	var columns []types.ColumnMeta
	if err := json.Unmarshal(rawColumns, &columns); err != nil {
		t.Fatalf("unmarshal columns: %v", err)
	}
	if got := columns[0].Key; got != "company_name" {
		t.Fatalf("config column key = %q, want company_name", got)
	}
	if got := columns[0].Label; got != "Company Name" {
		t.Fatalf("config column label = %q, want Company Name", got)
	}
}

func TestApplyColumnRenamesToDefinitionDoesNotChainOverlappingRenames(t *testing.T) {
	def := types.ViewDefinition{
		Sheets: []types.SheetSpec{
			{
				ID: "sheet-a",
				Components: []types.ComponentSpec{
					{
						ID: "table-a",
						DataSource: &types.DataSource{
							Transform: []types.TransformRule{
								{Column: "a", Source: "a", Type: "text"},
								{Column: "b", Source: "b", Type: "text"},
							},
						},
						Config: map[string]any{
							"columns": []any{
								map[string]any{"key": "a", "label": "A", "type": "text"},
								map[string]any{"key": "b", "label": "B", "type": "text"},
							},
						},
					},
				},
				Relations: []types.SheetRelation{
					{ID: "rel-from-a", ToSheetID: "sheet-b", FromColumn: "a", ToColumn: "alpha"},
					{ID: "rel-from-b", ToSheetID: "sheet-b", FromColumn: "b", ToColumn: "beta"},
				},
			},
			{
				ID: "sheet-b",
				Relations: []types.SheetRelation{
					{ID: "rel-to-a", ToSheetID: "sheet-a", FromColumn: "alpha", ToColumn: "a"},
					{ID: "rel-to-b", ToSheetID: "sheet-a", FromColumn: "beta", ToColumn: "b"},
				},
			},
		},
	}

	applyColumnRenamesToDefinition(&def, []columnRename{
		{SheetID: "sheet-a", OldKey: "a", NewKey: "b", NewLabel: "B Prime"},
		{SheetID: "sheet-a", OldKey: "b", NewKey: "c", NewLabel: "C"},
	})

	transform := def.Sheets[0].Components[0].DataSource.Transform
	if got := transform[0].Column; got != "b" {
		t.Fatalf("transform[0] column = %q, want b", got)
	}
	if got := transform[0].Source; got != "b" {
		t.Fatalf("transform[0] source = %q, want b", got)
	}
	if got := transform[1].Column; got != "c" {
		t.Fatalf("transform[1] column = %q, want c", got)
	}
	if got := transform[1].Source; got != "c" {
		t.Fatalf("transform[1] source = %q, want c", got)
	}

	if got := def.Sheets[0].Relations[0].FromColumn; got != "b" {
		t.Fatalf("sheet-a relation for original a = %q, want b", got)
	}
	if got := def.Sheets[0].Relations[1].FromColumn; got != "c" {
		t.Fatalf("sheet-a relation for original b = %q, want c", got)
	}
	if got := def.Sheets[1].Relations[0].ToColumn; got != "b" {
		t.Fatalf("sheet-b relation to original a = %q, want b", got)
	}
	if got := def.Sheets[1].Relations[1].ToColumn; got != "c" {
		t.Fatalf("sheet-b relation to original b = %q, want c", got)
	}

	rawColumns, err := json.Marshal(def.Sheets[0].Components[0].Config["columns"])
	if err != nil {
		t.Fatalf("marshal columns: %v", err)
	}
	var columns []types.ColumnMeta
	if err := json.Unmarshal(rawColumns, &columns); err != nil {
		t.Fatalf("unmarshal columns: %v", err)
	}
	if got := columns[0].Key; got != "b" {
		t.Fatalf("first config column key = %q, want b", got)
	}
	if got := columns[0].Label; got != "B Prime" {
		t.Fatalf("first config column label = %q, want B Prime", got)
	}
	if got := columns[1].Key; got != "c" {
		t.Fatalf("second config column key = %q, want c", got)
	}
	if got := columns[1].Label; got != "C" {
		t.Fatalf("second config column label = %q, want C", got)
	}
}

func TestDraftsRouteReturnsServiceUnavailableWhenDisabled(t *testing.T) {
	e := echo.New()
	NewViewsGroup(e.Group("/workspaces/:workspace_id/views"), nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/workspaces/ws-1/views/drafts", nil)
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /drafts status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestSyncViewMetadataFromPatchedDefinitionUsesDefinitionValues(t *testing.T) {
	view := &types.View{
		Name:        "Old name",
		Description: "Old description",
		Definition: types.ViewDefinition{
			Name:        "Old name",
			Description: "Old description",
		},
	}
	req := updateViewRequest{
		Definition: &types.ViewDefinition{
			Name:        "New nested name",
			Description: "New nested description",
		},
	}

	syncViewMetadataFromPatchedDefinition(view, req)

	if got := view.Name; got != "New nested name" {
		t.Fatalf("view name = %q, want %q", got, "New nested name")
	}
	if got := view.Description; got != "New nested description" {
		t.Fatalf("view description = %q, want %q", got, "New nested description")
	}
}

func TestSyncViewMetadataFromPatchedDefinitionRespectsExplicitTopLevelPatch(t *testing.T) {
	topLevelName := "Explicit top-level"
	topLevelDescription := "Explicit description"
	view := &types.View{
		Name:        "Old name",
		Description: "Old description",
	}
	req := updateViewRequest{
		Name:        &topLevelName,
		Description: &topLevelDescription,
		Definition: &types.ViewDefinition{
			Name:        "Nested name",
			Description: "Nested description",
		},
	}

	syncViewMetadataFromPatchedDefinition(view, req)

	if got := view.Name; got != "Old name" {
		t.Fatalf("view name = %q, want unchanged old value", got)
	}
	if got := view.Description; got != "Old description" {
		t.Fatalf("view description = %q, want unchanged old value", got)
	}
}
