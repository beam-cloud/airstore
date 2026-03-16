package apiv1

import (
	"encoding/json"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
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
