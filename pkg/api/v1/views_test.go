package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/beam-cloud/airstore/pkg/views"
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

func TestApplyColumnRenamesToDefinitionScopesRenameToComponent(t *testing.T) {
	def := types.ViewDefinition{
		Sheets: []types.SheetSpec{
			{
				ID: "sheet-a",
				Components: []types.ComponentSpec{
					{
						ID: "table-a",
						DataSource: &types.DataSource{
							Transform: []types.TransformRule{
								{Column: "shared", Source: "shared", Type: "text"},
							},
						},
						Config: map[string]any{
							"columns": []any{
								map[string]any{"key": "shared", "label": "Shared A", "type": "text"},
							},
						},
					},
					{
						ID: "table-b",
						DataSource: &types.DataSource{
							Transform: []types.TransformRule{
								{Column: "shared", Source: "shared", Type: "text"},
							},
						},
						Config: map[string]any{
							"columns": []any{
								map[string]any{"key": "shared", "label": "Shared B", "type": "text"},
							},
						},
					},
				},
			},
		},
	}

	applyColumnRenamesToDefinition(&def, []columnRename{
		{SheetID: "sheet-a", ComponentID: "table-a", OldKey: "shared", NewKey: "renamed_shared", NewLabel: "Renamed Shared"},
	})

	if got := def.Sheets[0].Components[0].DataSource.Transform[0].Column; got != "renamed_shared" {
		t.Fatalf("table-a transform column = %q, want renamed_shared", got)
	}
	if got := def.Sheets[0].Components[1].DataSource.Transform[0].Column; got != "shared" {
		t.Fatalf("table-b transform column = %q, want shared", got)
	}
}

func TestDeletedViewColumnsScopesColumnsByComponent(t *testing.T) {
	previous := types.ViewDefinition{
		Sheets: []types.SheetSpec{
			{
				ID: "sheet-a",
				Components: []types.ComponentSpec{
					{
						ID:   "table-a",
						Type: types.ComponentTypeTable,
						Config: map[string]any{"columns": []any{
							map[string]any{"key": "shared", "label": "Shared", "type": "text"},
							map[string]any{"key": "only_a", "label": "Only A", "type": "text"},
						}},
						DataSource: &types.DataSource{Transform: []types.TransformRule{
							{Column: "shared", Source: "shared", Type: "text"},
							{Column: "only_a", Source: "only_a", Type: "text"},
						}},
					},
					{
						ID:   "table-b",
						Type: types.ComponentTypeTable,
						Config: map[string]any{"columns": []any{
							map[string]any{"key": "shared", "label": "Shared", "type": "text"},
						}},
						DataSource: &types.DataSource{Transform: []types.TransformRule{
							{Column: "shared", Source: "shared", Type: "text"},
						}},
					},
				},
			},
		},
	}
	next := types.ViewDefinition{
		Sheets: []types.SheetSpec{
			{
				ID: "sheet-a",
				Components: []types.ComponentSpec{
					{
						ID:   "table-a",
						Type: types.ComponentTypeTable,
						Config: map[string]any{"columns": []any{
							map[string]any{"key": "shared", "label": "Shared", "type": "text"},
						}},
						DataSource: &types.DataSource{Transform: []types.TransformRule{
							{Column: "shared", Source: "shared", Type: "text"},
						}},
					},
					{
						ID:   "table-b",
						Type: types.ComponentTypeTable,
						Config: map[string]any{"columns": []any{
							map[string]any{"key": "shared", "label": "Shared", "type": "text"},
						}},
						DataSource: &types.DataSource{Transform: []types.TransformRule{
							{Column: "shared", Source: "shared", Type: "text"},
						}},
					},
				},
			},
		},
	}

	deleted := deletedViewColumns(previous, next)
	if got, want := len(deleted), 1; got != want {
		t.Fatalf("deleted column count = %d, want %d", got, want)
	}
	if got := deleted[0]; got.SheetID != "sheet-a" || got.ComponentID != "table-a" || got.Key != "only_a" {
		t.Fatalf("deleted column = %#v, want sheet-a/table-a/only_a", got)
	}
}

func TestSyntheticEmailThreadsSkipsOutputsWhenRealThreadExists(t *testing.T) {
	outputs := []*types.TaskOutput{
		{
			ID:         "out-1",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusActive,
			Data: map[string]any{
				"thread_id": "thread-1",
				"recipient": "Mike <mike@example.com>",
				"subject":   "A faster way to spin up dev environments",
				"content":   "Hey Mike, ...",
			},
		},
	}
	existing := map[string][]views.ThreadMessage{
		"gmail:thread-1": {{
			ID:       "gmail-msg-1",
			ThreadID: "thread-1",
			Subject:  "A faster way to spin up dev environments",
		}},
	}

	got := syntheticEmailThreads(outputs, existing)
	if len(got) != 0 {
		t.Fatalf("expected no synthetic threads when gmail thread exists, got %#v", got)
	}
}

func TestSyntheticEmailThreadsFallsBackWhenNoRealThreadExists(t *testing.T) {
	outputs := []*types.TaskOutput{
		{
			ID:         "out-1",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusActive,
			Data: map[string]any{
				"thread_id": "thread-1",
				"recipient": "Mike <mike@example.com>",
				"subject":   "A faster way to spin up dev environments",
				"content":   "Hey Mike, ...",
			},
		},
	}

	got := syntheticEmailThreads(outputs, nil)
	thread := got["gmail:thread-1"]
	if len(thread) != 1 {
		t.Fatalf("expected one synthetic thread message, got %#v", got)
	}
	if thread[0].ThreadID != "thread-1" {
		t.Fatalf("thread id = %q, want thread-1", thread[0].ThreadID)
	}
}

type mailboxTestBackend struct {
	repository.BackendRepository
	workspace *types.Workspace
	view      *types.View
	outputs   []*types.TaskOutput
}

func (b *mailboxTestBackend) GetWorkspaceByExternalId(context.Context, string) (*types.Workspace, error) {
	return b.workspace, nil
}

func (b *mailboxTestBackend) GetView(context.Context, uint, string) (*types.View, error) {
	return b.view, nil
}

func (b *mailboxTestBackend) GetConnection(context.Context, uint, uint, string) (*types.IntegrationConnection, error) {
	return nil, errors.New("connection not configured")
}

func (b *mailboxTestBackend) ListWorkspaceTaskOutputs(_ context.Context, _ uint, filter types.TaskOutputListFilter) ([]*types.TaskOutput, error) {
	var result []*types.TaskOutput
	for _, output := range b.outputs {
		if output == nil {
			continue
		}
		if filter.OutputType != nil && output.OutputType != *filter.OutputType {
			continue
		}
		if filter.SourceViewID != nil && *filter.SourceViewID != "" {
			result = append(result, output)
			continue
		}
		if len(filter.TaskIDs) > 0 {
			for _, taskID := range filter.TaskIDs {
				if output.TaskID == taskID {
					result = append(result, output)
					break
				}
			}
		}
	}
	return result, nil
}

func (b *mailboxTestBackend) ListChildTaskIDsByParents(context.Context, []string) (map[string]string, error) {
	return nil, nil
}

func mailboxResponseData(t *testing.T, vg *ViewsGroup, backend *mailboxTestBackend) struct {
	Threads          map[string]mailboxThread `json:"threads"`
	HasEmailActivity bool                     `json:"has_email_activity"`
} {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/workspaces/ws-1/views/view-1/mailbox?integration=all", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetParamNames("workspace_id", "view_id")
	c.SetParamValues("ws-1", "view-1")

	if err := vg.Mailbox(c); err != nil {
		t.Fatalf("Mailbox returned error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200", rec.Code)
	}

	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			Threads          map[string]mailboxThread `json:"threads"`
			HasEmailActivity bool                     `json:"has_email_activity"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode mailbox response: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success response, got %s", rec.Body.String())
	}
	return resp.Data
}

func TestMailboxReturnsProviderQualifiedThreadRefsForDeferredGmailOutput(t *testing.T) {
	backend := &mailboxTestBackend{
		workspace: &types.Workspace{Id: 7},
		view:      &types.View{Definition: types.ViewDefinition{}},
		outputs: []*types.TaskOutput{{
			ID:         "out-1",
			TaskID:     "task-1",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusActive,
			Data: map[string]any{
				"thread_id": "thread-123",
				"to":        "luke@example.com",
				"subject":   "Beam sandboxes",
				"status":    "sent",
			},
			Metadata: map[string]any{
				"integration": "gmail",
				"_tool":       "gmail",
			},
		}},
	}
	vg := &ViewsGroup{backend: backend}

	data := mailboxResponseData(t, vg, backend)

	if !data.HasEmailActivity {
		t.Fatal("expected mailbox email activity")
	}
	thread, ok := data.Threads["gmail:thread-123"]
	if !ok {
		t.Fatalf("expected gmail provider-qualified thread key, got %#v", data.Threads)
	}
	if got := thread.Messages[0].ThreadID; got != "thread-123" {
		t.Fatalf("thread id = %q, want thread-123", got)
	}
}

func TestMailboxReturnsProviderQualifiedThreadRefsForDeferredOutlookOutput(t *testing.T) {
	backend := &mailboxTestBackend{
		workspace: &types.Workspace{Id: 7},
		view:      &types.View{Definition: types.ViewDefinition{}},
		outputs: []*types.TaskOutput{{
			ID:         "out-1",
			TaskID:     "task-1",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusActive,
			Data: map[string]any{
				"conversation_id": "conv-123",
				"thread_id":       "conv-123",
				"to":              "luke@example.com",
				"subject":         "Beam sandboxes",
				"status":          "sent",
			},
			Metadata: map[string]any{
				"integration": "outlook",
				"_tool":       "outlook",
			},
		}},
	}
	vg := &ViewsGroup{backend: backend}

	data := mailboxResponseData(t, vg, backend)

	if _, ok := data.Threads["outlook:conv-123"]; !ok {
		t.Fatalf("expected outlook provider-qualified thread key, got %#v", data.Threads)
	}
}

func TestMailboxUsesRealDeferredToolOutputAfterApproval(t *testing.T) {
	backend := &mailboxTestBackend{
		workspace: &types.Workspace{Id: 7},
		view:      &types.View{Definition: types.ViewDefinition{}},
		outputs: []*types.TaskOutput{{
			ID:         "draft-1",
			TaskID:     "task-1",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusPending,
			Data: map[string]any{
				"to":      "luke@example.com",
				"subject": "Beam sandboxes",
				"content": "Draft body",
			},
			Metadata: map[string]any{
				types.TaskOutputMetadataApprovalUI: true,
			},
		}},
	}
	vg := &ViewsGroup{backend: backend}

	before := mailboxResponseData(t, vg, backend)
	if before.HasEmailActivity {
		t.Fatalf("expected no mailbox activity before approval, got %#v", before.Threads)
	}

	backend.outputs = append(backend.outputs, &types.TaskOutput{
		ID:         "sent-1",
		TaskID:     "task-1",
		OutputType: types.TaskOutputTypeEmail,
		Status:     types.TaskOutputStatusActive,
		Data: map[string]any{
			"thread_id":  "thread-123",
			"message_id": "msg-123",
			"to":         "luke@example.com",
			"subject":    "Beam sandboxes",
			"status":     "sent",
		},
		Metadata: map[string]any{
			"integration": "gmail",
			"_tool":       "gmail",
		},
	})

	after := mailboxResponseData(t, vg, backend)
	if !after.HasEmailActivity {
		t.Fatal("expected mailbox activity after deferred send output persisted")
	}
	if _, ok := after.Threads["gmail:thread-123"]; !ok {
		t.Fatalf("expected persisted provider thread after approval, got %#v", after.Threads)
	}
}

func TestMailboxTreatsApprovalDraftAsSecondaryWhenSentOutputExists(t *testing.T) {
	backend := &mailboxTestBackend{
		workspace: &types.Workspace{Id: 7},
		view:      &types.View{Definition: types.ViewDefinition{}},
		outputs: []*types.TaskOutput{
			{
				ID:         "draft-1",
				TaskID:     "task-1",
				OutputType: types.TaskOutputTypeEmail,
				Status:     types.TaskOutputStatusActive,
				Data: map[string]any{
					"to":      "luke@example.com",
					"subject": "Beam sandboxes",
					"content": "Draft body",
				},
				Metadata: map[string]any{
					types.TaskOutputMetadataApprovalUI: true,
				},
			},
			{
				ID:         "sent-1",
				TaskID:     "task-1",
				OutputType: types.TaskOutputTypeEmail,
				Status:     types.TaskOutputStatusActive,
				Data: map[string]any{
					"thread_id":  "thread-123",
					"message_id": "msg-123",
					"to":         "luke@example.com",
					"subject":    "Beam sandboxes",
					"status":     "sent",
				},
				Metadata: map[string]any{
					"integration": "gmail",
					"_tool":       "gmail",
				},
			},
		},
	}
	vg := &ViewsGroup{backend: backend}

	data := mailboxResponseData(t, vg, backend)

	if got := len(data.Threads); got != 1 {
		t.Fatalf("thread count = %d, want 1 with sent output preferred", got)
	}
	if _, ok := data.Threads["gmail:thread-123"]; !ok {
		t.Fatalf("expected gmail provider thread key, got %#v", data.Threads)
	}
}

func TestEmailOutputThreadRefUsesOutlookConversationID(t *testing.T) {
	output := &types.TaskOutput{
		OutputType: types.TaskOutputTypeEmail,
		Data: map[string]any{
			"conversation_id": "conv-123",
		},
	}

	ref := emailOutputThreadRef(output)
	if got := ref.ID; got != "conv-123" {
		t.Fatalf("thread ref id = %q, want conv-123", got)
	}
	if got := ref.Integration; got != string(types.SourceOutlook) {
		t.Fatalf("thread ref integration = %q, want outlook", got)
	}
}

func TestEmailOutputIntegrationPrefersExplicitMetadataOverURLInference(t *testing.T) {
	output := &types.TaskOutput{
		OutputType: types.TaskOutputTypeEmail,
		Metadata: map[string]any{
			"integration": "outlook",
		},
		Data: map[string]any{
			"thread_id":  "thread-123",
			"email_link": "https://mail.google.com/mail/u/0/#inbox/thread-123",
		},
	}

	if got := emailOutputIntegration(output); got != string(types.SourceOutlook) {
		t.Fatalf("integration = %q, want outlook", got)
	}
}

func TestThreadRefsFromCellValueUsesProvidedIntegration(t *testing.T) {
	refs := threadRefsFromCellValue("thread-a, thread-b, thread-a", string(types.SourceOutlook))
	if got := len(refs); got != 2 {
		t.Fatalf("thread ref count = %d, want 2", got)
	}
	for _, ref := range refs {
		if got := ref.Integration; got != string(types.SourceOutlook) {
			t.Fatalf("thread ref integration = %q, want outlook", got)
		}
	}
}

func TestDecodeViewRowID(t *testing.T) {
	got, err := decodeViewRowID("sheet-1%3Ac1%3Atask-1%3Atask")
	if err != nil {
		t.Fatalf("decodeViewRowID returned error: %v", err)
	}
	if want := "sheet-1:c1:task-1:task"; got != want {
		t.Fatalf("decoded row id = %q, want %q", got, want)
	}
}

func TestDecodeViewRowIDRejectsInvalidEscapes(t *testing.T) {
	if _, err := decodeViewRowID("sheet-1%ZZ"); err == nil {
		t.Fatal("expected invalid escape to return error")
	}
}

func TestSyncNameDescriptionPrefersTopLevelOverDefinition(t *testing.T) {
	view := &types.View{
		Name:        "Top name",
		Description: "Top desc",
		Definition: types.ViewDefinition{
			Name:        "Def name",
			Description: "Def desc",
		},
	}
	view.SyncNameDescription()

	if got := view.Name; got != "Top name" {
		t.Fatalf("view.Name = %q, want %q", got, "Top name")
	}
	if got := view.Definition.Name; got != "Top name" {
		t.Fatalf("view.Definition.Name = %q, want %q", got, "Top name")
	}
	if got := view.Description; got != "Top desc" {
		t.Fatalf("view.Description = %q, want %q", got, "Top desc")
	}
	if got := view.Definition.Description; got != "Top desc" {
		t.Fatalf("view.Definition.Description = %q, want %q", got, "Top desc")
	}
}

func TestSyncNameDescriptionFallsBackToDefinition(t *testing.T) {
	view := &types.View{
		Definition: types.ViewDefinition{
			Name:        "Def name",
			Description: "Def desc",
		},
	}
	view.SyncNameDescription()

	if got := view.Name; got != "Def name" {
		t.Fatalf("view.Name = %q, want %q", got, "Def name")
	}
	if got := view.Definition.Name; got != "Def name" {
		t.Fatalf("view.Definition.Name = %q, want %q", got, "Def name")
	}
}
