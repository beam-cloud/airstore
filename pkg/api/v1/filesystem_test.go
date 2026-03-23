package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/gateway/services"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type sourceStatProviderStub struct{}

func (sourceStatProviderStub) Name() string { return string(types.Wikipedia) }

func (sourceStatProviderStub) Stat(context.Context, *sources.ProviderContext, string) (*sources.FileInfo, error) {
	return nil, errors.New("unexpected provider stat")
}

func (sourceStatProviderStub) ReadDir(context.Context, *sources.ProviderContext, string) ([]sources.DirEntry, error) {
	return nil, errors.New("unexpected provider readdir")
}

func (sourceStatProviderStub) Read(context.Context, *sources.ProviderContext, string, int64, int64) ([]byte, error) {
	return nil, errors.New("unexpected provider read")
}

func (sourceStatProviderStub) Readlink(context.Context, *sources.ProviderContext, string) (string, error) {
	return "", errors.New("unexpected provider readlink")
}

func (sourceStatProviderStub) Search(context.Context, *sources.ProviderContext, string, int) ([]sources.SearchResult, error) {
	return nil, errors.New("unexpected provider search")
}

func TestFilesystemStatMarksSourceViewFoldersAsDirectories(t *testing.T) {
	group, query := newSourceStatTestGroup(t)

	resp := statFilesystemPath(t, group, query.Path)
	if !resp.Success {
		t.Fatalf("success = false, want true")
	}
	if !resp.Data.IsFolder {
		t.Fatalf("is_folder = false, want true")
	}
	if got := resp.Data.Path; got != query.Path {
		t.Fatalf("path = %q, want %q", got, query.Path)
	}
	if got := resp.Data.ChildCount; got == 0 {
		t.Fatalf("child_count = 0, want > 0")
	}
	if got, _ := resp.Data.Metadata[types.MetaKeyExternalID].(string); got != query.ExternalId {
		t.Fatalf("external_id = %q, want %q", got, query.ExternalId)
	}
	if got, _ := resp.Data.Metadata[types.MetaKeyGuidance].(string); got != query.Guidance {
		t.Fatalf("guidance = %q, want %q", got, query.Guidance)
	}
}

func TestFilesystemStatSupportsFolderQueryMetadataFiles(t *testing.T) {
	group, query := newSourceStatTestGroup(t)

	resp := statFilesystemPath(t, group, query.Path+"/.query.as")
	if !resp.Success {
		t.Fatalf("success = false, want true")
	}
	if resp.Data.IsFolder {
		t.Fatalf("is_folder = true, want false")
	}
	if got := resp.Data.Path; got != query.Path+"/.query.as" {
		t.Fatalf("path = %q, want %q", got, query.Path+"/.query.as")
	}
	if resp.Data.Size == 0 {
		t.Fatalf("size = 0, want > 0")
	}
}

type filesystemStatResponse struct {
	Success bool              `json:"success"`
	Data    types.VirtualFile `json:"data"`
	Error   string            `json:"error"`
}

func statFilesystemPath(t *testing.T, group *FilesystemGroup, path string) filesystemStatResponse {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/fs/stat?path="+path, nil)
	req = req.WithContext(auth.WithAuthInfo(req.Context(), &types.AuthInfo{
		TokenType: types.TokenTypeWorkspaceMember,
		Workspace: &types.WorkspaceInfo{Id: 1, ExternalId: "ws-1", Name: "Workspace"},
		Member:    &types.MemberInfo{Id: 7, ExternalId: "mem-1", Email: "test@example.com", Role: types.RoleAdmin},
	}))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := group.Stat(c); err != nil {
		t.Fatalf("Stat returned error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp filesystemStatResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return resp
}

func newSourceStatTestGroup(t *testing.T) (*FilesystemGroup, *types.FilesystemQuery) {
	t.Helper()

	registry := sources.NewRegistry()
	registry.Register(sourceStatProviderStub{})

	store := repository.NewMemoryFilesystemStore()
	now := time.Unix(1_700_000_000, 0).UTC()
	query := &types.FilesystemQuery{
		ExternalId:   "view-external-id",
		WorkspaceId:  1,
		Integration:  string(types.Wikipedia),
		Path:         "/sources/wikipedia/followup-view",
		Name:         "followup-view",
		QuerySpec:    `{"topic":"beam"}`,
		Guidance:     "Follow-up watch for Beam replies.",
		OutputFormat: types.ViewOutputFolder,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if _, err := store.CreateQuery(context.Background(), query); err != nil {
		t.Fatalf("CreateQuery returned error: %v", err)
	}
	if err := store.StoreQueryResults(context.Background(), 1, query.Path, []repository.QueryResult{{
		ID:       "result-1",
		Filename: "reply.txt",
		Size:     42,
		Mtime:    now.Unix(),
	}}, 0); err != nil {
		t.Fatalf("StoreQueryResults returned error: %v", err)
	}

	sourceService := services.NewSourceService(registry, nil, store)
	return &FilesystemGroup{
		sourceService:  sourceService,
		sourceRegistry: registry,
	}, query
}
