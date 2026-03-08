package apiv1

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/skills"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func newTestEchoWithDrafts(t *testing.T) *echo.Echo {
	t.Helper()
	e := echo.New()
	copilot := skills.NewCopilot(nil, nil)
	g := e.Group("/workspaces/:workspace_id/skills")
	NewSkillsGroup(g, nil, nil, copilot)
	return e
}

func TestCreateDraft(t *testing.T) {
	e := newTestEchoWithDrafts(t)

	body := `{"description":"Create an email triage skill"}`
	req := httptest.NewRequest(http.MethodPost, "/workspaces/ws-123/skills/drafts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Data struct {
			DraftID string `json:"draft_id"`
		} `json:"data"`
	}
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Data.DraftID)
}

func TestCreateDraftMissingDescription(t *testing.T) {
	e := newTestEchoWithDrafts(t)

	body := `{"description":""}`
	req := httptest.NewRequest(http.MethodPost, "/workspaces/ws-123/skills/drafts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateDraftInvalidBody(t *testing.T) {
	e := newTestEchoWithDrafts(t)

	req := httptest.NewRequest(http.MethodPost, "/workspaces/ws-123/skills/drafts", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetDraftNotFound(t *testing.T) {
	e := newTestEchoWithDrafts(t)

	req := httptest.NewRequest(http.MethodGet, "/workspaces/ws-123/skills/drafts/nonexistent-id", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreateThenGetDraft(t *testing.T) {
	e := newTestEchoWithDrafts(t)

	body := `{"description":"Create a skill"}`
	req := httptest.NewRequest(http.MethodPost, "/workspaces/ws-123/skills/drafts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		Data struct {
			DraftID string `json:"draft_id"`
		} `json:"data"`
	}
	err := json.Unmarshal(rec.Body.Bytes(), &createResp)
	require.NoError(t, err)
	draftID := createResp.Data.DraftID

	req = httptest.NewRequest(http.MethodGet, "/workspaces/ws-123/skills/drafts/"+draftID, nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		Data struct {
			ID          string `json:"id"`
			WorkspaceID string `json:"workspace_id"`
			Status      string `json:"status"`
		} `json:"data"`
	}
	err = json.Unmarshal(rec.Body.Bytes(), &getResp)
	require.NoError(t, err)
	require.Equal(t, draftID, getResp.Data.ID)
	require.Equal(t, "ws-123", getResp.Data.WorkspaceID)
	require.Equal(t, "active", getResp.Data.Status)
}

func TestGetDraftWrongWorkspaceReturnsNotFound(t *testing.T) {
	e := newTestEchoWithDrafts(t)

	body := `{"description":"Create a skill"}`
	req := httptest.NewRequest(http.MethodPost, "/workspaces/ws-123/skills/drafts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		Data struct {
			DraftID string `json:"draft_id"`
		} `json:"data"`
	}
	err := json.Unmarshal(rec.Body.Bytes(), &createResp)
	require.NoError(t, err)

	req = httptest.NewRequest(http.MethodGet, "/workspaces/ws-456/skills/drafts/"+createResp.Data.DraftID, nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestInstallDraftNotFound(t *testing.T) {
	e := newTestEchoWithDrafts(t)

	req := httptest.NewRequest(http.MethodPost, "/workspaces/ws-123/skills/drafts/nonexistent/install", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestInstallDraftEmptyContent(t *testing.T) {
	e := newTestEchoWithDrafts(t)

	body := `{"description":"Create a skill"}`
	req := httptest.NewRequest(http.MethodPost, "/workspaces/ws-123/skills/drafts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		Data struct {
			DraftID string `json:"draft_id"`
		} `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	draftID := createResp.Data.DraftID

	req = httptest.NewRequest(http.MethodPost, "/workspaces/ws-123/skills/drafts/"+draftID+"/install", nil)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestChatDraftMissingMessage(t *testing.T) {
	e := newTestEchoWithDrafts(t)

	body := `{"description":"Create a skill"}`
	req := httptest.NewRequest(http.MethodPost, "/workspaces/ws-123/skills/drafts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		Data struct {
			DraftID string `json:"draft_id"`
		} `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	draftID := createResp.Data.DraftID

	chatBody := `{"message":""}`
	req = httptest.NewRequest(http.MethodPost, "/workspaces/ws-123/skills/drafts/"+draftID+"/chat", strings.NewReader(chatBody))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestChatDraftNotFound(t *testing.T) {
	e := newTestEchoWithDrafts(t)

	chatBody := `{"message":"hello"}`
	req := httptest.NewRequest(http.MethodPost, "/workspaces/ws-123/skills/drafts/nonexistent/chat", strings.NewReader(chatBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestListDraftsEmpty(t *testing.T) {
	e := newTestEchoWithDrafts(t)

	req := httptest.NewRequest(http.MethodGet, "/workspaces/ws-123/skills/drafts", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Data []interface{} `json:"data"`
	}
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	require.NotNil(t, resp.Data)
	require.Len(t, resp.Data, 0)
}
