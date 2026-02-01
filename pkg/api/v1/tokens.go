package apiv1

import (
	"net/http"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

// TokensGroup handles workspace-scoped token management.
type TokensGroup struct {
	backend repository.BackendRepository
}

func NewTokensGroup(g *echo.Group, backend repository.BackendRepository) *TokensGroup {
	tg := &TokensGroup{backend: backend}
	g.POST("", tg.Create)
	g.GET("", tg.List)
	g.DELETE("/:token_id", tg.Revoke)
	return tg
}

type CreateTokenRequest struct {
	MemberId  string `json:"member_id"`
	Email     string `json:"email"`
	Name      string `json:"name"`
	ExpiresIn int    `json:"expires_in"`
}

type TokenResponse struct {
	Token    string      `json:"token"`
	Info     interface{} `json:"info"`
	MemberId string      `json:"member_id,omitempty"`
}

func (tg *TokensGroup) Create(c echo.Context) error {
	ctx := c.Request().Context()
	if !auth.IsAdmin(ctx) {
		return ErrorResponse(c, http.StatusForbidden, "admin access required")
	}

	var req CreateTokenRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}
	if req.MemberId == "" && req.Email == "" {
		return ErrorResponse(c, http.StatusBadRequest, "member_id or email required")
	}
	if req.Name == "" {
		req.Name = "API Token"
	}

	ws, err := tg.backend.GetWorkspaceByExternalId(ctx, c.Param("workspace_id"))
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	var member *types.WorkspaceMember
	var autoCreatedId string

	if req.MemberId != "" {
		member, err = tg.backend.GetMember(ctx, req.MemberId)
		if err != nil || member == nil {
			return ErrorResponse(c, http.StatusNotFound, "member not found")
		}
		if member.WorkspaceId != ws.Id {
			return ErrorResponse(c, http.StatusBadRequest, "member not in workspace")
		}
	} else {
		member, err = tg.backend.CreateMember(ctx, ws.Id, req.Email, req.Email, types.RoleMember)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, "failed to create member")
		}
		autoCreatedId = member.ExternalId
	}

	var expiresAt *time.Time
	if req.ExpiresIn > 0 {
		t := time.Now().Add(time.Duration(req.ExpiresIn) * time.Second)
		expiresAt = &t
	}

	token, raw, err := tg.backend.CreateToken(ctx, ws.Id, member.Id, req.Name, expiresAt, types.TokenTypeWorkspaceMember)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, Response{
		Success: true,
		Data:    TokenResponse{Token: raw, Info: token, MemberId: autoCreatedId},
	})
}

func (tg *TokensGroup) List(c echo.Context) error {
	ctx := c.Request().Context()
	if !auth.IsAdmin(ctx) {
		return ErrorResponse(c, http.StatusForbidden, "admin access required")
	}

	ws, err := tg.backend.GetWorkspaceByExternalId(ctx, c.Param("workspace_id"))
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	tokens, err := tg.backend.ListTokens(ctx, ws.Id)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{Success: true, Data: tokens})
}

func (tg *TokensGroup) Revoke(c echo.Context) error {
	if !auth.IsAdmin(c.Request().Context()) {
		return ErrorResponse(c, http.StatusForbidden, "admin access required")
	}
	if err := tg.backend.RevokeToken(c.Request().Context(), c.Param("token_id")); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, Response{Success: true})
}

// WorkerTokensGroup handles cluster-level worker tokens (admin only).
type WorkerTokensGroup struct {
	backend repository.BackendRepository
}

func NewWorkerTokensGroup(g *echo.Group, backend repository.BackendRepository) *WorkerTokensGroup {
	wt := &WorkerTokensGroup{backend: backend}
	g.GET("", wt.List)
	g.POST("", wt.Create)
	g.DELETE("/:token_id", wt.Revoke)
	return wt
}

type CreateWorkerTokenRequest struct {
	Name      string  `json:"name"`
	PoolName  *string `json:"pool_name,omitempty"`
	ExpiresIn int     `json:"expires_in"`
}

func (wt *WorkerTokensGroup) List(c echo.Context) error {
	tokens, err := wt.backend.ListWorkerTokens(c.Request().Context())
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, Response{Success: true, Data: tokens})
}

func (wt *WorkerTokensGroup) Create(c echo.Context) error {
	var req CreateWorkerTokenRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}
	if req.Name == "" {
		return ErrorResponse(c, http.StatusBadRequest, "name required")
	}

	var expiresAt *time.Time
	if req.ExpiresIn > 0 {
		t := time.Now().Add(time.Duration(req.ExpiresIn) * time.Second)
		expiresAt = &t
	}

	token, raw, err := wt.backend.CreateWorkerToken(c.Request().Context(), req.Name, req.PoolName, expiresAt)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, Response{
		Success: true,
		Data:    TokenResponse{Token: raw, Info: token},
	})
}

func (wt *WorkerTokensGroup) Revoke(c echo.Context) error {
	if err := wt.backend.RevokeToken(c.Request().Context(), c.Param("token_id")); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, Response{Success: true})
}
