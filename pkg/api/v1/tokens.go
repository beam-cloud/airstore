package apiv1

import (
	"net/http"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type TokensGroup struct {
	g       *echo.Group
	backend repository.BackendRepository
}

func NewTokensGroup(g *echo.Group, backend repository.BackendRepository) *TokensGroup {
	tg := &TokensGroup{g: g, backend: backend}
	tg.g.POST("", tg.Create)
	tg.g.GET("", tg.List)
	tg.g.DELETE("/:token_id", tg.Revoke)
	return tg
}

type CreateTokenRequest struct {
	MemberId  string          `json:"member_id"`  // Optional if email is provided
	Email     string          `json:"email"`      // For auto-creating member
	Name      string          `json:"name"`
	ExpiresIn int             `json:"expires_in"` // Seconds
	TokenType types.TokenType `json:"token_type"` // Optional, defaults to workspace_member
}

type TokenCreatedResponse struct {
	Token    string      `json:"token"`
	Info     interface{} `json:"info"`
	MemberId string      `json:"member_id,omitempty"` // Returned when auto-created
}

func (tg *TokensGroup) Create(c echo.Context) error {
	ctx := c.Request().Context()

	// Require admin role to create tokens
	if !auth.IsAdmin(ctx) {
		return ErrorResponse(c, http.StatusForbidden, "admin access required")
	}

	workspaceId := c.Param("workspace_id")

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
	if req.TokenType == "" {
		req.TokenType = types.TokenTypeWorkspaceMember
	}

	ws, err := tg.backend.GetWorkspaceByExternalId(ctx, workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	var member *types.WorkspaceMember
	var autoCreatedMemberId string

	if req.MemberId != "" {
		// Use existing member
		member, err = tg.backend.GetMember(ctx, req.MemberId)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, err.Error())
		}
		if member == nil {
			return ErrorResponse(c, http.StatusNotFound, "member not found")
		}
		if member.WorkspaceId != ws.Id {
			return ErrorResponse(c, http.StatusBadRequest, "member not in workspace")
		}
	} else {
		// Auto-create member with provided email
		member, err = tg.backend.CreateMember(ctx, ws.Id, req.Email, req.Email, types.RoleMember)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, "failed to create member: "+err.Error())
		}
		autoCreatedMemberId = member.ExternalId
	}

	var expiresAt *time.Time
	if req.ExpiresIn > 0 {
		t := time.Now().Add(time.Duration(req.ExpiresIn) * time.Second)
		expiresAt = &t
	}

	token, raw, err := tg.backend.CreateToken(ctx, ws.Id, member.Id, req.Name, expiresAt, req.TokenType)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, Response{
		Success: true,
		Data:    TokenCreatedResponse{Token: raw, Info: token, MemberId: autoCreatedMemberId},
	})
}

func (tg *TokensGroup) List(c echo.Context) error {
	ctx := c.Request().Context()

	// Require admin role to list tokens
	if !auth.IsAdmin(ctx) {
		return ErrorResponse(c, http.StatusForbidden, "admin access required")
	}

	workspaceId := c.Param("workspace_id")

	ws, err := tg.backend.GetWorkspaceByExternalId(ctx, workspaceId)
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
	ctx := c.Request().Context()

	// Require admin role to revoke tokens
	if !auth.IsAdmin(ctx) {
		return ErrorResponse(c, http.StatusForbidden, "admin access required")
	}

	tokenId := c.Param("token_id")

	if err := tg.backend.RevokeToken(ctx, tokenId); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{Success: true})
}
