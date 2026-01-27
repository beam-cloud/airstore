package apiv1

import (
	"net/http"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
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
	MemberId  string `json:"member_id"`
	Name      string `json:"name"`
	ExpiresIn int    `json:"expires_in"` // Seconds
}

type TokenCreatedResponse struct {
	Token string      `json:"token"`
	Info  interface{} `json:"info"`
}

func (tg *TokensGroup) Create(c echo.Context) error {
	workspaceId := c.Param("workspace_id")

	var req CreateTokenRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}
	if req.MemberId == "" {
		return ErrorResponse(c, http.StatusBadRequest, "member_id required")
	}
	if req.Name == "" {
		req.Name = "API Token"
	}

	ws, err := tg.backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	member, err := tg.backend.GetMember(c.Request().Context(), req.MemberId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if member == nil {
		return ErrorResponse(c, http.StatusNotFound, "member not found")
	}
	if member.WorkspaceId != ws.Id {
		return ErrorResponse(c, http.StatusBadRequest, "member not in workspace")
	}

	var expiresAt *time.Time
	if req.ExpiresIn > 0 {
		t := time.Now().Add(time.Duration(req.ExpiresIn) * time.Second)
		expiresAt = &t
	}

	token, raw, err := tg.backend.CreateToken(c.Request().Context(), ws.Id, member.Id, req.Name, expiresAt)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, Response{
		Success: true,
		Data:    TokenCreatedResponse{Token: raw, Info: token},
	})
}

func (tg *TokensGroup) List(c echo.Context) error {
	workspaceId := c.Param("workspace_id")

	ws, err := tg.backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	tokens, err := tg.backend.ListTokens(c.Request().Context(), ws.Id)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{Success: true, Data: tokens})
}

func (tg *TokensGroup) Revoke(c echo.Context) error {
	tokenId := c.Param("token_id")

	if err := tg.backend.RevokeToken(c.Request().Context(), tokenId); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{Success: true})
}
