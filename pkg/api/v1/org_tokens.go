package apiv1

import (
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

// OrgTokensGroup handles tenant-scoped organization tokens (cluster_admin only).
type OrgTokensGroup struct {
	backend repository.BackendRepository
}

// NewOrgTokensGroup registers routes for managing organization tokens.
func NewOrgTokensGroup(g *echo.Group, backend repository.BackendRepository) *OrgTokensGroup {
	ot := &OrgTokensGroup{backend: backend}
	g.POST("", ot.Create)
	g.GET("", ot.List)
	g.DELETE("/:token_id", ot.Revoke)
	return ot
}

// CreateOrgTokenRequest is the request body for creating an organization token.
type CreateOrgTokenRequest struct {
	Name      string `json:"name"`
	TenantId  string `json:"tenant_id"`
	ExpiresIn int    `json:"expires_in"` // seconds; 0 = no expiration
}

// OrgTokenInfo is the public-facing token metadata returned in API responses.
// It intentionally omits sensitive fields like token_hash and internal IDs.
type OrgTokenInfo struct {
	ExternalId string  `json:"external_id"`
	Name       string  `json:"name"`
	TokenType  string  `json:"token_type"`
	TenantId   *string `json:"tenant_id,omitempty"`
	ExpiresAt  *string `json:"expires_at,omitempty"`
	CreatedAt  string  `json:"created_at"`
	LastUsedAt *string `json:"last_used_at,omitempty"`
}

// OrgTokenResponse is the response body for token creation (includes raw token value).
type OrgTokenResponse struct {
	Token string       `json:"token"`
	Info  OrgTokenInfo `json:"info"`
}

// Create creates a new tenant-scoped organization token.
func (ot *OrgTokensGroup) Create(c echo.Context) error {
	var req CreateOrgTokenRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if req.Name == "" {
		return ErrorResponse(c, http.StatusBadRequest, "name is required")
	}
	if req.TenantId == "" {
		return ErrorResponse(c, http.StatusBadRequest, "tenant_id is required")
	}

	if req.ExpiresIn < 0 {
		return ErrorResponse(c, http.StatusBadRequest, "expires_in must not be negative")
	}

	var expiresAt *time.Time
	if req.ExpiresIn > 0 {
		t := time.Now().Add(time.Duration(req.ExpiresIn) * time.Second)
		expiresAt = &t
	}

	token, raw, err := ot.backend.CreateOrgToken(c.Request().Context(), req.Name, req.TenantId, expiresAt)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to create token")
	}

	return c.JSON(http.StatusCreated, Response{
		Success: true,
		Data:    OrgTokenResponse{Token: raw, Info: tokenToOrgInfo(token)},
	})
}

// List returns all organization tokens, optionally filtered by tenant_id query param.
func (ot *OrgTokensGroup) List(c echo.Context) error {
	tenantId := c.QueryParam("tenant_id")

	tokens, err := ot.backend.ListOrgTokens(c.Request().Context(), tenantId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to list tokens")
	}

	// Always return an array, never null
	result := make([]OrgTokenInfo, 0, len(tokens))
	for i := range tokens {
		result = append(result, tokenToOrgInfo(&tokens[i]))
	}

	return SuccessResponse(c, result)
}

// Revoke deletes an organization token by its external ID.
func (ot *OrgTokensGroup) Revoke(c echo.Context) error {
	tokenId := c.Param("token_id")
	if tokenId == "" {
		return ErrorResponse(c, http.StatusBadRequest, "token_id is required")
	}

	if err := ot.backend.RevokeOrgToken(c.Request().Context(), tokenId); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrorResponse(c, http.StatusNotFound, "token not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, "failed to revoke token")
	}
	return SuccessResponse(c, nil)
}

// tokenToOrgInfo converts an internal Token to a public-facing OrgTokenInfo.
func tokenToOrgInfo(t *types.Token) OrgTokenInfo {
	info := OrgTokenInfo{
		ExternalId: t.ExternalId,
		Name:       t.Name,
		TokenType:  string(t.TokenType),
		TenantId:   t.TenantId,
		CreatedAt:  t.CreatedAt.Format(time.RFC3339),
	}
	if t.ExpiresAt != nil {
		s := t.ExpiresAt.Format(time.RFC3339)
		info.ExpiresAt = &s
	}
	if t.LastUsedAt != nil {
		s := t.LastUsedAt.Format(time.RFC3339)
		info.LastUsedAt = &s
	}
	return info
}
