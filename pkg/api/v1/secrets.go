package apiv1

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// SecretsGroup manages per-workspace secrets (BYOK keys, etc.).
type SecretsGroup struct {
	g       *echo.Group
	backend repository.BackendRepository
}

// NewSecretsGroup registers routes for managing workspace secrets.
func NewSecretsGroup(g *echo.Group, backend repository.BackendRepository) *SecretsGroup {
	sg := &SecretsGroup{g: g, backend: backend}
	sg.g.PUT("/anthropic-key", sg.SetAnthropicKey)
	sg.g.DELETE("/anthropic-key", sg.DeleteAnthropicKey)
	sg.g.GET("/anthropic-key", sg.GetAnthropicKeyStatus)
	return sg
}

type SetAnthropicKeyRequest struct {
	APIKey string `json:"api_key"`
}

type AnthropicKeyStatusResponse struct {
	IsSet bool `json:"is_set"`
}

// SetAnthropicKey stores (or replaces) the workspace's BYOK Anthropic API key.
func (sg *SecretsGroup) SetAnthropicKey(c echo.Context) error {
	ctx := c.Request().Context()

	externalId := c.Param("workspace_id")
	ws, err := sg.backend.GetWorkspaceByExternalId(ctx, externalId)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	var req SetAnthropicKeyRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	apiKey := strings.TrimSpace(req.APIKey)
	if apiKey == "" {
		return ErrorResponse(c, http.StatusBadRequest, "api_key is required")
	}

	encoded, err := json.Marshal(apiKey)
	if err != nil {
		log.Error().Err(err).Uint("workspace_id", ws.Id).Msg("failed to encode anthropic key")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to store key")
	}

	if err := sg.backend.SetWorkspaceSecret(ctx, ws.Id, orchestration.WorkspaceSecretAnthropicKey, encoded); err != nil {
		log.Error().Err(err).Uint("workspace_id", ws.Id).Msg("failed to store anthropic key")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to store key")
	}

	return SuccessResponse(c, nil)
}

// DeleteAnthropicKey removes the workspace's BYOK Anthropic API key.
func (sg *SecretsGroup) DeleteAnthropicKey(c echo.Context) error {
	ctx := c.Request().Context()

	externalId := c.Param("workspace_id")
	ws, err := sg.backend.GetWorkspaceByExternalId(ctx, externalId)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	if err := sg.backend.DeleteWorkspaceSecret(ctx, ws.Id, orchestration.WorkspaceSecretAnthropicKey); err != nil {
		log.Error().Err(err).Uint("workspace_id", ws.Id).Msg("failed to delete anthropic key")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to delete key")
	}

	return SuccessResponse(c, nil)
}

// GetAnthropicKeyStatus returns whether a BYOK Anthropic API key is configured.
// The key value itself is never returned.
func (sg *SecretsGroup) GetAnthropicKeyStatus(c echo.Context) error {
	ctx := c.Request().Context()

	externalId := c.Param("workspace_id")
	ws, err := sg.backend.GetWorkspaceByExternalId(ctx, externalId)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	_, err = sg.backend.GetWorkspaceSecret(ctx, ws.Id, orchestration.WorkspaceSecretAnthropicKey)
	isSet := err == nil

	return SuccessResponse(c, AnthropicKeyStatusResponse{IsSet: isSet})
}
