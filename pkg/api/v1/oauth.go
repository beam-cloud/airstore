package apiv1

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

const (
	errMsgSessionInvalid  = "Invalid or expired OAuth session"
	errMsgProviderConfig  = "Provider not configured"
	errMsgNoAuthCode      = "Missing authorization code"
	errMsgTokenExchange   = "Token exchange failed"
	errMsgSaveConnection  = "Failed to save connection"
)

// OAuthGroup handles OAuth endpoints for workspace integrations.
type OAuthGroup struct {
	store    *oauth.Store
	registry *oauth.Registry
	backend  repository.BackendRepository
}

// NewOAuthGroup creates and registers OAuth routes.
func NewOAuthGroup(g *echo.Group, store *oauth.Store, registry *oauth.Registry, backend repository.BackendRepository) *OAuthGroup {
	og := &OAuthGroup{
		store:    store,
		registry: registry,
		backend:  backend,
	}

	g.POST("/sessions", og.CreateSession)
	g.GET("/sessions/:id", og.GetSession)
	g.GET("/callback", og.Callback)

	return og
}

type CreateSessionRequest struct {
	IntegrationType string `json:"integration_type"`
	ReturnTo        string `json:"return_to,omitempty"`
}

type CreateSessionResponse struct {
	SessionID    string `json:"session_id"`
	AuthorizeURL string `json:"authorize_url"`
}

// CreateSession creates a new OAuth session and returns the authorization URL.
func (og *OAuthGroup) CreateSession(c echo.Context) error {
	auth := c.Request().Header.Get("Authorization")
	if auth == "" {
		return ErrorResponse(c, http.StatusUnauthorized, "authorization required")
	}

	token := strings.TrimPrefix(auth, "Bearer ")
	if token == auth {
		return ErrorResponse(c, http.StatusUnauthorized, "bearer token required")
	}

	result, err := og.backend.ValidateToken(c.Request().Context(), token)
	if err != nil || result == nil {
		return ErrorResponse(c, http.StatusUnauthorized, "invalid token")
	}

	var req CreateSessionRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}

	if req.IntegrationType == "" {
		return ErrorResponse(c, http.StatusBadRequest, "integration_type required")
	}

	provider, err := og.registry.GetProviderForIntegration(req.IntegrationType)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, fmt.Sprintf("integration %s does not use OAuth", req.IntegrationType))
	}

	if req.ReturnTo != "" {
		if !strings.HasPrefix(req.ReturnTo, "/") &&
			!strings.HasPrefix(req.ReturnTo, "http://") &&
			!strings.HasPrefix(req.ReturnTo, "https://") {
			return ErrorResponse(c, http.StatusBadRequest, "return_to must be a relative path or full URL")
		}
	}

	session, err := og.store.Create(provider.Name(), result.WorkspaceId, result.WorkspaceExt, req.IntegrationType, req.ReturnTo)
	if err != nil {
		log.Error().Err(err).Msg("failed to create oauth session")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to create session")
	}

	authorizeURL, err := provider.AuthorizeURL(session.State, req.IntegrationType)
	if err != nil {
		og.store.Delete(session.ID)
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	log.Info().
		Str("session_id", session.ID).
		Str("workspace", result.WorkspaceExt).
		Str("integration", req.IntegrationType).
		Str("provider", provider.Name()).
		Msg("oauth session created")

	return c.JSON(http.StatusCreated, Response{
		Success: true,
		Data: CreateSessionResponse{
			SessionID:    session.ID,
			AuthorizeURL: authorizeURL,
		},
	})
}

type GetSessionResponse struct {
	Status       string `json:"status"`
	Error        string `json:"error,omitempty"`
	ConnectionID string `json:"connection_id,omitempty"`
}

// GetSession returns the status of an OAuth session.
func (og *OAuthGroup) GetSession(c echo.Context) error {
	id := c.Param("id")

	session, err := og.store.Get(id)
	if err != nil {
		if errors.Is(err, oauth.ErrSessionNotFound) || errors.Is(err, oauth.ErrSessionExpired) {
			return ErrorResponse(c, http.StatusNotFound, "session not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, "failed to get session")
	}

	return c.JSON(http.StatusOK, Response{
		Success: true,
		Data: GetSessionResponse{
			Status:       string(session.Status),
			Error:        session.Error,
			ConnectionID: session.ConnectionID,
		},
	})
}

// Callback handles OAuth callbacks from all providers.
// Provider is determined from session state, not URL path.
func (og *OAuthGroup) Callback(c echo.Context) error {
	state := c.QueryParam("state")
	code := c.QueryParam("code")
	errParam := c.QueryParam("error")

	session, err := og.store.GetByState(state)
	if err != nil {
		return renderErrorPage(c, errMsgSessionInvalid)
	}

	provider, err := og.registry.GetProvider(session.ProviderName)
	if err != nil {
		og.store.Fail(session.ID, err.Error())
		return renderErrorPage(c, errMsgProviderConfig)
	}

	if errParam != "" {
		og.store.Fail(session.ID, provider.Name()+": "+errParam)
		return renderErrorPage(c, fmt.Sprintf("%s authorization failed: %s", provider.Name(), errParam))
	}

	if code == "" {
		og.store.Fail(session.ID, errMsgNoAuthCode)
		return renderErrorPage(c, errMsgNoAuthCode)
	}

	creds, err := provider.Exchange(c.Request().Context(), code, session.IntegrationType)
	if err != nil {
		og.store.Fail(session.ID, err.Error())
		log.Error().Err(err).Str("session_id", session.ID).Str("provider", provider.Name()).Msg("oauth token exchange failed")
		return renderErrorPage(c, errMsgTokenExchange)
	}

	conn, err := og.backend.SaveConnection(
		c.Request().Context(),
		session.WorkspaceID,
		nil,
		session.IntegrationType,
		creds,
		"",
	)
	if err != nil {
		og.store.Fail(session.ID, err.Error())
		log.Error().Err(err).Str("session_id", session.ID).Msg("failed to save connection")
		return renderErrorPage(c, errMsgSaveConnection)
	}

	og.store.Complete(session.ID, conn.ExternalId)

	log.Info().
		Str("session_id", session.ID).
		Str("workspace", session.WorkspaceExt).
		Str("integration", session.IntegrationType).
		Str("provider", provider.Name()).
		Str("connection_id", conn.ExternalId).
		Msg("oauth connection saved")

	if session.ReturnTo != "" {
		return c.Redirect(http.StatusFound, session.ReturnTo)
	}

	return renderSuccessPage(c, session.IntegrationType)
}
