package apiv1

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// OAuthGroup handles OAuth endpoints for workspace integrations
type OAuthGroup struct {
	store    *oauth.Store
	registry *oauth.Registry
	backend  repository.BackendRepository
}

// NewOAuthGroup creates and registers OAuth routes
func NewOAuthGroup(g *echo.Group, store *oauth.Store, registry *oauth.Registry, backend repository.BackendRepository) *OAuthGroup {
	og := &OAuthGroup{
		store:    store,
		registry: registry,
		backend:  backend,
	}

	g.POST("/sessions", og.CreateSession)
	g.GET("/sessions/:id", og.GetSession)

	// Provider-specific callbacks
	g.GET("/google/callback", og.ProviderCallback("google"))
	g.GET("/github/callback", og.ProviderCallback("github"))
	g.GET("/notion/callback", og.ProviderCallback("notion"))
	g.GET("/slack/callback", og.ProviderCallback("slack"))
	g.GET("/linear/callback", og.ProviderCallback("linear"))

	return og
}

// CreateSessionRequest is the request body for POST /oauth/sessions
type CreateSessionRequest struct {
	IntegrationType string `json:"integration_type"`
	ReturnTo        string `json:"return_to,omitempty"` // Optional URL to redirect after callback (relative path or full URL)
}

// CreateSessionResponse is the response for POST /oauth/sessions
type CreateSessionResponse struct {
	SessionID    string `json:"session_id"`
	AuthorizeURL string `json:"authorize_url"`
}

// CreateSession creates a new OAuth session and returns the authorization URL
// Requires workspace token authentication via Authorization header
func (og *OAuthGroup) CreateSession(c echo.Context) error {
	// Get workspace from Authorization header (workspace token)
	auth := c.Request().Header.Get("Authorization")
	if auth == "" {
		return ErrorResponse(c, http.StatusUnauthorized, "authorization required")
	}

	token := strings.TrimPrefix(auth, "Bearer ")
	if token == auth {
		return ErrorResponse(c, http.StatusUnauthorized, "bearer token required")
	}

	// Validate workspace token
	result, err := og.backend.ValidateToken(c.Request().Context(), token)
	if err != nil || result == nil {
		return ErrorResponse(c, http.StatusUnauthorized, "invalid token")
	}

	var req CreateSessionRequest
	if err := c.Bind(&req); err != nil {
		log.Debug().Err(err).Msg("oauth session: failed to bind request")
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}

	log.Debug().
		Str("integration_type", req.IntegrationType).
		Str("return_to", req.ReturnTo).
		Msg("oauth session request")

	if req.IntegrationType == "" {
		return ErrorResponse(c, http.StatusBadRequest, "integration_type required")
	}

	// Get provider for this integration type
	provider, err := og.registry.GetProviderForIntegration(req.IntegrationType)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, fmt.Sprintf("integration %s does not use OAuth", req.IntegrationType))
	}

	// Check if provider is configured
	if !provider.IsConfigured() {
		return ErrorResponse(c, http.StatusServiceUnavailable, fmt.Sprintf("%s OAuth not configured", provider.Name()))
	}

	// Validate return_to if provided (can be relative path or full URL)
	if req.ReturnTo != "" {
		// Must start with / (relative) or http:// or https:// (absolute)
		if !strings.HasPrefix(req.ReturnTo, "/") &&
			!strings.HasPrefix(req.ReturnTo, "http://") &&
			!strings.HasPrefix(req.ReturnTo, "https://") {
			return ErrorResponse(c, http.StatusBadRequest, "return_to must be a relative path or full URL")
		}
	}

	// Create session
	session := og.store.Create(result.WorkspaceId, result.WorkspaceExt, req.IntegrationType, req.ReturnTo)

	// Generate authorize URL
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

// GetSessionResponse is the response for GET /oauth/sessions/:id
type GetSessionResponse struct {
	Status       string `json:"status"` // pending, complete, error
	Error        string `json:"error,omitempty"`
	ConnectionID string `json:"connection_id,omitempty"`
}

// GetSession returns the status of an OAuth session
func (og *OAuthGroup) GetSession(c echo.Context) error {
	id := c.Param("id")

	session := og.store.Get(id)
	if session == nil {
		return ErrorResponse(c, http.StatusNotFound, "session not found")
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

// ProviderCallback returns a handler for OAuth callbacks from a specific provider
func (og *OAuthGroup) ProviderCallback(providerName string) echo.HandlerFunc {
	return func(c echo.Context) error {
		state := c.QueryParam("state")
		code := c.QueryParam("code")
		errParam := c.QueryParam("error")

		// Find session by state
		session := og.store.GetByState(state)
		if session == nil {
			return og.errorPage(c, "Invalid or expired OAuth session")
		}

		// Get the provider for this integration
		provider, err := og.registry.GetProviderForIntegration(session.IntegrationType)
		if err != nil {
			og.store.Fail(session.ID, err.Error())
			return og.errorPage(c, "Provider not configured")
		}

		// Verify the callback is from the expected provider
		if provider.Name() != providerName {
			og.store.Fail(session.ID, "callback provider mismatch")
			return og.errorPage(c, "Provider mismatch")
		}

		// Check for error from provider
		if errParam != "" {
			og.store.Fail(session.ID, providerName+" error: "+errParam)
			return og.errorPage(c, fmt.Sprintf("%s authorization failed: %s", providerName, errParam))
		}

		if code == "" {
			og.store.Fail(session.ID, "missing authorization code")
			return og.errorPage(c, "Missing authorization code")
		}

		// Exchange code for tokens
		creds, err := provider.Exchange(c.Request().Context(), code, session.IntegrationType)
		if err != nil {
			og.store.Fail(session.ID, err.Error())
			log.Error().Err(err).Str("session_id", session.ID).Str("provider", providerName).Msg("oauth token exchange failed")
			return og.errorPage(c, "Token exchange failed")
		}

		// Save connection (workspace-shared, member_id = nil)
		conn, err := og.backend.SaveConnection(
			c.Request().Context(),
			session.WorkspaceID,
			nil, // shared connection
			session.IntegrationType,
			creds,
			"", // no explicit scope
		)
		if err != nil {
			og.store.Fail(session.ID, err.Error())
			log.Error().Err(err).Str("session_id", session.ID).Msg("failed to save connection")
			return og.errorPage(c, "Failed to save connection")
		}

		// Mark session complete
		og.store.Complete(session.ID, conn.ExternalId)

		log.Info().
			Str("session_id", session.ID).
			Str("workspace", session.WorkspaceExt).
			Str("integration", session.IntegrationType).
			Str("provider", providerName).
			Str("connection_id", conn.ExternalId).
			Msg("oauth connection saved")

		// Redirect if return_to was specified, otherwise show success page
		if session.ReturnTo != "" {
			return c.Redirect(http.StatusFound, session.ReturnTo)
		}

		return og.successPage(c, session.IntegrationType)
	}
}

func (og *OAuthGroup) errorPage(c echo.Context, message string) error {
	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
    <title>Connection Failed</title>
    <style>
        body { font-family: system-ui, sans-serif; max-width: 500px; margin: 100px auto; text-align: center; }
        h1 { color: #dc2626; }
        p { color: #666; }
    </style>
</head>
<body>
    <h1>Connection Failed</h1>
    <p>%s</p>
    <p>You can close this window and try again.</p>
</body>
</html>`, message)
	return c.HTML(http.StatusBadRequest, html)
}

func (og *OAuthGroup) successPage(c echo.Context, integrationType string) error {
	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
    <title>Connection Successful</title>
    <style>
        body { font-family: system-ui, sans-serif; max-width: 500px; margin: 100px auto; text-align: center; }
        h1 { color: #16a34a; }
        p { color: #666; }
        code { background: #f3f4f6; padding: 2px 6px; border-radius: 4px; }
    </style>
</head>
<body>
    <h1>Connected!</h1>
    <p><strong>%s</strong> has been connected to your workspace.</p>
    <p>You can close this window and return to the CLI.</p>
</body>
</html>`, integrationType)
	return c.HTML(http.StatusOK, html)
}
