package apiv1

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/skills"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

const (
	errMsgSessionInvalid = "Invalid or expired OAuth session. This usually means the callback URL doesn't match the server that created the session (e.g. session created on localhost but callback went to production)."
	errMsgProviderConfig = "This OAuth provider is not configured on the server. Check that the provider credentials are set in the gateway config."
	errMsgNoAuthCode     = "The OAuth provider did not return an authorization code. This can happen if you denied the request or the provider encountered an error."
	errMsgSaveConnection = "OAuth succeeded but we couldn't save the connection to the database. Please try again."
)

// OAuthGroup handles OAuth endpoints for workspace integrations.
type OAuthGroup struct {
	store      *oauth.Store
	registry   *oauth.Registry
	backend    repository.BackendRepository
	storage    *clients.StorageClient
	adminToken string
}

// NewOAuthGroup creates and registers OAuth routes.
// adminToken is the cluster-admin bearer token so that trusted callers (e.g. the
// dashboard backend) can create sessions without a per-workspace member token.
func NewOAuthGroup(g *echo.Group, store *oauth.Store, registry *oauth.Registry, backend repository.BackendRepository, storage *clients.StorageClient, adminToken string) *OAuthGroup {
	og := &OAuthGroup{
		store:      store,
		registry:   registry,
		backend:    backend,
		storage:    storage,
		adminToken: adminToken,
	}

	g.POST("/sessions", og.CreateSession)
	g.GET("/sessions/:id", og.GetSession)
	g.GET("/callback", og.Callback)

	return og
}

type CreateSessionRequest struct {
	IntegrationType string `json:"integration_type"`
	WorkspaceId     string `json:"workspace_id,omitempty"`
	ReturnTo        string `json:"return_to,omitempty"`
}

type CreateSessionResponse struct {
	SessionID    string `json:"session_id"`
	AuthorizeURL string `json:"authorize_url"`
}

type userFacingOAuthErrorClassifier interface {
	UserFacingError(integrationType, raw string) string
}

// CreateSession creates a new OAuth session and returns the authorization URL.
//
// Auth: accepts either a workspace-scoped member token (workspace derived from
// the token) or the cluster admin / org token (workspace_id must be in the
// request body).
func (og *OAuthGroup) CreateSession(c echo.Context) error {
	authHeader := c.Request().Header.Get("Authorization")
	if authHeader == "" {
		return ErrorResponse(c, http.StatusUnauthorized, "authorization required")
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	if token == authHeader {
		return ErrorResponse(c, http.StatusUnauthorized, "bearer token required")
	}

	var req CreateSessionRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}

	if req.IntegrationType == "" {
		return ErrorResponse(c, http.StatusBadRequest, "integration_type required")
	}

	// Resolve the caller identity and target workspace using the shared helper.
	info, err := resolveCallerWorkspace(c.Request().Context(), token, og.adminToken, req.WorkspaceId, og.backend)
	if err != nil {
		msg := err.Error()
		switch msg {
		case "authorization required", "invalid token":
			return ErrorResponse(c, http.StatusUnauthorized, msg)
		case "workspace not found":
			return ErrorResponse(c, http.StatusNotFound, msg)
		case "token does not have access to this workspace":
			return ErrorResponse(c, http.StatusForbidden, msg)
		default:
			return ErrorResponse(c, http.StatusBadRequest, msg)
		}
	}

	workspaceId := info.Workspace.Id
	workspaceExt := info.Workspace.ExternalId

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

	session, err := og.store.Create(provider.Name(), workspaceId, workspaceExt, req.IntegrationType, req.ReturnTo)
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
		Str("workspace", workspaceExt).
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
	errDescription := c.QueryParam("error_description")

	session, err := og.store.GetByState(state)
	if err != nil {
		return renderErrorPage(c, errMsgSessionInvalid)
	}

	// returnToOrError redirects to the app root when ReturnTo is set
	// (same-tab flow), otherwise falls back to the static error page.
	returnToOrError := func(message string) error {
		if session.ReturnTo != "" {
			if u, err := url.Parse(session.ReturnTo); err == nil {
				u.Path = "/"
				u.RawQuery = ""
				u.Fragment = ""
				log.Warn().Str("session_id", session.ID).Str("error", message).Msg("oauth callback error, redirecting to UI")
				return c.Redirect(http.StatusFound, u.String())
			}
		}
		return renderErrorPage(c, message)
	}

	provider, err := og.registry.GetProvider(session.ProviderName)
	if err != nil {
		og.store.Fail(session.ID, err.Error())
		return returnToOrError(errMsgProviderConfig)
	}

	if errParam != "" {
		rawError := fmt.Sprintf("%s: %s", provider.Name(), errParam)
		if errDescription != "" {
			rawError = rawError + ": " + errDescription
		}
		userFacingError := classifyOAuthError(provider, session.IntegrationType, rawError)
		og.store.Fail(session.ID, userFacingError)
		log.Warn().
			Str("session_id", session.ID).
			Str("provider", provider.Name()).
			Str("integration", session.IntegrationType).
			Str("raw_error", rawError).
			Msg("oauth authorization failed")
		return returnToOrError(userFacingError)
	}

	if code == "" {
		og.store.Fail(session.ID, errMsgNoAuthCode)
		return returnToOrError(errMsgNoAuthCode)
	}

	creds, err := provider.Exchange(c.Request().Context(), code, session.IntegrationType)
	if err != nil {
		userFacingError := classifyOAuthError(provider, session.IntegrationType, err.Error())
		og.store.Fail(session.ID, userFacingError)
		log.Error().Err(err).Str("session_id", session.ID).Str("provider", provider.Name()).Msg("oauth token exchange failed")
		return returnToOrError(userFacingError)
	}
	var scopes []string
	if creds != nil && creds.Extra != nil {
		scopes = types.CSVToList(creds.Extra[types.CredentialMetaGrantedScopes])
	}
	creds = oauth.AnnotateCredentials(session.IntegrationType, creds, scopes)

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
		return returnToOrError(errMsgSaveConnection)
	}

	og.store.Complete(session.ID, conn.ExternalId)

	// Auto-provision a managed source write-back skill for OAuth write-capable sources.
	if og.storage != nil && types.SupportsSourceWrite(types.IntegrationName(session.IntegrationType)) {
		if err := skills.UpsertManagedSourceSkill(c.Request().Context(), og.storage, session.WorkspaceExt, session.IntegrationType); err != nil {
			log.Warn().
				Err(err).
				Str("workspace", session.WorkspaceExt).
				Str("integration", session.IntegrationType).
				Msg("failed to provision managed source skill")
		}
	}

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

func classifyOAuthError(provider oauth.Provider, integrationType, raw string) string {
	if raw == "" {
		return raw
	}
	classifier, ok := provider.(userFacingOAuthErrorClassifier)
	if !ok {
		return raw
	}
	userFacing := strings.TrimSpace(classifier.UserFacingError(integrationType, raw))
	if userFacing == "" {
		return raw
	}
	return userFacing
}
