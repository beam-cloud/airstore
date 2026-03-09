package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/skills"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type ConnectionsGroup struct {
	g              *echo.Group
	backend        repository.BackendRepository
	sourceRegistry *sources.Registry
	storage        *clients.StorageClient
}

func NewConnectionsGroup(g *echo.Group, backend repository.BackendRepository, sourceRegistry *sources.Registry, storage *clients.StorageClient) *ConnectionsGroup {
	cg := &ConnectionsGroup{g: g, backend: backend, sourceRegistry: sourceRegistry, storage: storage}
	cg.g.POST("", cg.Create)
	cg.g.GET("", cg.List)
	cg.g.DELETE("/:connection_id", cg.Delete)
	return cg
}

type CreateConnectionRequest struct {
	MemberId        string            `json:"member_id,omitempty"` // Empty = shared
	IntegrationType string            `json:"integration_type"`
	Scope           string            `json:"scope,omitempty"`
	AccessToken     string            `json:"access_token,omitempty"`
	RefreshToken    string            `json:"refresh_token,omitempty"`
	APIKey          string            `json:"api_key,omitempty"`
	Extra           map[string]string `json:"extra,omitempty"`
}

func serializeConnection(conn *types.IntegrationConnection, workspaceExternalId string) map[string]any {
	out := map[string]any{
		"id":               conn.Id,
		"external_id":      conn.ExternalId,
		"workspace_id":     workspaceExternalId,
		"integration_type": conn.IntegrationType,
		"scope":            conn.Scope,
		"created_at":       conn.CreatedAt,
		"updated_at":       conn.UpdatedAt,
		"status":           "active",
	}
	if conn.MemberId != nil {
		out["member_id"] = *conn.MemberId
	}

	extra := map[string]any{}
	creds, err := repository.DecryptCredentials(conn)
	if err == nil && creds != nil {
		for key, value := range creds.Extra {
			if key == types.CredentialMetaGrantedScopes || key == types.CredentialMetaCapabilities {
				continue
			}
			extra[key] = value
		}
		scopes := types.CSVToList(creds.Extra[types.CredentialMetaGrantedScopes])
		if len(scopes) > 0 {
			extra["granted_scopes"] = scopes
		}
		capabilities := types.CSVToList(creds.Extra[types.CredentialMetaCapabilities])
		if len(capabilities) > 0 {
			extra["capabilities"] = capabilities
		}
		if types.SupportsSourceWrite(types.IntegrationName(conn.IntegrationType)) {
			extra["write_enabled"] = types.CredentialsSupportSourceWrite(types.IntegrationName(conn.IntegrationType), creds)
		}
	}
	if len(extra) > 0 {
		out["extra"] = extra
	}
	return out
}

func (cg *ConnectionsGroup) Create(c echo.Context) error {
	ctx := c.Request().Context()
	workspaceId := c.Param("workspace_id")

	var req CreateConnectionRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}
	if req.IntegrationType == "" {
		return ErrorResponse(c, http.StatusBadRequest, "integration_type required")
	}
	if req.AccessToken == "" && req.APIKey == "" {
		return ErrorResponse(c, http.StatusBadRequest, "access_token or api_key required")
	}

	ws, err := cg.backend.GetWorkspaceByExternalId(ctx, workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	var memberId *uint
	if req.MemberId != "" {
		// Personal connection - require admin or self
		member, err := cg.backend.GetMember(ctx, req.MemberId)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, err.Error())
		}
		if member == nil {
			return ErrorResponse(c, http.StatusNotFound, "member not found")
		}
		if member.WorkspaceId != ws.Id {
			return ErrorResponse(c, http.StatusBadRequest, "member not in workspace")
		}

		// Check authorization: admin can create for anyone, members only for themselves
		if !auth.IsAdmin(ctx) && member.Id != auth.MemberId(ctx) {
			return ErrorResponse(c, http.StatusForbidden, "cannot create connection for another member")
		}

		memberId = &member.Id
	} else {
		// Shared connection - require admin
		if !auth.IsAdmin(ctx) {
			return ErrorResponse(c, http.StatusForbidden, "admin access required for shared connections")
		}
	}

	creds := &types.IntegrationCredentials{
		AccessToken:  req.AccessToken,
		RefreshToken: req.RefreshToken,
		APIKey:       req.APIKey,
		Extra:        req.Extra,
	}
	creds = oauth.AnnotateCredentials(req.IntegrationType, creds, oauth.ParseScopeString(req.Scope))

	// Validate credentials if the provider supports it
	if cg.sourceRegistry != nil {
		if provider := cg.sourceRegistry.Get(req.IntegrationType); provider != nil {
			if v, ok := provider.(sources.CredentialValidator); ok {
				if err := v.ValidateCredentials(ctx, creds); err != nil {
					return ErrorResponse(c, http.StatusBadRequest, "invalid credentials: "+err.Error())
				}
			}
		}
	}

	conn, err := cg.backend.SaveConnection(ctx, ws.Id, memberId, req.IntegrationType, creds, req.Scope)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if cg.storage != nil && types.SupportsSourceWrite(types.IntegrationName(req.IntegrationType)) {
		if err := skills.UpsertManagedSourceSkill(ctx, cg.storage, ws.ExternalId, req.IntegrationType); err != nil {
			log.Warn().Err(err).Str("workspace", ws.ExternalId).Str("integration", req.IntegrationType).Msg("failed to provision managed source skill")
		}
	}

	return c.JSON(http.StatusCreated, Response{Success: true, Data: serializeConnection(conn, ws.ExternalId)})
}

func (cg *ConnectionsGroup) List(c echo.Context) error {
	workspaceId := c.Param("workspace_id")

	ws, err := cg.backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	conns, err := cg.backend.ListConnections(c.Request().Context(), ws.Id)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	result := make([]map[string]any, 0, len(conns))
	for i := range conns {
		result = append(result, serializeConnection(&conns[i], ws.ExternalId))
	}
	return c.JSON(http.StatusOK, Response{Success: true, Data: result})
}

func (cg *ConnectionsGroup) Delete(c echo.Context) error {
	ctx := c.Request().Context()
	connId := c.Param("connection_id")

	// Fetch connection to check permissions
	conn, err := cg.backend.GetConnectionByExternalId(ctx, connId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if conn == nil {
		return ErrorResponse(c, http.StatusNotFound, "connection not found")
	}

	// Check authorization: any workspace member can disconnect shared connections;
	// personal connections require admin or the owning member.
	if !conn.IsShared() {
		if !auth.IsAdmin(ctx) && *conn.MemberId != auth.MemberId(ctx) {
			return ErrorResponse(c, http.StatusForbidden, "cannot delete another member's connection")
		}
	}

	if err := cg.backend.DeleteConnection(ctx, connId); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	if cg.storage != nil && types.SupportsSourceWrite(types.IntegrationName(conn.IntegrationType)) {
		remaining, err := cg.backend.ListConnections(ctx, conn.WorkspaceId)
		if err != nil {
			log.Warn().Err(err).Uint("workspace_id", conn.WorkspaceId).Str("integration", conn.IntegrationType).Msg("failed to check remaining connections")
		} else {
			hasRemaining := false
			for _, c := range remaining {
				if c.IntegrationType == conn.IntegrationType {
					hasRemaining = true
					break
				}
			}
			if !hasRemaining {
				ws, wsErr := cg.backend.GetWorkspace(ctx, conn.WorkspaceId)
				if wsErr != nil {
					log.Warn().Err(wsErr).Uint("workspace_id", conn.WorkspaceId).Msg("failed to resolve workspace for managed skill cleanup")
				} else if ws != nil {
					if err := skills.DeleteManagedSourceSkill(ctx, cg.storage, ws.ExternalId, conn.IntegrationType); err != nil {
						log.Warn().Err(err).Str("workspace", ws.ExternalId).Str("integration", conn.IntegrationType).Msg("failed to delete managed source skill")
					}
				}
			}
		}
	}

	return c.JSON(http.StatusOK, Response{Success: true})
}
