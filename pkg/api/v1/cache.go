package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/compression"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// CacheGroup provides cache management endpoints.
type CacheGroup struct {
	routerGroup     *echo.Group
	compressedStore *compression.CompressedStore
}

// NewCacheGroup registers cache management routes.
// compressedStore may be nil if compression caching is disabled.
func NewCacheGroup(routerGroup *echo.Group, compressedStore *compression.CompressedStore) *CacheGroup {
	g := &CacheGroup{
		routerGroup:     routerGroup,
		compressedStore: compressedStore,
	}
	g.routerGroup.POST("/flush", g.FlushCompressionCache)
	return g
}

// FlushCompressionCache deletes all compressed content and pointer keys
// for the authenticated workspace.
func (g *CacheGroup) FlushCompressionCache(c echo.Context) error {
	if g.compressedStore == nil {
		return SuccessResponse(c, map[string]interface{}{
			"flushed":     true,
			"keys_deleted": 0,
			"message":     "compression cache is not enabled",
		})
	}

	wsId := auth.WorkspaceId(c.Request().Context())
	if wsId == 0 {
		return ErrorResponse(c, http.StatusBadRequest, "workspace context required")
	}

	deleted, err := g.compressedStore.FlushWorkspace(c.Request().Context(), wsId)
	if err != nil {
		log.Error().Err(err).Uint("workspace", wsId).Msg("failed to flush compression cache")
		return ErrorResponse(c, http.StatusInternalServerError, "flush failed")
	}

	log.Info().Uint("workspace", wsId).Int("keys_deleted", deleted).Msg("compression cache flushed")
	return SuccessResponse(c, map[string]interface{}{
		"flushed":      true,
		"keys_deleted": deleted,
	})
}
