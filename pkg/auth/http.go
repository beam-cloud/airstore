package auth

import (
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// HTTPMiddleware validates auth tokens and adds AuthInfo to context.
// Allows requests to proceed without auth; routes must explicitly require auth.
func HTTPMiddleware(validator TokenValidator) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			token := strings.TrimPrefix(c.Request().Header.Get("Authorization"), "Bearer ")
			if token == "" {
				return next(c)
			}

			ctx := c.Request().Context()

			if validator.ValidateClusterToken(token) {
				ctx = WithAuthInfo(ctx, &types.AuthInfo{TokenType: types.TokenTypeClusterAdmin})
				c.SetRequest(c.Request().WithContext(ctx))
				return next(c)
			}

			info, err := validator.ValidateToken(ctx, token)
			if err != nil {
				log.Debug().Err(err).Msg("auth: invalid token")
				return c.JSON(http.StatusUnauthorized, map[string]string{"error": "invalid token"})
			}

			if info != nil {
				ctx = WithAuthInfo(ctx, info)
				c.SetRequest(c.Request().WithContext(ctx))
			}

			return next(c)
		}
	}
}

// Handler wrappers

func WithAuth(h echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if err := RequireAuth(c.Request().Context()); err != nil {
			return c.JSON(http.StatusUnauthorized, map[string]string{"error": err.Error()})
		}
		return h(c)
	}
}

func WithClusterAdmin(h echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if err := RequireClusterAdmin(c.Request().Context()); err != nil {
			return c.JSON(http.StatusForbidden, map[string]string{"error": err.Error()})
		}
		return h(c)
	}
}

func WithAdmin(h echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if err := RequireAdmin(c.Request().Context()); err != nil {
			return c.JSON(http.StatusForbidden, map[string]string{"error": err.Error()})
		}
		return h(c)
	}
}

func WithWorkspaceAccess(h echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		wsId := c.Param("workspace_id")
		if wsId == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "workspace_id required"})
		}
		if err := RequireWorkspaceAccess(c.Request().Context(), wsId); err != nil {
			return c.JSON(http.StatusForbidden, map[string]string{"error": err.Error()})
		}
		return h(c)
	}
}

// Middleware factories

func RequireClusterAdminMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc { return WithClusterAdmin(next) }
}

func RequireAuthMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc { return WithAuth(next) }
}
