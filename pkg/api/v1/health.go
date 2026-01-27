package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type HealthGroup struct {
	redisClient *common.RedisClient
	routerGroup *echo.Group
}

func NewHealthGroup(g *echo.Group, rdb *common.RedisClient) *HealthGroup {
	group := &HealthGroup{routerGroup: g, redisClient: rdb}

	g.GET("", group.HealthCheck)

	return group
}

func (h *HealthGroup) HealthCheck(c echo.Context) error {
	err := h.redisClient.Ping(c.Request().Context()).Err()
	if err != nil {
		log.Error().Err(err).Msg("health check failed")
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"status": "not ok",
			"error":  err.Error(),
		})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"status": "ok",
	})
}
