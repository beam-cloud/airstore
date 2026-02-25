package apiv1

import (
	"context"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
)

type HealthGroup struct {
	routerGroup     *echo.Group
	redisProbe      func(context.Context) error
	postgresProbe   func(context.Context) error
	migrationsReady func() bool
}

type healthDependencyState struct {
	Enabled bool   `json:"enabled"`
	Ready   bool   `json:"ready"`
	Error   string `json:"error,omitempty"`
}

type healthResponse struct {
	Status       string                           `json:"status"`
	Live         bool                             `json:"live"`
	Ready        bool                             `json:"ready"`
	Dependencies map[string]healthDependencyState `json:"dependencies"`
}

func NewHealthGroup(g *echo.Group, redisProbe func(context.Context) error, postgresProbe func(context.Context) error, migrationsReady func() bool) *HealthGroup {
	group := &HealthGroup{
		routerGroup:     g,
		redisProbe:      redisProbe,
		postgresProbe:   postgresProbe,
		migrationsReady: migrationsReady,
	}

	g.GET("/ready", group.ReadinessCheck)
	g.GET("/live", group.LivenessCheck)

	return group
}

func (h *HealthGroup) LivenessCheck(c echo.Context) error {
	resp := h.buildResponse(c.Request().Context())
	resp.Live = true
	return c.JSON(http.StatusOK, resp)
}

func (h *HealthGroup) ReadinessCheck(c echo.Context) error {
	resp := h.buildResponse(c.Request().Context())
	if !resp.Ready {
		return c.JSON(http.StatusServiceUnavailable, resp)
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *HealthGroup) buildResponse(ctx context.Context) healthResponse {
	redisState := checkDependency(ctx, h.redisProbe)
	postgresState := checkDependency(ctx, h.postgresProbe)

	migrationsEnabled := postgresState.Enabled
	migrationsState := healthDependencyState{
		Enabled: migrationsEnabled,
		Ready:   true,
	}
	if migrationsEnabled {
		if h.migrationsReady != nil {
			migrationsState.Ready = h.migrationsReady()
		}
		if !migrationsState.Ready {
			migrationsState.Error = "migrations not ready"
		}
	}

	deps := map[string]healthDependencyState{
		"redis":      redisState,
		"postgres":   postgresState,
		"migrations": migrationsState,
	}

	ready := true
	for _, dep := range deps {
		if dep.Enabled && !dep.Ready {
			ready = false
			break
		}
	}

	status := "ok"
	if !ready {
		status = "degraded"
	}

	return healthResponse{
		Status:       status,
		Live:         true,
		Ready:        ready,
		Dependencies: deps,
	}
}

func checkDependency(ctx context.Context, probe func(context.Context) error) healthDependencyState {
	if probe == nil {
		return healthDependencyState{
			Enabled: false,
			Ready:   true,
		}
	}

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := probe(pingCtx); err != nil {
		return healthDependencyState{
			Enabled: true,
			Ready:   false,
			Error:   err.Error(),
		}
	}

	return healthDependencyState{
		Enabled: true,
		Ready:   true,
	}
}
