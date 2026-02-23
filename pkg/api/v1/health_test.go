package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestHealthReadinessWithoutDependencies(t *testing.T) {
	e := echo.New()
	NewHealthGroup(e.Group("/health"), nil, nil, nil)

	code, resp := performHealthRequest(t, e, http.MethodGet, "/health/ready")
	require.Equal(t, http.StatusOK, code)
	require.True(t, resp.Ready)
	require.True(t, resp.Live)
	require.Equal(t, "ok", resp.Status)
	require.False(t, resp.Dependencies["redis"].Enabled)
	require.False(t, resp.Dependencies["postgres"].Enabled)
	require.False(t, resp.Dependencies["migrations"].Enabled)
}

func TestHealthReadinessDependencyFailure(t *testing.T) {
	e := echo.New()
	NewHealthGroup(
		e.Group("/health"),
		func(context.Context) error { return errors.New("redis unavailable") },
		nil,
		nil,
	)

	code, resp := performHealthRequest(t, e, http.MethodGet, "/health/ready")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.False(t, resp.Ready)
	require.Equal(t, "degraded", resp.Status)
	require.True(t, resp.Dependencies["redis"].Enabled)
	require.False(t, resp.Dependencies["redis"].Ready)
	require.Contains(t, resp.Dependencies["redis"].Error, "redis unavailable")
}

func TestHealthLivenessStaysUpWhenDependenciesFail(t *testing.T) {
	e := echo.New()
	NewHealthGroup(
		e.Group("/health"),
		func(context.Context) error { return errors.New("redis unavailable") },
		func(context.Context) error { return errors.New("postgres unavailable") },
		func() bool { return false },
	)

	code, resp := performHealthRequest(t, e, http.MethodGet, "/health/live")
	require.Equal(t, http.StatusOK, code)
	require.True(t, resp.Live)
	require.False(t, resp.Ready)
	require.Equal(t, "degraded", resp.Status)
}

func TestHealthReadinessFailsWhenMigrationsNotReady(t *testing.T) {
	e := echo.New()
	NewHealthGroup(
		e.Group("/health"),
		nil,
		func(context.Context) error { return nil },
		func() bool { return false },
	)

	code, resp := performHealthRequest(t, e, http.MethodGet, "/health/ready")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.True(t, resp.Dependencies["postgres"].Enabled)
	require.True(t, resp.Dependencies["migrations"].Enabled)
	require.False(t, resp.Dependencies["migrations"].Ready)
	require.Contains(t, resp.Dependencies["migrations"].Error, "migrations not ready")
}

func TestHealthRootRouteNotRegistered(t *testing.T) {
	e := echo.New()
	NewHealthGroup(e.Group("/health"), nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func performHealthRequest(t *testing.T, e *echo.Echo, method, path string) (int, healthResponse) {
	t.Helper()

	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	var resp healthResponse
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	return rec.Code, resp
}
