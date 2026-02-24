package gateway

import (
	"net/http"
	"strings"

	apiv1 "github.com/beam-cloud/airstore/pkg/api/v1"
	"github.com/labstack/echo/v4"
)

func shouldSkipHTTPRequestLog(c echo.Context) bool {
	path := c.Path()
	if path == "" {
		path = c.Request().URL.Path
	}

	base := apiv1.HttpServerBaseRoute

	switch path {
	case base + "/health/live", base + "/health/ready":
		return true
	}

	if c.Request().Method != http.MethodGet {
		return false
	}

	isAccessLogPollTemplate := path == base+"/workspaces/:workspace_id/access-log"
	isAccessLogPollURL := strings.HasPrefix(path, base+"/workspaces/") && strings.HasSuffix(path, "/access-log")
	if (isAccessLogPollTemplate || isAccessLogPollURL) && c.QueryParam("cursor") != "" {
		return true
	}

	return false
}
