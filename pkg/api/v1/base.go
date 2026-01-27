package apiv1

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

const (
	HttpServerBaseRoute string = "/api/v1"
	HttpServerRootRoute string = ""
)

func NewHTTPError(code int, message string) error {
	return echo.NewHTTPError(code, map[string]interface{}{
		"message": message,
	})
}

func HTTPBadRequest(message string) error {
	return NewHTTPError(http.StatusBadRequest, message)
}

func HTTPInternalServerError(message string) error {
	return NewHTTPError(http.StatusInternalServerError, message)
}

func HTTPConflict(message string) error {
	return NewHTTPError(http.StatusConflict, message)
}

func HTTPUnauthorized(message string) error {
	return NewHTTPError(http.StatusUnauthorized, message)
}

func HTTPForbidden(message string) error {
	return NewHTTPError(http.StatusForbidden, message)
}

func HTTPNotFound() error {
	return NewHTTPError(http.StatusNotFound, "")
}

// Response is a standard API response structure
type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// SuccessResponse returns a successful response
func SuccessResponse(c echo.Context, data interface{}) error {
	return c.JSON(http.StatusOK, Response{
		Success: true,
		Data:    data,
	})
}

// ErrorResponse returns an error response
func ErrorResponse(c echo.Context, code int, message string) error {
	return c.JSON(code, Response{
		Success: false,
		Error:   message,
	})
}
