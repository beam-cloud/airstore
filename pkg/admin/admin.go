package admin

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

//go:embed static/*
var staticFiles embed.FS

// Service is the admin UI service
type Service struct {
	config        types.AdminConfig
	backend       repository.BackendRepository
	session       *SessionManager
	oauth         *OAuthService
	storageClient *clients.StorageClient
}

// NewService creates a new Admin service.
// storageClient can be nil if workspace storage is not configured.
func NewService(config types.AdminConfig, backend repository.BackendRepository, storageClient *clients.StorageClient) *Service {
	session := NewSessionManager(config.SessionKey)
	oauth := NewOAuthService(config.OAuth.Google, session)

	return &Service{
		config:        config,
		backend:       backend,
		session:       session,
		oauth:         oauth,
		storageClient: storageClient,
	}
}

// RegisterRoutes registers the admin routes with the Echo instance
func (s *Service) RegisterRoutes(e *echo.Echo) {
	// OAuth routes (public)
	e.GET("/auth/google", s.oauth.HandleLogin)
	e.GET("/auth/google/callback", s.oauth.HandleCallback)

	// Admin group
	admin := e.Group("/admin")

	// Public routes
	admin.GET("/login", s.handleLoginPage)
	admin.GET("/logout", s.oauth.HandleLogout)

	// Serve embedded static files
	if staticFS, err := fs.Sub(staticFiles, "static"); err == nil {
		admin.GET("/static/*", echo.WrapHandler(
			http.StripPrefix("/admin/static/", http.FileServer(http.FS(staticFS))),
		))
	}

	// Protected routes (require auth)
	protected := admin.Group("", s.requireAuth)
	protected.GET("", s.handleDashboard)
	protected.GET("/", s.handleDashboard)

	// API routes
	NewAPIGroup(protected.Group("/api"), s.backend, s.session, s.storageClient)

	log.Info().Msg("admin UI registered at /admin")
}

// requireAuth middleware validates session
func (s *Service) requireAuth(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		session := s.session.Get(c)
		if session == nil {
			if isAPIRequest(c) {
				return echo.NewHTTPError(http.StatusUnauthorized, "unauthorized")
			}
			return c.Redirect(http.StatusFound, "/admin/login")
		}
		c.Set("session", session)
		return next(c)
	}
}

// handleLoginPage renders the login page
func (s *Service) handleLoginPage(c echo.Context) error {
	if s.session.Get(c) != nil {
		return c.Redirect(http.StatusFound, "/admin")
	}
	return c.HTML(http.StatusOK, loginPageHTML)
}

// handleDashboard renders the main dashboard
func (s *Service) handleDashboard(c echo.Context) error {
	return c.HTML(http.StatusOK, dashboardPageHTML)
}

// isAPIRequest checks if the request expects JSON
func isAPIRequest(c echo.Context) bool {
	return c.Request().Header.Get("Accept") == "application/json"
}

const loginPageHTML = `<!DOCTYPE html>
<html>
<head>
    <title>Login - Airstore Admin</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-100 min-h-screen flex items-center justify-center">
    <div class="bg-white p-8 rounded-lg shadow-md max-w-md w-full">
        <div class="text-center mb-8">
            <h1 class="text-2xl font-bold text-gray-800">Airstore Admin</h1>
            <p class="text-gray-600 mt-2">Sign in to manage workspaces and integrations</p>
        </div>
        <a href="/auth/google" 
           class="flex items-center justify-center gap-3 w-full bg-white border border-gray-300 rounded-lg px-4 py-3 text-gray-700 hover:bg-gray-50 transition-colors">
            <svg class="w-5 h-5" viewBox="0 0 24 24">
                <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/>
                <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>
                <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/>
                <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/>
            </svg>
            <span class="font-medium">Sign in with Google</span>
        </a>
    </div>
</body>
</html>`

const dashboardPageHTML = `<!DOCTYPE html>
<html>
<head>
    <title>Airstore Admin</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-100 min-h-screen">
    <div id="app">
        <div class="flex items-center justify-center min-h-screen">
            <div class="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
        </div>
    </div>
    <script src="/admin/static/app.js"></script>
</body>
</html>`
