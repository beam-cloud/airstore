package admin

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const (
	googleUserInfoURL = "https://www.googleapis.com/oauth2/v2/userinfo"
	stateCookieName   = "oauth_state"
)

// GoogleUser contains user info from Google
type GoogleUser struct {
	ID      string `json:"id"`
	Email   string `json:"email"`
	Name    string `json:"name"`
	Picture string `json:"picture"`
}

// OAuthService handles Google OAuth authentication
type OAuthService struct {
	oauth   *oauth2.Config
	allowed []string
	session *SessionManager
}

// NewOAuthService creates a new OAuth service
func NewOAuthService(cfg types.GoogleOAuthConfig, session *SessionManager) *OAuthService {
	return &OAuthService{
		oauth: &oauth2.Config{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			RedirectURL:  cfg.RedirectURL,
			Scopes: []string{
				"https://www.googleapis.com/auth/userinfo.email",
				"https://www.googleapis.com/auth/userinfo.profile",
			},
			Endpoint: google.Endpoint,
		},
		allowed: cfg.AllowedEmails,
		session: session,
	}
}

// HandleLogin initiates the OAuth flow
func (o *OAuthService) HandleLogin(c echo.Context) error {
	state := generateState()

	c.SetCookie(&http.Cookie{
		Name:     stateCookieName,
		Value:    state,
		Path:     "/",
		HttpOnly: true,
		Secure:   c.Request().TLS != nil,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   300,
	})

	return c.Redirect(http.StatusFound, o.oauth.AuthCodeURL(state, oauth2.AccessTypeOffline))
}

// HandleCallback handles the OAuth callback
func (o *OAuthService) HandleCallback(c echo.Context) error {
	// Validate state
	stateCookie, err := c.Cookie(stateCookieName)
	if err != nil || c.QueryParam("state") != stateCookie.Value {
		return o.errorPage(c, "Invalid OAuth state")
	}

	// Clear state cookie
	c.SetCookie(&http.Cookie{Name: stateCookieName, Path: "/", MaxAge: -1})

	// Check for OAuth error
	if errParam := c.QueryParam("error"); errParam != "" {
		return o.errorPage(c, "OAuth error: "+errParam)
	}

	// Exchange code for token
	code := c.QueryParam("code")
	if code == "" {
		return o.errorPage(c, "Missing authorization code")
	}

	ctx, cancel := context.WithTimeout(c.Request().Context(), 10*time.Second)
	defer cancel()

	token, err := o.oauth.Exchange(ctx, code)
	if err != nil {
		return o.errorPage(c, "Token exchange failed")
	}

	// Get user info
	user, err := o.fetchUser(ctx, token)
	if err != nil {
		return o.errorPage(c, "Failed to get user info")
	}

	// Check allowed list
	if len(o.allowed) > 0 && !slices.Contains(o.allowed, user.Email) {
		return o.errorPage(c, "Email not authorized: "+user.Email)
	}

	// Create session
	sessionToken, err := o.session.Create(user.Email, user.Name, user.Picture)
	if err != nil {
		return o.errorPage(c, "Failed to create session")
	}

	o.session.Set(c, sessionToken)
	return c.Redirect(http.StatusFound, "/admin")
}

// HandleLogout clears the session
func (o *OAuthService) HandleLogout(c echo.Context) error {
	o.session.Clear(c)
	return c.Redirect(http.StatusFound, "/admin/login")
}

// fetchUser gets user info from Google
func (o *OAuthService) fetchUser(ctx context.Context, token *oauth2.Token) (*GoogleUser, error) {
	client := o.oauth.Client(ctx, token)
	resp, err := client.Get(googleUserInfoURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var user GoogleUser
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, err
	}
	return &user, nil
}

// errorPage renders an error page
func (o *OAuthService) errorPage(c echo.Context, msg string) error {
	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
    <title>Error - Airstore Admin</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-100 min-h-screen flex items-center justify-center">
    <div class="bg-white p-8 rounded-lg shadow-md max-w-md">
        <h1 class="text-xl font-bold text-red-600 mb-4">Error</h1>
        <p class="text-gray-700 mb-4">%s</p>
        <a href="/admin/login" class="text-blue-600 hover:underline">Back to Login</a>
    </div>
</body>
</html>`, msg)
	return c.HTML(http.StatusBadRequest, html)
}

func generateState() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}
