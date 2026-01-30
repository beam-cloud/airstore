package admin

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v4"
)

const (
	cookieName      = "airstore_session"
	sessionDuration = 24 * time.Hour
)

// Claims contains the JWT claims for an admin session
type Claims struct {
	Email   string `json:"email"`
	Name    string `json:"name"`
	Picture string `json:"picture"`
	jwt.RegisteredClaims
}

// SessionManager handles JWT session creation and validation
type SessionManager struct {
	secret []byte
}

// NewSessionManager creates a new session manager
func NewSessionManager(secret string) *SessionManager {
	if secret == "" {
		// Generate random key (sessions won't persist across restarts)
		b := make([]byte, 32)
		rand.Read(b)
		secret = hex.EncodeToString(b)
	}
	return &SessionManager{secret: []byte(secret)}
}

// Create generates a new JWT session token
func (s *SessionManager) Create(email, name, picture string) (string, error) {
	claims := Claims{
		Email:   email,
		Name:    name,
		Picture: picture,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(sessionDuration)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "airstore",
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
}

// Validate parses and validates a JWT token
func (s *SessionManager) Validate(tokenStr string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (any, error) {
		return s.secret, nil
	})
	if err != nil {
		return nil, err
	}
	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims, nil
	}
	return nil, jwt.ErrSignatureInvalid
}

// Get retrieves and validates the session from request cookies
func (s *SessionManager) Get(c echo.Context) *Claims {
	cookie, err := c.Cookie(cookieName)
	if err != nil {
		return nil
	}
	claims, err := s.Validate(cookie.Value)
	if err != nil {
		return nil
	}
	return claims
}

// Set stores the session token in a cookie
func (s *SessionManager) Set(c echo.Context, token string) {
	c.SetCookie(&http.Cookie{
		Name:     cookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   c.Request().TLS != nil,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(sessionDuration.Seconds()),
	})
}

// Clear removes the session cookie
func (s *SessionManager) Clear(c echo.Context) {
	c.SetCookie(&http.Cookie{
		Name:   cookieName,
		Value:  "",
		Path:   "/",
		MaxAge: -1,
	})
}
