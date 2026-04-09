package oauth

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestMicrosoftAuthorizeURL_TeamsScopes(t *testing.T) {
	provider := NewMicrosoftProvider(types.ProviderOAuthCredentials{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
	}, "https://example.com/oauth/callback")

	authorizeURL, err := provider.AuthorizeURL("state-123", "teams")
	if err != nil {
		t.Fatalf("AuthorizeURL returned error: %v", err)
	}

	parsed, err := url.Parse(authorizeURL)
	if err != nil {
		t.Fatalf("parse authorize URL: %v", err)
	}

	if got := parsed.Query().Get("prompt"); got != "consent" {
		t.Fatalf("expected prompt=consent, got %q", got)
	}

	gotScopes := ParseScopeString(parsed.Query().Get("scope"))
	if !reflect.DeepEqual(gotScopes, microsoftTeamsScopes) {
		t.Fatalf("expected teams scopes %v, got %v", microsoftTeamsScopes, gotScopes)
	}
}

func TestMicrosoftUserFacingError_TeamsConsentRequired(t *testing.T) {
	provider := NewMicrosoftProvider(types.ProviderOAuthCredentials{}, "https://example.com/oauth/callback")

	tests := []string{
		"microsoft: access_denied: AADSTS65001: The user or administrator has not consented to use the application.",
		"microsoft: consent_required",
		"microsoft: invalid_grant: Need admin approval",
	}

	for _, raw := range tests {
		if got := provider.UserFacingError("teams", raw); got != teamsAdminConsentMessage {
			t.Fatalf("expected consent message for %q, got %q", raw, got)
		}
	}
}

func TestMicrosoftUserFacingError_UnrelatedErrorsPassThrough(t *testing.T) {
	provider := NewMicrosoftProvider(types.ProviderOAuthCredentials{}, "https://example.com/oauth/callback")

	raw := "microsoft: access_denied"
	if got := provider.UserFacingError("teams", raw); got != raw {
		t.Fatalf("expected raw error to pass through, got %q", got)
	}

	consentRaw := "microsoft: access_denied: AADSTS65001: Need admin approval"
	if got := provider.UserFacingError("outlook", consentRaw); got != consentRaw {
		t.Fatalf("expected non-Teams error to pass through, got %q", got)
	}
}
