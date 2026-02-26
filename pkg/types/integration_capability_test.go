package types

import "testing"

func TestDetermineCredentialCapabilities_GmailScopes(t *testing.T) {
	caps := DetermineCredentialCapabilities(Gmail, []string{"https://www.googleapis.com/auth/gmail.modify"})
	if !ListContainsFold(caps, string(CapabilitySourceRead)) {
		t.Fatalf("expected source_read capability, got %v", caps)
	}
	if !ListContainsFold(caps, string(CapabilitySourceWrite)) {
		t.Fatalf("expected source_write capability, got %v", caps)
	}

	readOnly := DetermineCredentialCapabilities(Gmail, []string{"https://www.googleapis.com/auth/gmail.readonly"})
	if ListContainsFold(readOnly, string(CapabilitySourceWrite)) {
		t.Fatalf("did not expect source_write capability for readonly scopes, got %v", readOnly)
	}
}

func TestDetermineCredentialCapabilities_NotionHasWriteWithoutScopes(t *testing.T) {
	caps := DetermineCredentialCapabilities(Notion, nil)
	if !ListContainsFold(caps, string(CapabilitySourceWrite)) {
		t.Fatalf("expected source_write capability for notion without scope hints, got %v", caps)
	}
}

func TestCredentialsSupportSourceWrite(t *testing.T) {
	creds := &IntegrationCredentials{
		AccessToken: "token",
		Extra: map[string]string{
			CredentialMetaCapabilities: "source_read,source_write",
		},
	}
	if !CredentialsSupportSourceWrite(GitHub, creds) {
		t.Fatalf("expected source write support from capabilities metadata")
	}

	creds = &IntegrationCredentials{
		AccessToken: "token",
		Extra: map[string]string{
			CredentialMetaGrantedScopes: "https://www.googleapis.com/auth/gmail.readonly",
		},
	}
	if CredentialsSupportSourceWrite(Gmail, creds) {
		t.Fatalf("did not expect gmail source write support for readonly scope")
	}
}
