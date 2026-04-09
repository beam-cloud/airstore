package types

import "testing"

func TestDetermineCredentialCapabilities_GmailScopes(t *testing.T) {
	// New scope: gmail.compose grants write
	caps := DetermineCredentialCapabilities(Gmail, []string{"https://www.googleapis.com/auth/gmail.compose"})
	if !ListContainsFold(caps, string(CapabilitySourceRead)) {
		t.Fatalf("expected source_read capability, got %v", caps)
	}
	if !ListContainsFold(caps, string(CapabilitySourceWrite)) {
		t.Fatalf("expected source_write capability for gmail.compose, got %v", caps)
	}

	// Legacy scope: gmail.modify still grants write
	legacy := DetermineCredentialCapabilities(Gmail, []string{"https://www.googleapis.com/auth/gmail.modify"})
	if !ListContainsFold(legacy, string(CapabilitySourceWrite)) {
		t.Fatalf("expected source_write capability for gmail.modify, got %v", legacy)
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

func TestDetermineCredentialCapabilities_TeamsScopes(t *testing.T) {
	// Full Microsoft Graph URL scopes (as granted by OAuth) should yield source_write
	caps := DetermineCredentialCapabilities(Teams, []string{
		"https://graph.microsoft.com/Team.ReadBasic.All",
		"https://graph.microsoft.com/Channel.ReadBasic.All",
		"https://graph.microsoft.com/ChannelMessage.Send",
		"https://graph.microsoft.com/Chat.ReadWrite",
		"https://graph.microsoft.com/User.Read",
	})
	if !ListContainsFold(caps, string(CapabilitySourceRead)) {
		t.Fatalf("expected source_read capability, got %v", caps)
	}
	if !ListContainsFold(caps, string(CapabilitySourceWrite)) {
		t.Fatalf("expected source_write capability for Teams write scopes, got %v", caps)
	}

	// Read-only scopes should not yield source_write
	readOnly := DetermineCredentialCapabilities(Teams, []string{
		"https://graph.microsoft.com/Team.ReadBasic.All",
		"https://graph.microsoft.com/Channel.ReadBasic.All",
		"https://graph.microsoft.com/User.Read",
	})
	if ListContainsFold(readOnly, string(CapabilitySourceWrite)) {
		t.Fatalf("did not expect source_write capability for read-only Teams scopes, got %v", readOnly)
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
