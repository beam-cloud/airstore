package oauth

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestAnnotateCredentials_SetsScopesAndCapabilities(t *testing.T) {
	creds := &types.IntegrationCredentials{
		AccessToken: "token",
		Extra:       map[string]string{},
	}
	annotated := AnnotateCredentials(types.GitHub.String(), creds, []string{"repo", "user:email"})
	if annotated.Extra[types.CredentialMetaGrantedScopes] == "" {
		t.Fatalf("expected granted scopes metadata")
	}
	caps := types.CSVToList(annotated.Extra[types.CredentialMetaCapabilities])
	if !types.ListContainsFold(caps, string(types.CapabilitySourceWrite)) {
		t.Fatalf("expected source_write in capabilities, got %v", caps)
	}
}

func TestAnnotateCredentials_RespectsReadOnlyScopes(t *testing.T) {
	creds := &types.IntegrationCredentials{
		AccessToken: "token",
		Extra:       map[string]string{},
	}
	annotated := AnnotateCredentials(types.Gmail.String(), creds, []string{"https://www.googleapis.com/auth/gmail.readonly"})
	caps := types.CSVToList(annotated.Extra[types.CredentialMetaCapabilities])
	if types.ListContainsFold(caps, string(types.CapabilitySourceWrite)) {
		t.Fatalf("did not expect source_write for readonly gmail scope, got %v", caps)
	}
}

func TestMergeCredentialMetadata(t *testing.T) {
	target := &types.IntegrationCredentials{
		AccessToken: "new-token",
		Extra:       map[string]string{},
	}
	source := &types.IntegrationCredentials{
		AccessToken: "old-token",
		Extra: map[string]string{
			types.CredentialMetaGrantedScopes: "repo,user:email",
			types.CredentialMetaCapabilities:  "source_read,source_write",
		},
	}
	merged := MergeCredentialMetadata(target, source)
	if merged.Extra[types.CredentialMetaGrantedScopes] == "" {
		t.Fatalf("expected granted scopes metadata to be merged")
	}
	if merged.Extra[types.CredentialMetaCapabilities] == "" {
		t.Fatalf("expected capabilities metadata to be merged")
	}
}
