package oauth

import (
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

func ParseScopeString(scope string) []string {
	if strings.TrimSpace(scope) == "" {
		return nil
	}
	scope = strings.ReplaceAll(scope, ",", " ")
	parts := strings.Fields(scope)
	if len(parts) == 0 {
		return nil
	}
	return parts
}

func NormalizeScopes(scopes ...[]string) []string {
	merged := make([]string, 0)
	for _, list := range scopes {
		merged = append(merged, list...)
	}
	return types.CSVToList(types.ListToCSV(merged))
}

func AnnotateCredentials(integrationType string, creds *types.IntegrationCredentials, grantedScopes []string) *types.IntegrationCredentials {
	if creds == nil {
		return nil
	}

	integration := types.IntegrationName(integrationType)
	scopes := NormalizeScopes(grantedScopes)
	if len(scopes) == 0 {
		scopes = types.OAuthWriteScopeHints(integration)
	}

	if creds.Extra == nil {
		creds.Extra = make(map[string]string)
	}
	if len(scopes) > 0 {
		creds.Extra[types.CredentialMetaGrantedScopes] = types.ListToCSV(scopes)
	}

	capabilities := types.DetermineCredentialCapabilities(integration, scopes)
	if len(capabilities) > 0 {
		creds.Extra[types.CredentialMetaCapabilities] = types.ListToCSV(capabilities)
	}
	return creds
}

// MergeCredentialMetadata copies all Extra keys from source to target when missing.
// This keeps persisted metadata (capabilities, scopes, cloud_id, etc.) stable across token refreshes.
func MergeCredentialMetadata(target, source *types.IntegrationCredentials) *types.IntegrationCredentials {
	if target == nil {
		return nil
	}
	if source == nil || source.Extra == nil {
		return target
	}
	if target.Extra == nil {
		target.Extra = make(map[string]string)
	}
	for key, value := range source.Extra {
		if _, ok := target.Extra[key]; ok {
			continue
		}
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			target.Extra[key] = trimmed
		}
	}
	return target
}
