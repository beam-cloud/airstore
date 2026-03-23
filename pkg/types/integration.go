package types

import "strings"

type IntegrationScope string

const (
	ScopePersonal IntegrationScope = "personal"
	ScopeShared   IntegrationScope = "shared"
)

type IntegrationCapability string

const (
	CapabilitySourceRead  IntegrationCapability = "source_read"
	CapabilitySourceWrite IntegrationCapability = "source_write"
)

const (
	// Credential metadata keys used for OAuth capability gating.
	CredentialMetaGrantedScopes = "oauth_granted_scopes"
	CredentialMetaCapabilities  = "airstore_capabilities"
)

type IntegrationAuthType string

const (
	AuthNone   IntegrationAuthType = "none"
	AuthAPIKey IntegrationAuthType = "apikey"
	AuthOAuth  IntegrationAuthType = "oauth"
)

type IntegrationMeta struct {
	Name                IntegrationName
	DisplayName         string
	Description         string
	Icon                string
	AuthType            IntegrationAuthType
	Scope               IntegrationScope
	Capabilities        []IntegrationCapability
	OAuthWriteScopeHint []string
}

var integrations = map[IntegrationName]IntegrationMeta{
	Wikipedia: {
		Name:        Wikipedia,
		DisplayName: "Wikipedia",
		Description: "Encyclopedic knowledge lookup",
		Icon:        "book-open",
		AuthType:    AuthNone,
		Scope:       ScopeShared,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
		},
	},
	Weather: {
		Name:        Weather,
		DisplayName: "Weather",
		Description: "Weather and forecasts",
		Icon:        "cloud-sun",
		AuthType:    AuthAPIKey,
		Scope:       ScopeShared,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
		},
	},
	Exa: {
		Name:        Exa,
		DisplayName: "Exa",
		Description: "Neural web search",
		Icon:        "search",
		AuthType:    AuthAPIKey,
		Scope:       ScopeShared,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
		},
	},
	GitHub: {
		Name:        GitHub,
		DisplayName: "GitHub",
		Description: "Repository and PR management",
		Icon:        "github",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
			CapabilitySourceWrite,
		},
		OAuthWriteScopeHint: []string{"repo"},
	},
	Gmail: {
		Name:        Gmail,
		DisplayName: "Gmail",
		Description: "Email access and management",
		Icon:        "mail",
		AuthType:    AuthOAuth,
		Scope:       ScopePersonal,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
			CapabilitySourceWrite,
		},
		OAuthWriteScopeHint: []string{"https://www.googleapis.com/auth/gmail.modify"},
	},
	Notion: {
		Name:        Notion,
		DisplayName: "Notion",
		Description: "Workspace pages and databases",
		Icon:        "file-text",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
			CapabilitySourceWrite,
		},
	},
	GDrive: {
		Name:        GDrive,
		DisplayName: "Google Drive",
		Description: "Cloud file storage",
		Icon:        "hard-drive",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
		},
	},
	Slack: {
		Name:        Slack,
		DisplayName: "Slack",
		Description: "Channels, messages, and files",
		Icon:        "slack",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
			CapabilitySourceWrite,
		},
		OAuthWriteScopeHint: []string{"chat:write", "chat:write.public"},
	},
	Linear: {
		Name:        Linear,
		DisplayName: "Linear",
		Description: "Issues, projects, and team workflows",
		Icon:        "square-kanban",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
			CapabilitySourceWrite,
		},
		OAuthWriteScopeHint: []string{"write", "issues:create", "comments:create"},
	},
	PostHog: {
		Name:        PostHog,
		DisplayName: "PostHog",
		Description: "Product analytics and feature flags",
		Icon:        "bar-chart",
		AuthType:    AuthAPIKey,
		Scope:       ScopeShared,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
		},
	},
	Confluence: {
		Name:        Confluence,
		DisplayName: "Confluence",
		Description: "Wiki pages and spaces",
		Icon:        "book",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
		},
	},
	Web: {
		Name:        Web,
		DisplayName: "Web",
		Description: "Crawl websites into markdown files",
		Icon:        "globe",
		AuthType:    AuthNone,
		Scope:       ScopeShared,
		Capabilities: []IntegrationCapability{
			CapabilitySourceRead,
		},
	},
	Browser: {
		Name:        Browser,
		DisplayName: "Browser",
		Description: "Headless browser automation via Kernel cloud browsers",
		Icon:        "monitor",
		AuthType:    AuthNone,
		Scope:       ScopeShared,
	},
}

func GetIntegrationMeta(name IntegrationName) (IntegrationMeta, bool) {
	meta, ok := integrations[name]
	return meta, ok
}

func ListIntegrations() []IntegrationMeta {
	result := make([]IntegrationMeta, 0, len(integrations))
	for _, meta := range integrations {
		result = append(result, meta)
	}
	return result
}

func RequiresAuth(name IntegrationName) bool {
	meta, ok := integrations[name]
	return ok && meta.AuthType != AuthNone
}

func IsPersonalScope(name IntegrationName) bool {
	meta, ok := integrations[name]
	return ok && meta.Scope == ScopePersonal
}

func SupportsCapability(name IntegrationName, capability IntegrationCapability) bool {
	meta, ok := integrations[name]
	if !ok {
		return false
	}
	for _, c := range meta.Capabilities {
		if c == capability {
			return true
		}
	}
	return false
}

func SupportsSourceRead(name IntegrationName) bool {
	return SupportsCapability(name, CapabilitySourceRead)
}

func SupportsSourceWrite(name IntegrationName) bool {
	return SupportsCapability(name, CapabilitySourceWrite)
}

func OAuthWriteScopeHints(name IntegrationName) []string {
	meta, ok := integrations[name]
	if !ok || len(meta.OAuthWriteScopeHint) == 0 {
		return nil
	}
	out := make([]string, len(meta.OAuthWriteScopeHint))
	copy(out, meta.OAuthWriteScopeHint)
	return out
}

func CSVToList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func ListToCSV(values []string) string {
	if len(values) == 0 {
		return ""
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trimmed)
	}
	return strings.Join(out, ",")
}

func ListContainsFold(values []string, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), want) {
			return true
		}
	}
	return false
}

func DetermineCredentialCapabilities(integration IntegrationName, grantedScopes []string) []string {
	capabilities := make([]string, 0, 2)
	if SupportsSourceRead(integration) {
		capabilities = append(capabilities, string(CapabilitySourceRead))
	}
	if !SupportsSourceWrite(integration) {
		return capabilities
	}

	scopeHints := OAuthWriteScopeHints(integration)
	if len(scopeHints) == 0 {
		capabilities = append(capabilities, string(CapabilitySourceWrite))
		return capabilities
	}
	for _, hint := range scopeHints {
		if ListContainsFold(grantedScopes, hint) {
			capabilities = append(capabilities, string(CapabilitySourceWrite))
			return capabilities
		}
	}
	return capabilities
}

func CredentialsSupportSourceWrite(integration IntegrationName, creds *IntegrationCredentials) bool {
	if creds == nil || (!SupportsSourceWrite(integration)) {
		return false
	}
	if creds.AccessToken == "" && creds.APIKey == "" {
		return false
	}
	if creds.Extra == nil {
		return false
	}

	if rawCaps := creds.Extra[CredentialMetaCapabilities]; rawCaps != "" {
		capabilities := CSVToList(rawCaps)
		return ListContainsFold(capabilities, string(CapabilitySourceWrite))
	}

	scopeHints := OAuthWriteScopeHints(integration)
	if len(scopeHints) == 0 {
		return true
	}

	scopes := CSVToList(creds.Extra[CredentialMetaGrantedScopes])
	for _, hint := range scopeHints {
		if ListContainsFold(scopes, hint) {
			return true
		}
	}
	return false
}
