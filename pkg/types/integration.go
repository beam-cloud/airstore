package types

// IntegrationScope defines whether an integration is personal or workspace-shared
type IntegrationScope string

const (
	ScopePersonal IntegrationScope = "personal"
	ScopeShared   IntegrationScope = "shared"
)

// IntegrationAuthType defines how an integration authenticates
type IntegrationAuthType string

const (
	AuthNone   IntegrationAuthType = "none"
	AuthAPIKey IntegrationAuthType = "apikey"
	AuthOAuth  IntegrationAuthType = "oauth"
)

// IntegrationMeta contains metadata about an integration type
type IntegrationMeta struct {
	Tool        ToolName
	DisplayName string
	Description string
	Icon        string
	AuthType    IntegrationAuthType
	Scope       IntegrationScope
}

// integrations is the canonical registry of all supported integrations
var integrations = map[ToolName]IntegrationMeta{
	ToolWikipedia: {
		Tool:        ToolWikipedia,
		DisplayName: "Wikipedia",
		Description: "Encyclopedic knowledge lookup",
		Icon:        "book-open",
		AuthType:    AuthNone,
		Scope:       ScopeShared,
	},
	ToolWeather: {
		Tool:        ToolWeather,
		DisplayName: "Weather",
		Description: "Weather and forecasts",
		Icon:        "cloud-sun",
		AuthType:    AuthAPIKey,
		Scope:       ScopeShared,
	},
	ToolExa: {
		Tool:        ToolExa,
		DisplayName: "Exa",
		Description: "Neural web search",
		Icon:        "search",
		AuthType:    AuthAPIKey,
		Scope:       ScopeShared,
	},
	ToolGitHub: {
		Tool:        ToolGitHub,
		DisplayName: "GitHub",
		Description: "Repository and PR management",
		Icon:        "github",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
	},
}

// GetIntegrationMeta returns metadata for an integration
func GetIntegrationMeta(tool ToolName) (IntegrationMeta, bool) {
	meta, ok := integrations[tool]
	return meta, ok
}

// ListIntegrations returns all registered integrations
func ListIntegrations() []IntegrationMeta {
	result := make([]IntegrationMeta, 0, len(integrations))
	for _, meta := range integrations {
		result = append(result, meta)
	}
	return result
}

// RequiresAuth returns true if the integration needs credentials
func RequiresAuth(tool ToolName) bool {
	meta, ok := integrations[tool]
	return ok && meta.AuthType != AuthNone
}

// IsPersonalScope returns true if the integration is personal by default
func IsPersonalScope(tool ToolName) bool {
	meta, ok := integrations[tool]
	return ok && meta.Scope == ScopePersonal
}
