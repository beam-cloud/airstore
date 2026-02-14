package types

type IntegrationScope string

const (
	ScopePersonal IntegrationScope = "personal"
	ScopeShared   IntegrationScope = "shared"
)

type IntegrationAuthType string

const (
	AuthNone   IntegrationAuthType = "none"
	AuthAPIKey IntegrationAuthType = "apikey"
	AuthOAuth  IntegrationAuthType = "oauth"
)

type IntegrationMeta struct {
	Name        IntegrationName
	DisplayName string
	Description string
	Icon        string
	AuthType    IntegrationAuthType
	Scope       IntegrationScope
}

var integrations = map[IntegrationName]IntegrationMeta{
	Wikipedia: {
		Name:        Wikipedia,
		DisplayName: "Wikipedia",
		Description: "Encyclopedic knowledge lookup",
		Icon:        "book-open",
		AuthType:    AuthNone,
		Scope:       ScopeShared,
	},
	Weather: {
		Name:        Weather,
		DisplayName: "Weather",
		Description: "Weather and forecasts",
		Icon:        "cloud-sun",
		AuthType:    AuthAPIKey,
		Scope:       ScopeShared,
	},
	Exa: {
		Name:        Exa,
		DisplayName: "Exa",
		Description: "Neural web search",
		Icon:        "search",
		AuthType:    AuthAPIKey,
		Scope:       ScopeShared,
	},
	GitHub: {
		Name:        GitHub,
		DisplayName: "GitHub",
		Description: "Repository and PR management",
		Icon:        "github",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
	},
	Gmail: {
		Name:        Gmail,
		DisplayName: "Gmail",
		Description: "Email access and management",
		Icon:        "mail",
		AuthType:    AuthOAuth,
		Scope:       ScopePersonal,
	},
	Notion: {
		Name:        Notion,
		DisplayName: "Notion",
		Description: "Workspace pages and databases",
		Icon:        "file-text",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
	},
	GDrive: {
		Name:        GDrive,
		DisplayName: "Google Drive",
		Description: "Cloud file storage",
		Icon:        "hard-drive",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
	},
	Slack: {
		Name:        Slack,
		DisplayName: "Slack",
		Description: "Channels, messages, and files",
		Icon:        "slack",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
	},
	Linear: {
		Name:        Linear,
		DisplayName: "Linear",
		Description: "Issues, projects, and team workflows",
		Icon:        "square-kanban",
		AuthType:    AuthOAuth,
		Scope:       ScopeShared,
	},
	PostHog: {
		Name:        PostHog,
		DisplayName: "PostHog",
		Description: "Product analytics and feature flags",
		Icon:        "bar-chart",
		AuthType:    AuthAPIKey,
		Scope:       ScopeShared,
	},
	Web: {
		Name:        Web,
		DisplayName: "Web",
		Description: "Crawl websites into markdown files",
		Icon:        "globe",
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
