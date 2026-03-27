package types

// IntegrationName identifies an integration (tool or source).
type IntegrationName string

const (
	Wikipedia IntegrationName = "wikipedia"
	Weather   IntegrationName = "weather"
	Exa       IntegrationName = "exa"
	GitHub    IntegrationName = "github"
	Gmail     IntegrationName = "gmail"
	Notion    IntegrationName = "notion"
	GDrive    IntegrationName = "gdrive"
	Slack     IntegrationName = "slack"
	Linear    IntegrationName = "linear"
	PostHog     IntegrationName = "posthog"
	Confluence  IntegrationName = "confluence"
	Web         IntegrationName = "web"
	Browser     IntegrationName = "browser"
	ViewTool    IntegrationName = "view"
)

func (n IntegrationName) String() string { return string(n) }
