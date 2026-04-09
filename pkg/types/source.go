package types

// SourceType identifies an integration data source.
type SourceType string

const (
	SourceGmail   SourceType = "gmail"
	SourceGitHub  SourceType = "github"
	SourceNotion  SourceType = "notion"
	SourceGDrive  SourceType = "gdrive"
	SourceSlack   SourceType = "slack"
	SourceTeams      SourceType = "teams"
	SourceLinear  SourceType = "linear"
	SourcePostHog    SourceType = "posthog"
	SourceConfluence SourceType = "confluence"
	SourceOutlook    SourceType = "outlook"
	SourceWeb        SourceType = "web"
)

func (s SourceType) String() string { return string(s) }
