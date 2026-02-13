package types

// SourceType identifies an integration data source.
type SourceType string

const (
	SourceGmail   SourceType = "gmail"
	SourceGitHub  SourceType = "github"
	SourceNotion  SourceType = "notion"
	SourceGDrive  SourceType = "gdrive"
	SourceSlack   SourceType = "slack"
	SourceLinear  SourceType = "linear"
	SourcePostHog SourceType = "posthog"
)

func (s SourceType) String() string { return string(s) }
