package types

// ToolName is a type-safe tool identifier
type ToolName string

// Tool name constants - add new tools here
const (
	ToolWikipedia ToolName = "wikipedia"
	ToolWeather   ToolName = "weather"
	ToolExa       ToolName = "exa"
	ToolGitHub    ToolName = "github"
	ToolGmail     ToolName = "gmail"
	ToolNotion    ToolName = "notion"
	ToolGDrive    ToolName = "gdrive"
	ToolSlack     ToolName = "slack"
	ToolLinear    ToolName = "linear"
)

// String returns the string representation
func (t ToolName) String() string {
	return string(t)
}
