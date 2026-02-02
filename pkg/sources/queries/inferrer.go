// Package queries provides smart query inference and execution for integration sources.
//
// Query inference converts folder/file names to source-specific search queries.
// For example: "unread-emails" → Gmail query "is:unread"
//
// Inference happens on the gateway side via BAML. The client just calls gRPC.
package queries

// GmailQuerySpec is the output format for Gmail queries.
type GmailQuerySpec struct {
	Query          string `json:"gmail_query"`
	Limit          int    `json:"limit"`
	FilenameFormat string `json:"filename_format"`
}

// GDriveQuerySpec is the output format for Google Drive queries.
type GDriveQuerySpec struct {
	Query          string `json:"gdrive_query"`
	Limit          int    `json:"limit"`
	FilenameFormat string `json:"filename_format"`
}

// NotionQuerySpec is the output format for Notion queries.
type NotionQuerySpec struct {
	Query          string `json:"notion_query"`
	Limit          int    `json:"limit"`
	FilenameFormat string `json:"filename_format"`
}

// GitHubQuerySpec is the output format for GitHub queries.
type GitHubQuerySpec struct {
	Query          string `json:"github_query"`
	SearchType     string `json:"search_type"`  // "repos", "issues", or "prs"
	ContentType    string `json:"content_type"` // "diff", "markdown", or "json"
	Limit          int    `json:"limit"`
	FilenameFormat string `json:"filename_format"`
}

// SlackQuerySpec is the output format for Slack queries.
type SlackQuerySpec struct {
	Query          string `json:"slack_query"`
	Limit          int    `json:"limit"`
	FilenameFormat string `json:"filename_format"`
}

// LinearQuerySpec is the output format for Linear queries.
type LinearQuerySpec struct {
	Query          string `json:"linear_query"`
	SearchType     string `json:"search_type"` // "issues" or "projects"
	Limit          int    `json:"limit"`
	FilenameFormat string `json:"filename_format"`
}
