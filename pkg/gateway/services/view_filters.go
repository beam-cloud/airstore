package services

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

// Per-integration filter structs. Each integration gets a typed filter that
// the backend converts to a QuerySpec JSON string. The filter is what the
// UI form builds and what SDK users pass.

type GmailFilter struct {
	From               string `json:"from"`
	To                 string `json:"to"`
	Subject            string `json:"subject"`
	Label              string `json:"label"`
	Filename           string `json:"filename"`
	NewerThan          string `json:"newer_than"`
	OlderThan          string `json:"older_than"`
	HasAttachment      *bool  `json:"has_attachment"`
	IsUnread           *bool  `json:"is_unread"`
	IsStarred          *bool  `json:"is_starred"`
	IncludeAttachments *bool  `json:"include_attachments"`
	IncludeInline      *bool  `json:"include_inline"`
	IncludeMessageBody *bool  `json:"include_message_body"`
}

type GitHubFilter struct {
	Repo        string `json:"repo"`
	Type        string `json:"type"`
	State       string `json:"state"`
	Label       string `json:"label"`
	Author      string `json:"author"`
	ContentType string `json:"content_type"`
}

type GDriveFilter struct {
	NameContains   string `json:"name_contains"`
	MimeType       string `json:"mime_type"`
	SharedWithMe   *bool  `json:"shared_with_me"`
	Starred        *bool  `json:"starred"`
	ModifiedAfter  string `json:"modified_after"`
	ModifiedBefore string `json:"modified_before"`
	FolderID       string `json:"folder_id"`
}

type NotionFilter struct {
	Search string `json:"search"`
}

type SlackFilter struct {
	Channel     string `json:"channel"`
	From        string `json:"from"`
	After       string `json:"after"`
	Before      string `json:"before"`
	HasLink     *bool  `json:"has_link"`
	HasReaction *bool  `json:"has_reaction"`
}

type LinearFilter struct {
	Type     string `json:"type"`
	Team     string `json:"team"`
	State    string `json:"state"`
	Assignee string `json:"assignee"`
	Priority string `json:"priority"`
	Label    string `json:"label"`
	Project  string `json:"project"`
}

type PostHogFilter struct {
	Type      string `json:"type"`
	Query     string `json:"query"`
	ProjectID int    `json:"project_id"`
}

type ConfluenceFilter struct {
	CQL         string `json:"cql"`
	Space       string `json:"space"`
	ContentType string `json:"content_type"` // "page", "blogpost", or "all"
	Label       string `json:"label"`
}

type AgentMailFilter struct {
	Inbox   string `json:"inbox"`
	From    string `json:"from"`
	Subject string `json:"subject"`
}

type WebFilter struct {
	Mode         string   `json:"mode"` // "map" or "search"
	URL          string   `json:"url"`
	Query        string   `json:"query"`
	IncludePaths []string `json:"include_paths"`
}

// buildQuerySpecFromFilter converts a structured per-integration filter into
// the same JSON shape that parseQuerySpec() already consumes. This is the
// bridge between the new "query mode" API and the existing execution engine.
func buildQuerySpecFromFilter(integration string, filter json.RawMessage, limit int) (string, error) {
	if limit <= 0 {
		limit = defaultPageSize
	}

	switch types.SourceType(integration) {
	case types.SourceGmail:
		return buildGmailFilter(filter, limit)
	case types.SourceGitHub:
		return buildGitHubFilter(filter, limit)
	case types.SourceGDrive:
		return buildGDriveFilter(filter, limit)
	case types.SourceNotion:
		return buildNotionFilter(filter, limit)
	case types.SourceSlack:
		return buildSlackFilter(filter, limit)
	case types.SourceLinear:
		return buildLinearFilter(filter, limit)
	case types.SourcePostHog:
		return buildPostHogFilter(filter, limit)
	case types.SourceConfluence:
		return buildConfluenceFilter(filter, limit)
	case types.SourceWeb:
		return buildWebFilter(filter, limit)
	case types.SourceAgentMail:
		return buildAgentMailFilter(filter, limit)
	default:
		return "", fmt.Errorf("unsupported integration for filter: %s", integration)
	}
}

// queryKey returns the JSON key for a given integration's query string.
func queryKey(integration string) string {
	return integration + "_query"
}

// newSpec creates a base query spec with integration key, query, and limit.
// Providers determine filename_format from metadata (e.g., content_type) at
// execution time via DefaultFilenameFormat — filter builders never set it.
func newSpec(integration, query string, limit int) map[string]any {
	return map[string]any{
		queryKey(integration): query,
		"limit":               limit,
	}
}

func buildGmailFilter(raw json.RawMessage, limit int) (string, error) {
	var f GmailFilter
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", err
	}
	var parts []string
	if f.From != "" {
		parts = append(parts, "from:"+quoteIfNeeded(f.From))
	}
	if f.To != "" {
		parts = append(parts, "to:"+quoteIfNeeded(f.To))
	}
	if f.Subject != "" {
		parts = append(parts, "subject:"+quoteIfNeeded(f.Subject))
	}
	if f.Label != "" {
		parts = append(parts, "label:"+f.Label)
	}
	if f.Filename != "" {
		parts = append(parts, "filename:"+quoteIfNeeded(f.Filename))
	}
	if f.NewerThan != "" {
		parts = append(parts, "newer_than:"+f.NewerThan)
	}
	if f.OlderThan != "" {
		parts = append(parts, "older_than:"+f.OlderThan)
	}
	if f.HasAttachment != nil && *f.HasAttachment {
		parts = append(parts, "has:attachment")
	}
	if f.IsUnread != nil && *f.IsUnread {
		parts = append(parts, "is:unread")
	}
	if f.IsStarred != nil && *f.IsStarred {
		parts = append(parts, "is:starred")
	}

	spec := newSpec("gmail", strings.Join(parts, " "), limit)

	if f.IncludeAttachments != nil {
		spec["include_attachments"] = *f.IncludeAttachments
	} else if f.HasAttachment != nil && *f.HasAttachment {
		// If the user asks for has_attachment, include attachment files by default.
		spec["include_attachments"] = true
	}
	if f.IncludeInline != nil {
		spec["include_inline"] = *f.IncludeInline
	}
	if f.IncludeMessageBody != nil {
		spec["include_message_body"] = *f.IncludeMessageBody
	}

	return marshalSpec(spec)
}

func buildGitHubFilter(raw json.RawMessage, limit int) (string, error) {
	var f GitHubFilter
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", err
	}
	if f.Repo == "" {
		return "", fmt.Errorf("repo is required for GitHub filter")
	}
	var parts []string
	parts = append(parts, "repo:"+f.Repo)
	if f.State != "" {
		parts = append(parts, "is:"+f.State)
	}
	if f.Label != "" {
		parts = append(parts, "label:"+f.Label)
	}
	if f.Author != "" {
		parts = append(parts, "author:"+f.Author)
	}
	spec := newSpec("github", strings.Join(parts, " "), limit)
	if f.Type != "" {
		spec["search_type"] = f.Type
	}
	if f.ContentType != "" {
		spec["content_type"] = f.ContentType
	}
	return marshalSpec(spec)
}

func buildGDriveFilter(raw json.RawMessage, limit int) (string, error) {
	var f GDriveFilter
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", err
	}
	var parts []string
	if f.NameContains != "" {
		parts = append(parts, fmt.Sprintf("name contains '%s'", escDriveQuote(f.NameContains)))
	}
	if f.MimeType != "" {
		mimeMap := map[string]string{
			"pdf":          "application/pdf",
			"document":     "application/vnd.google-apps.document",
			"spreadsheet":  "application/vnd.google-apps.spreadsheet",
			"presentation": "application/vnd.google-apps.presentation",
		}
		if m, ok := mimeMap[f.MimeType]; ok {
			parts = append(parts, fmt.Sprintf("mimeType = '%s'", m))
		} else {
			parts = append(parts, fmt.Sprintf("mimeType = '%s'", escDriveQuote(f.MimeType)))
		}
	}
	if f.SharedWithMe != nil && *f.SharedWithMe {
		parts = append(parts, "sharedWithMe = true")
	}
	if f.Starred != nil && *f.Starred {
		parts = append(parts, "starred = true")
	}
	if f.ModifiedAfter != "" {
		parts = append(parts, fmt.Sprintf("modifiedTime > '%sT00:00:00'", f.ModifiedAfter))
	}
	if f.ModifiedBefore != "" {
		parts = append(parts, fmt.Sprintf("modifiedTime < '%sT00:00:00'", f.ModifiedBefore))
	}
	if f.FolderID != "" {
		parts = append(parts, fmt.Sprintf("'%s' in parents", escDriveQuote(f.FolderID)))
	}
	return marshalSpec(newSpec("gdrive", strings.Join(parts, " and "), limit))
}

func buildNotionFilter(raw json.RawMessage, limit int) (string, error) {
	var f NotionFilter
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", err
	}
	return marshalSpec(newSpec("notion", f.Search, limit))
}

func buildSlackFilter(raw json.RawMessage, limit int) (string, error) {
	var f SlackFilter
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", err
	}
	var parts []string
	if f.Channel != "" {
		parts = append(parts, "in:"+f.Channel)
	}
	if f.From != "" {
		parts = append(parts, "from:"+f.From)
	}
	if f.After != "" {
		parts = append(parts, "after:"+f.After)
	}
	if f.Before != "" {
		parts = append(parts, "before:"+f.Before)
	}
	if f.HasLink != nil && *f.HasLink {
		parts = append(parts, "has:link")
	}
	if f.HasReaction != nil && *f.HasReaction {
		parts = append(parts, "has:reaction")
	}
	return marshalSpec(newSpec("slack", strings.Join(parts, " "), limit))
}

func buildLinearFilter(raw json.RawMessage, limit int) (string, error) {
	var f LinearFilter
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", err
	}
	var parts []string
	if f.Team != "" {
		parts = append(parts, "team:"+quoteIfNeeded(f.Team))
	}
	if f.State != "" {
		parts = append(parts, "state:"+quoteIfNeeded(f.State))
	}
	if f.Assignee != "" {
		parts = append(parts, "assignee:"+quoteIfNeeded(f.Assignee))
	}
	if f.Priority != "" {
		priorityMap := map[string]string{
			"urgent": "1", "high": "2", "medium": "3", "low": "4",
		}
		if num, ok := priorityMap[strings.ToLower(f.Priority)]; ok {
			parts = append(parts, "priority:"+num)
		} else {
			parts = append(parts, "priority:"+quoteIfNeeded(f.Priority))
		}
	}
	if f.Label != "" {
		parts = append(parts, "label:"+quoteIfNeeded(f.Label))
	}
	if f.Project != "" {
		parts = append(parts, "project:"+quoteIfNeeded(f.Project))
	}
	spec := newSpec("linear", strings.Join(parts, " "), limit)
	if f.Type != "" {
		spec["search_type"] = f.Type
	}
	return marshalSpec(spec)
}

func buildPostHogFilter(raw json.RawMessage, limit int) (string, error) {
	var f PostHogFilter
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", err
	}
	spec := newSpec("posthog", f.Query, limit)
	if f.Type != "" {
		spec["search_type"] = f.Type
	}
	if f.ProjectID > 0 {
		spec["project_id"] = f.ProjectID
	}
	return marshalSpec(spec)
}

func buildConfluenceFilter(raw json.RawMessage, limit int) (string, error) {
	var f ConfluenceFilter
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", err
	}
	// If a raw CQL is provided, use it directly.
	if f.CQL != "" {
		spec := map[string]any{
			"cql_query": f.CQL,
			"limit":     limit,
		}
		if f.ContentType != "" && f.ContentType != "all" {
			spec["content_type"] = f.ContentType
		}
		return marshalSpec(spec)
	}
	// Build CQL from structured fields.
	var parts []string
	if f.Space != "" {
		parts = append(parts, "space="+f.Space)
	}
	if f.ContentType != "" && f.ContentType != "all" {
		parts = append(parts, "type="+f.ContentType)
	}
	if f.Label != "" {
		parts = append(parts, fmt.Sprintf("label=%q", f.Label))
	}
	cql := strings.Join(parts, " AND ")
	if cql != "" {
		cql += " ORDER BY lastModified DESC"
	}
	spec := map[string]any{
		"cql_query": cql,
		"limit":     limit,
	}
	return marshalSpec(spec)
}

func buildAgentMailFilter(raw json.RawMessage, limit int) (string, error) {
	var f AgentMailFilter
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", err
	}
	var parts []string
	if f.From != "" {
		parts = append(parts, f.From)
	}
	if f.Subject != "" {
		parts = append(parts, f.Subject)
	}
	spec := newSpec("agentmail", strings.Join(parts, " "), limit)
	if f.Inbox != "" {
		spec["inbox_filter"] = f.Inbox
	}
	return marshalSpec(spec)
}

func buildWebFilter(raw json.RawMessage, limit int) (string, error) {
	var f WebFilter
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", err
	}
	mode := f.Mode
	if mode == "" {
		mode = "map"
	}
	query := f.URL
	if mode == "search" {
		query = f.Query
	}
	spec := newSpec("web", query, limit)
	spec["web_mode"] = mode
	if len(f.IncludePaths) > 0 {
		spec["include_paths"] = f.IncludePaths
	}
	return marshalSpec(spec)
}

// quoteIfNeeded wraps a value in double quotes if it contains spaces.
func quoteIfNeeded(s string) string {
	if strings.Contains(s, " ") {
		return `"` + s + `"`
	}
	return s
}

func marshalSpec(spec map[string]any) (string, error) {
	data, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// escDriveQuote escapes single quotes in values interpolated into Google Drive
// query strings (e.g. name contains 'O\'Reilly').
func escDriveQuote(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}
