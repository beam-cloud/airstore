package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
)

const linearAPIBase = "https://api.linear.app/graphql"

// LinearProvider implements sources.Provider for Linear integration.
// Primary usage is via smart queries: mkdir /sources/linear/my-assigned-issues
type LinearProvider struct {
	httpClient *http.Client
}

func NewLinearProvider() *LinearProvider {
	return &LinearProvider{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (l *LinearProvider) Name() string { return types.ToolLinear.String() }

func (l *LinearProvider) checkAuth(pctx *sources.ProviderContext) error {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return sources.ErrNotConnected
	}
	return nil
}

// Provider interface implementation

func (l *LinearProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if err := l.checkAuth(pctx); err != nil {
		return nil, err
	}
	if path == "" {
		return sources.DirInfo(), nil
	}
	return nil, sources.ErrNotFound
}

func (l *LinearProvider) ReadDir(ctx context.Context, pctx *sources.ProviderContext, path string) ([]sources.DirEntry, error) {
	if err := l.checkAuth(pctx); err != nil {
		return nil, err
	}
	if path == "" {
		return []sources.DirEntry{}, nil
	}
	return nil, sources.ErrNotFound
}

func (l *LinearProvider) Read(ctx context.Context, pctx *sources.ProviderContext, path string, offset, length int64) ([]byte, error) {
	if err := l.checkAuth(pctx); err != nil {
		return nil, err
	}
	return nil, sources.ErrNotFound
}

func (l *LinearProvider) Readlink(ctx context.Context, pctx *sources.ProviderContext, path string) (string, error) {
	return "", sources.ErrNotFound
}

func (l *LinearProvider) Search(ctx context.Context, pctx *sources.ProviderContext, query string, limit int) ([]sources.SearchResult, error) {
	if err := l.checkAuth(pctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}

	issues, _, err := l.searchIssues(ctx, pctx.Credentials.AccessToken, query, limit, "")
	if err != nil {
		return nil, err
	}

	results := make([]sources.SearchResult, 0, len(issues))
	for _, issue := range issues {
		id := linearGetStr(issue, "id")
		identifier := linearGetStr(issue, "identifier")
		title := linearGetStr(issue, "title")
		mtime := linearParseTime(linearGetStr(issue, "updatedAt"))

		safeTitle := sources.SanitizeFilename(title)
		if len(safeTitle) > 50 {
			safeTitle = safeTitle[:50]
		}

		results = append(results, sources.SearchResult{
			Name:    fmt.Sprintf("%s_%s.md", identifier, safeTitle),
			Id:      id,
			Mode:    sources.ModeFile,
			Size:    0,
			Mtime:   mtime,
			Preview: fmt.Sprintf("%s: %s", identifier, title),
		})
	}
	return results, nil
}

// QueryExecutor interface implementation

func (l *LinearProvider) ExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	if err := l.checkAuth(pctx); err != nil {
		return nil, err
	}

	limit := linearClamp(spec.Limit, 1, 100, 50)
	token := pctx.Credentials.AccessToken
	searchType := linearGetMeta(spec.Metadata, "search_type", "issues")

	var results []sources.QueryResult
	var nextCursor string

	switch searchType {
	case "projects":
		projects, cursor, err := l.searchProjects(ctx, token, spec.Query, limit, spec.PageToken)
		if err != nil {
			return nil, err
		}
		results = l.projectsToResults(projects, spec.FilenameFormat)
		nextCursor = cursor
	default:
		issues, cursor, err := l.searchIssues(ctx, token, spec.Query, limit, spec.PageToken)
		if err != nil {
			return nil, err
		}
		results = l.issuesToResults(issues, spec.FilenameFormat)
		nextCursor = cursor
	}

	return &sources.QueryResponse{
		Results:       results,
		NextPageToken: nextCursor,
		HasMore:       nextCursor != "",
	}, nil
}

func (l *LinearProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if err := l.checkAuth(pctx); err != nil {
		return nil, err
	}

	issue, err := l.fetchIssue(ctx, pctx.Credentials.AccessToken, resultID)
	if err != nil {
		return nil, err
	}
	return formatIssueMarkdown(issue), nil
}

func (l *LinearProvider) FormatFilename(format string, metadata map[string]string) string {
	if format == "" {
		format = "{identifier}_{title}.md"
	}

	result := format
	for key, value := range metadata {
		safeValue := sources.SanitizeFilename(value)
		if key != "id" && key != "identifier" && len(safeValue) > 40 {
			safeValue = safeValue[:40]
		}
		result = strings.ReplaceAll(result, "{"+key+"}", safeValue)
	}

	if !strings.Contains(result, ".") {
		result += ".md"
	}
	if result == "" || result == ".md" {
		if id := metadata["identifier"]; id != "" {
			return id + ".md"
		}
		if id := metadata["id"]; id != "" {
			if len(id) > 8 {
				id = id[:8]
			}
			return id + ".md"
		}
		return "issue.md"
	}
	return result
}

var _ sources.Provider = (*LinearProvider)(nil)
var _ sources.QueryExecutor = (*LinearProvider)(nil)

// Result converters

func (l *LinearProvider) issuesToResults(issues []map[string]any, filenameFormat string) []sources.QueryResult {
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat("linear")
	}

	results := make([]sources.QueryResult, 0, len(issues))
	for _, issue := range issues {
		updatedAt := linearGetStr(issue, "updatedAt")
		createdAt := linearGetStr(issue, "createdAt")

		metadata := map[string]string{
			"id":         linearGetStr(issue, "id"),
			"identifier": linearGetStr(issue, "identifier"),
			"title":      linearGetStr(issue, "title"),
			"state":      linearGetNested(issue, "state", "name"),
			"assignee":   linearGetNested(issue, "assignee", "name"),
			"team":       linearGetNested(issue, "team", "key"),
			"priority":   fmt.Sprintf("%d", int(linearGetFloat(issue, "priority"))),
			"date":       linearFmtDate(updatedAt),
			"created":    linearFmtDate(createdAt),
		}

		results = append(results, sources.QueryResult{
			ID:       metadata["id"],
			Filename: l.FormatFilename(filenameFormat, metadata),
			Metadata: metadata,
			Size:     0,
			Mtime:    linearParseTime(updatedAt),
		})
	}
	return results
}

func (l *LinearProvider) projectsToResults(projects []map[string]any, filenameFormat string) []sources.QueryResult {
	if filenameFormat == "" {
		filenameFormat = "{name}_{id}.md"
	}

	results := make([]sources.QueryResult, 0, len(projects))
	for _, project := range projects {
		updatedAt := linearGetStr(project, "updatedAt")
		metadata := map[string]string{
			"id":    linearGetStr(project, "id"),
			"name":  linearGetStr(project, "name"),
			"state": linearGetStr(project, "state"),
			"date":  linearFmtDate(updatedAt),
		}

		results = append(results, sources.QueryResult{
			ID:       metadata["id"],
			Filename: l.FormatFilename(filenameFormat, metadata),
			Metadata: metadata,
			Size:     0,
			Mtime:    linearParseTime(updatedAt),
		})
	}
	return results
}

// GraphQL API methods

func (l *LinearProvider) searchIssues(ctx context.Context, token, query string, limit int, cursor string) ([]map[string]any, string, error) {
	const gql = `query($filter: IssueFilter, $first: Int, $after: String) {
		issues(filter: $filter, first: $first, after: $after, orderBy: updatedAt) {
			pageInfo { hasNextPage endCursor }
			nodes {
				id identifier title description priority createdAt updatedAt
				state { id name type }
				assignee { id name email }
				team { id key name }
				labels { nodes { id name } }
				comments { nodes { id body createdAt user { name } } }
			}
		}
	}`

	vars := map[string]any{"filter": l.parseQueryToFilter(query), "first": limit}
	if cursor != "" {
		vars["after"] = cursor
	}

	var resp struct {
		Data struct {
			Issues struct {
				PageInfo struct {
					HasNextPage bool   `json:"hasNextPage"`
					EndCursor   string `json:"endCursor"`
				} `json:"pageInfo"`
				Nodes []map[string]any `json:"nodes"`
			} `json:"issues"`
		} `json:"data"`
		Errors []graphqlError `json:"errors"`
	}

	if err := l.graphql(ctx, token, gql, vars, &resp); err != nil {
		return nil, "", err
	}
	if len(resp.Errors) > 0 {
		return nil, "", fmt.Errorf("linear API: %s", resp.Errors[0].Message)
	}

	nextCursor := ""
	if resp.Data.Issues.PageInfo.HasNextPage {
		nextCursor = resp.Data.Issues.PageInfo.EndCursor
	}
	return resp.Data.Issues.Nodes, nextCursor, nil
}

func (l *LinearProvider) searchProjects(ctx context.Context, token, query string, limit int, cursor string) ([]map[string]any, string, error) {
	const gql = `query($first: Int, $after: String) {
		projects(first: $first, after: $after, orderBy: updatedAt) {
			pageInfo { hasNextPage endCursor }
			nodes { id name description state createdAt updatedAt teams { nodes { key name } } }
		}
	}`

	vars := map[string]any{"first": limit}
	if cursor != "" {
		vars["after"] = cursor
	}

	var resp struct {
		Data struct {
			Projects struct {
				PageInfo struct {
					HasNextPage bool   `json:"hasNextPage"`
					EndCursor   string `json:"endCursor"`
				} `json:"pageInfo"`
				Nodes []map[string]any `json:"nodes"`
			} `json:"projects"`
		} `json:"data"`
		Errors []graphqlError `json:"errors"`
	}

	if err := l.graphql(ctx, token, gql, vars, &resp); err != nil {
		return nil, "", err
	}
	if len(resp.Errors) > 0 {
		return nil, "", fmt.Errorf("linear API: %s", resp.Errors[0].Message)
	}

	nextCursor := ""
	if resp.Data.Projects.PageInfo.HasNextPage {
		nextCursor = resp.Data.Projects.PageInfo.EndCursor
	}
	return resp.Data.Projects.Nodes, nextCursor, nil
}

func (l *LinearProvider) fetchIssue(ctx context.Context, token, id string) (map[string]any, error) {
	const gql = `query($id: String!) {
		issue(id: $id) {
			id identifier title description priority estimate createdAt updatedAt url
			state { name type }
			assignee { name email }
			creator { name }
			team { key name }
			project { name }
			labels { nodes { name } }
			comments { nodes { body createdAt user { name } } }
			parent { identifier title }
			children { nodes { identifier title state { name } } }
		}
	}`

	var resp struct {
		Data struct {
			Issue map[string]any `json:"issue"`
		} `json:"data"`
		Errors []graphqlError `json:"errors"`
	}

	if err := l.graphql(ctx, token, gql, map[string]any{"id": id}, &resp); err != nil {
		return nil, err
	}
	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("linear API: %s", resp.Errors[0].Message)
	}
	if resp.Data.Issue == nil {
		return nil, sources.ErrNotFound
	}
	return resp.Data.Issue, nil
}

func (l *LinearProvider) parseQueryToFilter(query string) map[string]any {
	// Tokenize the query, respecting quotes and OR
	tokens := linearTokenize(query)
	
	// Split by OR to handle alternatives
	orGroups := linearSplitOR(tokens)
	
	if len(orGroups) > 1 {
		// Multiple OR groups - each group becomes an AND, combined with OR
		var orFilters []map[string]any
		for _, group := range orGroups {
			groupFilter := l.parseTokenGroup(group)
			if len(groupFilter) > 0 {
				orFilters = append(orFilters, groupFilter)
			}
		}
		if len(orFilters) > 1 {
			return map[string]any{"or": orFilters}
		} else if len(orFilters) == 1 {
			return orFilters[0]
		}
		return make(map[string]any)
	}
	
	// Single group - AND logic between filters
	return l.parseTokenGroup(tokens)
}

// parseTokenGroupFlat returns individual filters as a slice (for OR combining)
func (l *LinearProvider) parseTokenGroupFlat(tokens []string) []map[string]any {
	var filters []map[string]any
	var searchTerms []string

	for _, token := range tokens {
		switch {
		case strings.HasPrefix(token, "assignee:"):
			value := strings.TrimPrefix(token, "assignee:")
			if value == "me" {
				filters = append(filters, map[string]any{"assignee": map[string]any{"isMe": map[string]any{"eq": true}}})
			} else {
				filters = append(filters, map[string]any{"assignee": map[string]any{"name": map[string]any{"containsIgnoreCase": value}}})
			}
		case strings.HasPrefix(token, "creator:"):
			value := strings.TrimPrefix(token, "creator:")
			filters = append(filters, map[string]any{"creator": map[string]any{"name": map[string]any{"containsIgnoreCase": value}}})
		case strings.HasPrefix(token, "state:"):
			value := l.normalizeState(strings.TrimPrefix(token, "state:"))
			filters = append(filters, map[string]any{"state": map[string]any{"name": map[string]any{"containsIgnoreCase": value}}})
		case strings.HasPrefix(token, "team:"):
			filters = append(filters, map[string]any{"team": map[string]any{"key": map[string]any{"eqIgnoreCase": strings.TrimPrefix(token, "team:")}}})
		case strings.HasPrefix(token, "priority:"):
			var p int
			fmt.Sscanf(strings.TrimPrefix(token, "priority:"), "%d", &p)
			filters = append(filters, map[string]any{"priority": map[string]any{"eq": p}})
		case strings.HasPrefix(token, "label:"):
			filters = append(filters, map[string]any{"labels": map[string]any{"name": map[string]any{"containsIgnoreCase": strings.TrimPrefix(token, "label:")}}})
		case token == "is:bug":
			filters = append(filters, map[string]any{"labels": map[string]any{"name": map[string]any{"containsIgnoreCase": "bug"}}})
		case token == "is:feature":
			filters = append(filters, map[string]any{"labels": map[string]any{"name": map[string]any{"containsIgnoreCase": "feature"}}})
		default:
			if token != "" {
				searchTerms = append(searchTerms, token)
			}
		}
	}

	// Add text search filters
	if len(searchTerms) > 0 {
		text := strings.Join(searchTerms, " ")
		filters = append(filters, map[string]any{"title": map[string]any{"containsIgnoreCase": text}})
		filters = append(filters, map[string]any{"description": map[string]any{"containsIgnoreCase": text}})
	}
	return filters
}

func (l *LinearProvider) normalizeState(value string) string {
	switch strings.ToLower(value) {
	case "in progress", "in-progress", "inprogress", "doing":
		return "In Progress"
	case "todo", "to do", "to-do":
		return "Todo"
	case "done", "completed", "complete":
		return "Done"
	case "backlog":
		return "Backlog"
	case "canceled", "cancelled":
		return "Canceled"
	}
	return value
}

// linearTokenize splits a query into tokens, respecting quoted strings
func linearTokenize(query string) []string {
	var tokens []string
	var current strings.Builder
	inQuotes := false
	
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case c == '"':
			inQuotes = !inQuotes
			// Don't include the quote in the token
		case c == ' ' && !inQuotes:
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(c)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// linearSplitOR splits tokens by OR keyword
func linearSplitOR(tokens []string) [][]string {
	var groups [][]string
	var current []string
	
	for _, t := range tokens {
		if strings.EqualFold(t, "OR") {
			if len(current) > 0 {
				groups = append(groups, current)
				current = nil
			}
		} else {
			current = append(current, t)
		}
	}
	if len(current) > 0 {
		groups = append(groups, current)
	}
	return groups
}

// parseTokenGroup parses a group of tokens into a Linear filter (AND logic within group)
func (l *LinearProvider) parseTokenGroup(tokens []string) map[string]any {
	filter := make(map[string]any)
	var searchTerms []string

	for _, token := range tokens {
		switch {
		case strings.HasPrefix(token, "assignee:"):
			value := strings.TrimPrefix(token, "assignee:")
			if value == "me" {
				filter["assignee"] = map[string]any{"isMe": map[string]any{"eq": true}}
			} else {
				filter["assignee"] = map[string]any{"name": map[string]any{"containsIgnoreCase": value}}
			}
		case strings.HasPrefix(token, "creator:"):
			value := strings.TrimPrefix(token, "creator:")
			filter["creator"] = map[string]any{"name": map[string]any{"containsIgnoreCase": value}}
		case strings.HasPrefix(token, "state:"):
			value := l.normalizeState(strings.TrimPrefix(token, "state:"))
			filter["state"] = map[string]any{"name": map[string]any{"containsIgnoreCase": value}}
		case strings.HasPrefix(token, "team:"):
			filter["team"] = map[string]any{"key": map[string]any{"eqIgnoreCase": strings.TrimPrefix(token, "team:")}}
		case strings.HasPrefix(token, "priority:"):
			var p int
			fmt.Sscanf(strings.TrimPrefix(token, "priority:"), "%d", &p)
			filter["priority"] = map[string]any{"eq": p}
		case strings.HasPrefix(token, "label:"):
			filter["labels"] = map[string]any{"name": map[string]any{"containsIgnoreCase": strings.TrimPrefix(token, "label:")}}
		case token == "is:bug":
			filter["labels"] = map[string]any{"name": map[string]any{"containsIgnoreCase": "bug"}}
		case token == "is:feature":
			filter["labels"] = map[string]any{"name": map[string]any{"containsIgnoreCase": "feature"}}
		default:
			if token != "" {
				searchTerms = append(searchTerms, token)
			}
		}
	}

	// Add text search
	if len(searchTerms) > 0 {
		text := strings.Join(searchTerms, " ")
		textFilter := []map[string]any{
			{"title": map[string]any{"containsIgnoreCase": text}},
			{"description": map[string]any{"containsIgnoreCase": text}},
		}
		
		if len(filter) == 0 {
			filter["or"] = textFilter
		} else {
			var andFilters []map[string]any
			for k, v := range filter {
				andFilters = append(andFilters, map[string]any{k: v})
			}
			andFilters = append(andFilters, map[string]any{"or": textFilter})
			return map[string]any{"and": andFilters}
		}
	}
	return filter
}

type graphqlError struct {
	Message string `json:"message"`
}

func (l *LinearProvider) graphql(ctx context.Context, token, query string, vars map[string]any, result any) error {
	body, err := json.Marshal(map[string]any{"query": query, "variables": vars})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", linearAPIBase, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := l.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("linear API %s: %s", resp.Status, string(b))
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

// Markdown formatting

func formatIssueMarkdown(issue map[string]any) []byte {
	var b strings.Builder

	identifier := linearGetStr(issue, "identifier")
	title := linearGetStr(issue, "title")
	description := linearGetStr(issue, "description")
	url := linearGetStr(issue, "url")
	priority := int(linearGetFloat(issue, "priority"))
	createdAt := linearGetStr(issue, "createdAt")
	updatedAt := linearGetStr(issue, "updatedAt")

	stateName := linearGetNested(issue, "state", "name")
	assigneeName := linearGetNested(issue, "assignee", "name")
	creatorName := linearGetNested(issue, "creator", "name")
	teamName := linearGetNested(issue, "team", "name")
	projectName := linearGetNested(issue, "project", "name")
	labels := linearGetLabels(issue)

	// Title
	fmt.Fprintf(&b, "# %s: %s\n\n", identifier, title)

	// Metadata table
	b.WriteString("| Field | Value |\n|-------|-------|\n")
	fmt.Fprintf(&b, "| Status | %s |\n", stateName)
	if assigneeName != "" {
		fmt.Fprintf(&b, "| Assignee | %s |\n", assigneeName)
	}
	if teamName != "" {
		fmt.Fprintf(&b, "| Team | %s |\n", teamName)
	}
	if projectName != "" {
		fmt.Fprintf(&b, "| Project | %s |\n", projectName)
	}
	if priority > 0 && priority <= 4 {
		names := []string{"", "Urgent", "High", "Medium", "Low"}
		fmt.Fprintf(&b, "| Priority | %s |\n", names[priority])
	}
	if len(labels) > 0 {
		fmt.Fprintf(&b, "| Labels | %s |\n", strings.Join(labels, ", "))
	}
	if creatorName != "" {
		fmt.Fprintf(&b, "| Created by | %s |\n", creatorName)
	}
	if t := linearFmtDateTime(createdAt); t != "" {
		fmt.Fprintf(&b, "| Created | %s |\n", t)
	}
	if t := linearFmtDateTime(updatedAt); t != "" {
		fmt.Fprintf(&b, "| Updated | %s |\n", t)
	}
	if url != "" {
		fmt.Fprintf(&b, "| URL | %s |\n", url)
	}
	b.WriteString("\n")

	// Description
	if description != "" {
		fmt.Fprintf(&b, "## Description\n\n%s\n\n", description)
	}

	// Parent issue
	if parent, ok := issue["parent"].(map[string]any); ok {
		if pid := linearGetStr(parent, "identifier"); pid != "" {
			fmt.Fprintf(&b, "## Parent Issue\n\n- %s: %s\n\n", pid, linearGetStr(parent, "title"))
		}
	}

	// Sub-issues
	if children := linearGetNodes(issue, "children"); len(children) > 0 {
		b.WriteString("## Sub-issues\n\n")
		for _, child := range children {
			cid := linearGetStr(child, "identifier")
			ctitle := linearGetStr(child, "title")
			cstate := linearGetNested(child, "state", "name")
			fmt.Fprintf(&b, "- %s: %s [%s]\n", cid, ctitle, cstate)
		}
		b.WriteString("\n")
	}

	// Comments
	if comments := linearGetNodes(issue, "comments"); len(comments) > 0 {
		b.WriteString("## Comments\n\n")
		for i, c := range comments {
			body := linearGetStr(c, "body")
			user := linearGetNested(c, "user", "name")
			if user == "" {
				user = "Unknown"
			}
			ts := linearFmtDateTime(linearGetStr(c, "createdAt"))
			fmt.Fprintf(&b, "### Comment %d - %s (%s)\n\n%s\n\n", i+1, user, ts, body)
		}
	}

	return []byte(b.String())
}

// Helpers (prefixed with linear to avoid conflicts with other providers)

func linearGetStr(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func linearGetFloat(m map[string]any, key string) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return 0
}

func linearGetNested(m map[string]any, key, subkey string) string {
	if nested, ok := m[key].(map[string]any); ok {
		return linearGetStr(nested, subkey)
	}
	return ""
}

func linearGetNodes(m map[string]any, key string) []map[string]any {
	if container, ok := m[key].(map[string]any); ok {
		if nodes, ok := container["nodes"].([]any); ok {
			result := make([]map[string]any, 0, len(nodes))
			for _, n := range nodes {
				if nm, ok := n.(map[string]any); ok {
					result = append(result, nm)
				}
			}
			return result
		}
	}
	return nil
}

func linearGetLabels(issue map[string]any) []string {
	nodes := linearGetNodes(issue, "labels")
	labels := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if name := linearGetStr(n, "name"); name != "" {
			labels = append(labels, name)
		}
	}
	return labels
}

func linearGetMeta(m map[string]string, key, def string) string {
	if m != nil {
		if v, ok := m[key]; ok {
			return v
		}
	}
	return def
}

func linearParseTime(s string) int64 {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.Unix()
	}
	return sources.NowUnix()
}

func linearFmtDate(s string) string {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.Format("2006-01-02")
	}
	return ""
}

func linearFmtDateTime(s string) string {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.Format("2006-01-02 15:04")
	}
	return ""
}

func linearClamp(v, min, max, def int) int {
	if v <= 0 {
		return def
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
