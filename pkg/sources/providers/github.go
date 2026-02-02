package providers

import (
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

const (
	githubAPIBase = "https://api.github.com"
)

// GitHubProvider implements sources.Provider for GitHub integration.
// It exposes GitHub resources as a read-only filesystem under /sources/github/
type GitHubProvider struct {
	httpClient *http.Client
}

// NewGitHubProvider creates a new GitHub source provider
func NewGitHubProvider() *GitHubProvider {
	return &GitHubProvider{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (g *GitHubProvider) Name() string {
	return types.ToolGitHub.String()
}

// Stat returns file/directory attributes
func (g *GitHubProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	// Root is a directory
	if path == "" {
		return sources.DirInfo(), nil
	}

	parts := strings.Split(path, "/")

	// Handle known paths
	switch parts[0] {
	case "views":
		return g.statViews(ctx, pctx, parts[1:])
	case "repos":
		return g.statRepos(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

// ReadDir lists directory contents
func (g *GitHubProvider) ReadDir(ctx context.Context, pctx *sources.ProviderContext, path string) ([]sources.DirEntry, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	// Root directory
	if path == "" {
		return []sources.DirEntry{
			{Name: "views", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
			{Name: "repos", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
		}, nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "views":
		return g.readdirViews(ctx, pctx, parts[1:])
	case "repos":
		return g.readdirRepos(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

// Read reads file content
func (g *GitHubProvider) Read(ctx context.Context, pctx *sources.ProviderContext, path string, offset, length int64) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "views":
		return g.readViews(ctx, pctx, parts[1:], offset, length)
	case "repos":
		return g.readRepos(ctx, pctx, parts[1:], offset, length)
	default:
		return nil, sources.ErrNotFound
	}
}

// Readlink is not supported for GitHub
func (g *GitHubProvider) Readlink(ctx context.Context, pctx *sources.ProviderContext, path string) (string, error) {
	return "", sources.ErrNotFound
}

// Search is not supported for GitHub (use the repository browser instead)
func (g *GitHubProvider) Search(ctx context.Context, pctx *sources.ProviderContext, query string, limit int) ([]sources.SearchResult, error) {
	return nil, sources.ErrSearchNotSupported
}

// --- Views ---
// /views/repos.json - list of user's repositories
// /views/notifications.json - recent notifications
// /views/issues.json - assigned issues

func (g *GitHubProvider) statViews(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	if len(parts) == 0 {
		return sources.DirInfo(), nil
	}

	switch parts[0] {
	case "repos.json", "notifications.json", "issues.json":
		// Get data to determine size
		data, err := g.getViewData(ctx, pctx, parts[0])
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	default:
		return nil, sources.ErrNotFound
	}
}

func (g *GitHubProvider) readdirViews(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	if len(parts) == 0 {
		return []sources.DirEntry{
			{Name: "repos.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "notifications.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "issues.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil
	}
	return nil, sources.ErrNotDir
}

func (g *GitHubProvider) readViews(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) == 0 {
		return nil, sources.ErrIsDir
	}

	data, err := g.getViewData(ctx, pctx, parts[0])
	if err != nil {
		return nil, err
	}

	return sliceData(data, offset, length), nil
}

func (g *GitHubProvider) getViewData(ctx context.Context, pctx *sources.ProviderContext, view string) ([]byte, error) {
	token := pctx.Credentials.AccessToken

	switch view {
	case "repos.json":
		return g.fetchReposList(ctx, token)
	case "notifications.json":
		return g.fetchNotifications(ctx, token)
	case "issues.json":
		return g.fetchAssignedIssues(ctx, token)
	default:
		return nil, sources.ErrNotFound
	}
}

// --- Repos ---
// /repos/<owner>/<repo>/info.json - repository info
// /repos/<owner>/<repo>/prs.json - pull requests
// /repos/<owner>/<repo>/issues.json - issues

func (g *GitHubProvider) statRepos(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	switch len(parts) {
	case 0:
		return sources.DirInfo(), nil
	case 1:
		// /repos/<owner> - directory
		return sources.DirInfo(), nil
	case 2:
		// /repos/<owner>/<repo> - directory
		return sources.DirInfo(), nil
	case 3:
		// /repos/<owner>/<repo>/<file>.json
		owner, repo, file := parts[0], parts[1], parts[2]
		data, err := g.getRepoFileData(ctx, pctx, owner, repo, file)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	default:
		return nil, sources.ErrNotFound
	}
}

func (g *GitHubProvider) readdirRepos(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	token := pctx.Credentials.AccessToken

	switch len(parts) {
	case 0:
		// List owners from user's repos
		repos, err := g.fetchReposRaw(ctx, token)
		if err != nil {
			return nil, err
		}

		// Get unique owners
		owners := make(map[string]bool)
		for _, r := range repos {
			if owner, ok := r["owner"].(map[string]any); ok {
				if login, ok := owner["login"].(string); ok {
					owners[login] = true
				}
			}
		}

		entries := make([]sources.DirEntry, 0, len(owners))
		for owner := range owners {
			entries = append(entries, sources.DirEntry{
				Name:  owner,
				Mode:  sources.ModeDir,
				IsDir: true,
				Mtime: sources.NowUnix(),
			})
		}
		return entries, nil

	case 1:
		// List repos for an owner
		owner := parts[0]
		repos, err := g.fetchReposRaw(ctx, token)
		if err != nil {
			return nil, err
		}

		entries := make([]sources.DirEntry, 0)
		for _, r := range repos {
			if ownerObj, ok := r["owner"].(map[string]any); ok {
				if login, ok := ownerObj["login"].(string); ok && login == owner {
					if name, ok := r["name"].(string); ok {
						entries = append(entries, sources.DirEntry{
							Name:  name,
							Mode:  sources.ModeDir,
							IsDir: true,
							Mtime: sources.NowUnix(),
						})
					}
				}
			}
		}
		return entries, nil

	case 2:
		// List files in a repo
		return []sources.DirEntry{
			{Name: "info.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "prs.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "issues.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil

	default:
		return nil, sources.ErrNotDir
	}
}

func (g *GitHubProvider) readRepos(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) < 3 {
		return nil, sources.ErrIsDir
	}

	owner, repo, file := parts[0], parts[1], parts[2]
	data, err := g.getRepoFileData(ctx, pctx, owner, repo, file)
	if err != nil {
		return nil, err
	}

	return sliceData(data, offset, length), nil
}

func (g *GitHubProvider) getRepoFileData(ctx context.Context, pctx *sources.ProviderContext, owner, repo, file string) ([]byte, error) {
	token := pctx.Credentials.AccessToken

	switch file {
	case "info.json":
		return g.fetchRepoInfo(ctx, token, owner, repo)
	case "prs.json":
		return g.fetchRepoPRs(ctx, token, owner, repo)
	case "issues.json":
		return g.fetchRepoIssues(ctx, token, owner, repo)
	default:
		return nil, sources.ErrNotFound
	}
}

// --- API methods ---

func (g *GitHubProvider) fetchReposList(ctx context.Context, token string) ([]byte, error) {
	repos, err := g.fetchReposRaw(ctx, token)
	if err != nil {
		return nil, err
	}

	// Simplify the response
	result := make([]map[string]any, 0, len(repos))
	for _, r := range repos {
		result = append(result, map[string]any{
			"full_name":   r["full_name"],
			"description": r["description"],
			"private":     r["private"],
			"language":    r["language"],
			"stars":       r["stargazers_count"],
			"url":         r["html_url"],
		})
	}

	return jsonMarshal(map[string]any{"repos": result, "count": len(result)})
}

func (g *GitHubProvider) fetchReposRaw(ctx context.Context, token string) ([]map[string]any, error) {
	var repos []map[string]any
	if err := g.request(ctx, token, "GET", "/user/repos?per_page=100&sort=updated", &repos); err != nil {
		return nil, err
	}
	return repos, nil
}

func (g *GitHubProvider) fetchNotifications(ctx context.Context, token string) ([]byte, error) {
	var notifs []map[string]any
	if err := g.request(ctx, token, "GET", "/notifications?per_page=50", &notifs); err != nil {
		return nil, err
	}

	result := make([]map[string]any, 0, len(notifs))
	for _, n := range notifs {
		subj := n["subject"].(map[string]any)
		repo := n["repository"].(map[string]any)
		result = append(result, map[string]any{
			"id":         n["id"],
			"reason":     n["reason"],
			"unread":     n["unread"],
			"title":      subj["title"],
			"type":       subj["type"],
			"repo":       repo["full_name"],
			"updated_at": n["updated_at"],
		})
	}

	return jsonMarshal(map[string]any{"notifications": result, "count": len(result)})
}

func (g *GitHubProvider) fetchAssignedIssues(ctx context.Context, token string) ([]byte, error) {
	var issues []map[string]any
	if err := g.request(ctx, token, "GET", "/issues?filter=assigned&per_page=50&state=open", &issues); err != nil {
		return nil, err
	}

	result := make([]map[string]any, 0, len(issues))
	for _, i := range issues {
		// Skip PRs (they appear in issues endpoint)
		if _, ok := i["pull_request"]; ok {
			continue
		}
		result = append(result, map[string]any{
			"number":     i["number"],
			"title":      i["title"],
			"state":      i["state"],
			"repo":       i["repository"].(map[string]any)["full_name"],
			"url":        i["html_url"],
			"created_at": i["created_at"],
		})
	}

	return jsonMarshal(map[string]any{"issues": result, "count": len(result)})
}

func (g *GitHubProvider) fetchRepoInfo(ctx context.Context, token, owner, repo string) ([]byte, error) {
	var result map[string]any
	if err := g.request(ctx, token, "GET", fmt.Sprintf("/repos/%s/%s", owner, repo), &result); err != nil {
		return nil, err
	}

	return jsonMarshal(map[string]any{
		"name":           result["name"],
		"full_name":      result["full_name"],
		"description":    result["description"],
		"private":        result["private"],
		"language":       result["language"],
		"stars":          result["stargazers_count"],
		"forks":          result["forks_count"],
		"open_issues":    result["open_issues_count"],
		"default_branch": result["default_branch"],
		"url":            result["html_url"],
		"clone_url":      result["clone_url"],
	})
}

func (g *GitHubProvider) fetchRepoPRs(ctx context.Context, token, owner, repo string) ([]byte, error) {
	var prs []map[string]any
	path := fmt.Sprintf("/repos/%s/%s/pulls?state=open&per_page=50", owner, repo)
	if err := g.request(ctx, token, "GET", path, &prs); err != nil {
		return nil, err
	}

	result := make([]map[string]any, 0, len(prs))
	for _, p := range prs {
		user := ""
		if u, ok := p["user"].(map[string]any); ok {
			user, _ = u["login"].(string)
		}
		result = append(result, map[string]any{
			"number": p["number"],
			"title":  p["title"],
			"state":  p["state"],
			"user":   user,
			"draft":  p["draft"],
			"url":    p["html_url"],
		})
	}

	return jsonMarshal(map[string]any{"pull_requests": result, "count": len(result)})
}

func (g *GitHubProvider) fetchRepoIssues(ctx context.Context, token, owner, repo string) ([]byte, error) {
	var issues []map[string]any
	path := fmt.Sprintf("/repos/%s/%s/issues?state=open&per_page=50", owner, repo)
	if err := g.request(ctx, token, "GET", path, &issues); err != nil {
		return nil, err
	}

	result := make([]map[string]any, 0, len(issues))
	for _, i := range issues {
		// Skip PRs
		if _, ok := i["pull_request"]; ok {
			continue
		}
		user := ""
		if u, ok := i["user"].(map[string]any); ok {
			user, _ = u["login"].(string)
		}
		result = append(result, map[string]any{
			"number": i["number"],
			"title":  i["title"],
			"state":  i["state"],
			"user":   user,
			"url":    i["html_url"],
		})
	}

	return jsonMarshal(map[string]any{"issues": result, "count": len(result)})
}

func (g *GitHubProvider) request(ctx context.Context, token, method, path string, result any) error {
	url := githubAPIBase + path
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		var ghErr struct {
			Message string `json:"message"`
		}
		json.Unmarshal(body, &ghErr)
		if ghErr.Message != "" {
			return fmt.Errorf("github API: %s", ghErr.Message)
		}
		return fmt.Errorf("github API: %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// --- Helpers ---

func jsonMarshal(v any) ([]byte, error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func sliceData(data []byte, offset, length int64) []byte {
	if offset >= int64(len(data)) {
		return nil
	}
	end := int64(len(data))
	if length > 0 && offset+length < end {
		end = offset + length
	}
	return data[offset:end]
}

// ============================================================================
// QueryExecutor implementation
// ============================================================================

// ExecuteQuery runs a GitHub search query and returns results with generated filenames.
// This implements the sources.QueryExecutor interface for filesystem queries.
// Supports pagination via spec.PageToken for fetching subsequent pages.
func (g *GitHubProvider) ExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	token := pctx.Credentials.AccessToken
	limit := spec.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100 // GitHub max per page
	}

	// Parse page from PageToken (format: "page:N")
	page := 1
	if spec.PageToken != "" {
		fmt.Sscanf(spec.PageToken, "page:%d", &page)
	}

	// Detect search type from query - query syntax takes precedence over metadata
	// because special syntax like "list:org/" requires specific handling
	searchType := g.detectSearchType(spec.Query)
	
	// Only use metadata search_type if query detection didn't find special syntax
	if searchType == "repos" || searchType == "prs" {
		if st := spec.Metadata["search_type"]; st != "" {
			searchType = st
		}
	}

	// Get content type from metadata or default based on search type
	contentType := spec.Metadata["content_type"]
	if contentType == "" {
		if searchType == "repos" || searchType == "list-repos" {
			contentType = "markdown"
		} else {
			contentType = "markdown"
		}
	}

	var results []sources.QueryResult
	var nextPageToken string
	var hasMore bool

	switch searchType {
	case "list-repos":
		results, nextPageToken, hasMore = g.listOrgOrUserRepos(ctx, token, spec.Query, limit, page, spec.FilenameFormat, contentType)
	case "repos":
		results, nextPageToken, hasMore = g.searchRepositories(ctx, token, spec.Query, limit, page, spec.FilenameFormat, contentType)
	case "issues":
		results, nextPageToken, hasMore = g.searchIssues(ctx, token, spec.Query, "issues", limit, page, spec.FilenameFormat, contentType)
	case "prs":
		results, nextPageToken, hasMore = g.searchIssues(ctx, token, spec.Query, "prs", limit, page, spec.FilenameFormat, contentType)
	case "commits":
		results, nextPageToken, hasMore = g.listCommits(ctx, token, spec.Query, limit, page, spec.FilenameFormat, contentType)
	case "releases":
		results, nextPageToken, hasMore = g.listReleases(ctx, token, spec.Query, limit, page, spec.FilenameFormat, contentType)
	case "workflows":
		results, nextPageToken, hasMore = g.listWorkflowRuns(ctx, token, spec.Query, limit, page, spec.FilenameFormat, contentType)
	case "files":
		results, nextPageToken, hasMore = g.listFiles(ctx, token, spec.Query, limit, spec.FilenameFormat, contentType)
	case "branches":
		results, nextPageToken, hasMore = g.listBranches(ctx, token, spec.Query, limit, page, spec.FilenameFormat)
	default:
		results, nextPageToken, hasMore = g.searchIssues(ctx, token, spec.Query, "prs", limit, page, spec.FilenameFormat, contentType)
	}

	return &sources.QueryResponse{
		Results:       results,
		NextPageToken: nextPageToken,
		HasMore:       hasMore,
	}, nil
}

// detectSearchType determines the search type from the query
func (g *GitHubProvider) detectSearchType(query string) string {
	q := strings.ToLower(query)
	// Check for org/user listing first (special syntax)
	if strings.Contains(q, "list:org/") || strings.Contains(q, "list:user/") {
		return "list-repos"
	}
	if strings.Contains(q, "language:") || strings.Contains(q, "stars:") || strings.Contains(q, "forks:") || strings.Contains(q, "topic:") {
		return "repos"
	}
	if strings.Contains(q, "is:pr") || strings.Contains(q, "is:merged") {
		return "prs"
	}
	if strings.Contains(q, "is:issue") {
		return "issues"
	}
	// Default to prs if repo: is present without other type hints
	if strings.Contains(q, "repo:") {
		return "prs"
	}
	return "repos"
}

// parseRepoFromQuery extracts owner/repo from a query string
func (g *GitHubProvider) parseRepoFromQuery(query string) (owner, repo string) {
	// Look for repo:owner/name
	if idx := strings.Index(query, "repo:"); idx != -1 {
		start := idx + len("repo:")
		end := start
		for end < len(query) && query[end] != ' ' {
			end++
		}
		parts := strings.SplitN(query[start:end], "/", 2)
		if len(parts) == 2 {
			return parts[0], parts[1]
		}
	}
	return "", ""
}

// listOrgOrUserRepos lists repositories from an org or user using REST API (not search)
// This supports private repos which the search API does not
// Query format: "list:org/ORGNAME type:private" or "list:user/USERNAME type:public"
func (g *GitHubProvider) listOrgOrUserRepos(ctx context.Context, token, query string, limit, page int, filenameFormat, contentType string) ([]sources.QueryResult, string, bool) {
	// Parse the query
	var endpoint string
	repoType := "all" // all, public, private

	// Extract list:org/X or list:user/X
	if strings.Contains(query, "list:org/") {
		start := strings.Index(query, "list:org/") + len("list:org/")
		end := start
		for end < len(query) && query[end] != ' ' {
			end++
		}
		org := query[start:end]
		endpoint = fmt.Sprintf("/orgs/%s/repos", org)
	} else if strings.Contains(query, "list:user/") {
		start := strings.Index(query, "list:user/") + len("list:user/")
		end := start
		for end < len(query) && query[end] != ' ' {
			end++
		}
		user := query[start:end]
		if user == "me" {
			endpoint = "/user/repos"
		} else {
			endpoint = fmt.Sprintf("/users/%s/repos", user)
		}
	} else {
		// Fallback to user repos
		endpoint = "/user/repos"
	}

	// Extract type filter
	if strings.Contains(query, "type:private") {
		repoType = "private"
	} else if strings.Contains(query, "type:public") {
		repoType = "public"
	}

	// Build URL with pagination and type
	url := fmt.Sprintf("%s%s?per_page=%d&page=%d&sort=updated&type=%s", githubAPIBase, endpoint, limit, page, repoType)

	var repos []map[string]any
	if err := g.requestURL(ctx, token, "GET", url, &repos); err != nil {
		return nil, "", false
	}

	if filenameFormat == "" {
		filenameFormat = "{name}.json"
	}

	results := make([]sources.QueryResult, 0, len(repos))
	for _, item := range repos {
		id := formatInt(item["id"])
		name, _ := item["name"].(string)
		fullName, _ := item["full_name"].(string)
		description, _ := item["description"].(string)
		language, _ := item["language"].(string)
		stars := formatInt(item["stargazers_count"])
		private, _ := item["private"].(bool)

		visibility := "public"
		if private {
			visibility = "private"
		}

		metadata := map[string]string{
			"id":           id,
			"name":         name,
			"full_name":    fullName,
			"description":  description,
			"language":     language,
			"stars":        stars,
			"visibility":   visibility,
			"type":         "repo",
			"content_type": contentType,
		}

		filename := g.FormatFilename(filenameFormat, metadata)
		resultID := fmt.Sprintf("repo:%s", fullName)

		results = append(results, sources.QueryResult{
			ID:       resultID,
			Filename: filename,
			Metadata: metadata,
			Size:     int64(64 * 1024), // 64KB generous estimate
			Mtime:    sources.NowUnix(),
		})
	}

	// For REST API listing, hasMore is based on whether we got a full page
	hasMore := len(repos) == limit
	nextPageToken := ""
	if hasMore {
		nextPageToken = fmt.Sprintf("page:%d", page+1)
	}

	return results, nextPageToken, hasMore
}

// searchRepositories searches GitHub repositories
func (g *GitHubProvider) searchRepositories(ctx context.Context, token, query string, limit, page int, filenameFormat, contentType string) ([]sources.QueryResult, string, bool) {
	url := fmt.Sprintf("%s/search/repositories?q=%s&per_page=%d&page=%d&sort=updated", githubAPIBase, urlEncode(query), limit, page)

	var response struct {
		TotalCount int              `json:"total_count"`
		Items      []map[string]any `json:"items"`
	}

	if err := g.requestURL(ctx, token, "GET", url, &response); err != nil {
		return nil, "", false
	}

	if filenameFormat == "" {
		filenameFormat = "{name}.json"
	}

	results := make([]sources.QueryResult, 0, len(response.Items))
	for _, item := range response.Items {
		// Use integer formatting to avoid scientific notation
		id := formatInt(item["id"])
		name, _ := item["name"].(string)
		fullName, _ := item["full_name"].(string)
		description, _ := item["description"].(string)
		language, _ := item["language"].(string)
		stars := formatInt(item["stargazers_count"])

		// Use generous size for repo JSON to ensure full content is read
		// Actual repo JSON is typically 2-10KB
		repoMaxSize := int64(64 * 1024) // 64KB
		_ = description                 // available but we use max size

		metadata := map[string]string{
			"id":           id,
			"name":         name,
			"full_name":    fullName,
			"description":  description,
			"language":     language,
			"stars":        stars,
			"type":         "repo",
			"content_type": contentType,
		}

		filename := g.FormatFilename(filenameFormat, metadata)
		// Result ID encodes the type and full_name for fetching
		resultID := fmt.Sprintf("repo:%s", fullName)

		results = append(results, sources.QueryResult{
			ID:       resultID,
			Filename: filename,
			Metadata: metadata,
			Size:     repoMaxSize,
			Mtime:    sources.NowUnix(),
		})
	}

	hasMore := page*limit < response.TotalCount && len(response.Items) == limit
	nextPageToken := ""
	if hasMore {
		nextPageToken = fmt.Sprintf("page:%d", page+1)
	}

	return results, nextPageToken, hasMore
}

// searchIssues searches GitHub issues and PRs
func (g *GitHubProvider) searchIssues(ctx context.Context, token, query, searchType string, limit, page int, filenameFormat, contentType string) ([]sources.QueryResult, string, bool) {
	// Add is:issue or is:pr qualifier if not already present
	q := query
	if searchType == "prs" && !strings.Contains(strings.ToLower(q), "is:pr") {
		q = q + " is:pr"
	} else if searchType == "issues" && !strings.Contains(strings.ToLower(q), "is:issue") {
		q = q + " is:issue"
	}

	url := fmt.Sprintf("%s/search/issues?q=%s&per_page=%d&page=%d&sort=updated", githubAPIBase, urlEncode(q), limit, page)

	var response struct {
		TotalCount int              `json:"total_count"`
		Items      []map[string]any `json:"items"`
	}

	if err := g.requestURL(ctx, token, "GET", url, &response); err != nil {
		return nil, "", false
	}

	// Default filename format based on content type
	// Note: diff content is wrapped in markdown for preview support
	if filenameFormat == "" {
		switch contentType {
		case "diff":
			filenameFormat = "{number}_{title}_diff.md"
		case "markdown":
			filenameFormat = "{number}_{title}.md"
		default:
			filenameFormat = "{number}_{title}.json"
		}
	}

	results := make([]sources.QueryResult, 0, len(response.Items))
	for _, item := range response.Items {
		id := formatInt(item["id"])
		number := formatInt(item["number"])
		title, _ := item["title"].(string)
		state, _ := item["state"].(string)
		body, _ := item["body"].(string)

		// Extract owner/repo from repository_url
		repoURL, _ := item["repository_url"].(string)
		owner, repo := extractOwnerRepo(repoURL)

		// Determine if it's a PR or issue
		itemType := "issue"
		if _, ok := item["pull_request"]; ok {
			itemType = "pr"
		}

		// Get author
		author := ""
		if user, ok := item["user"].(map[string]any); ok {
			author, _ = user["login"].(string)
		}

		// Report a large size to ensure full content is read
		// FUSE uses this to limit reads, so we must report >= actual content size
		// We use a generous upper bound since we don't know actual size until fetch
		var maxSize int64
		switch contentType {
		case "diff":
			// Diffs can be very large (megabytes for big PRs)
			maxSize = 10 * 1024 * 1024 // 10MB
		case "markdown":
			// Issue/PR markdown is typically smaller
			maxSize = 1 * 1024 * 1024 // 1MB
		default:
			// JSON metadata
			maxSize = 512 * 1024 // 512KB
		}
		_ = body // body is available but we use max size to be safe

		metadata := map[string]string{
			"id":           id,
			"number":       number,
			"title":        title,
			"state":        state,
			"owner":        owner,
			"repo":         repo,
			"type":         itemType,
			"author":       author,
			"content_type": contentType,
		}

		filename := g.FormatFilename(filenameFormat, metadata)
		// Result ID encodes type, owner, repo, number for fetching content
		resultID := fmt.Sprintf("%s:%s/%s#%s:%s", itemType, owner, repo, number, contentType)

		results = append(results, sources.QueryResult{
			ID:       resultID,
			Filename: filename,
			Metadata: metadata,
			Size:     maxSize,
			Mtime:    sources.NowUnix(),
		})
	}

	hasMore := page*limit < response.TotalCount && len(response.Items) == limit
	nextPageToken := ""
	if hasMore {
		nextPageToken = fmt.Sprintf("page:%d", page+1)
	}

	return results, nextPageToken, hasMore
}

// ============================================================================
// Commits
// ============================================================================

// listCommits fetches commits from a repository
func (g *GitHubProvider) listCommits(ctx context.Context, token, query string, limit, page int, filenameFormat, contentType string) ([]sources.QueryResult, string, bool) {
	owner, repo := g.parseRepoFromQuery(query)
	if owner == "" || repo == "" {
		return nil, "", false
	}

	// Build URL with optional filters
	url := fmt.Sprintf("%s/repos/%s/%s/commits?per_page=%d&page=%d", githubAPIBase, owner, repo, limit, page)

	// Parse optional filters from query
	if strings.Contains(query, "branch:") {
		if branch := g.extractQueryParam(query, "branch:"); branch != "" {
			url += "&sha=" + branch
		}
	}
	if strings.Contains(query, "author:") {
		if author := g.extractQueryParam(query, "author:"); author != "" {
			url += "&author=" + author
		}
	}
	if strings.Contains(query, "since:") {
		if since := g.extractQueryParam(query, "since:"); since != "" {
			url += "&since=" + since + "T00:00:00Z"
		}
	}
	if strings.Contains(query, "path:") {
		if path := g.extractQueryParam(query, "path:"); path != "" {
			url += "&path=" + path
		}
	}

	var commits []map[string]any
	if err := g.requestURL(ctx, token, "GET", url, &commits); err != nil {
		return nil, "", false
	}

	if filenameFormat == "" {
		filenameFormat = "{sha_short}_{message}.md"
	}

	results := make([]sources.QueryResult, 0, len(commits))
	for _, item := range commits {
		sha, _ := item["sha"].(string)
		shaShort := sha
		if len(sha) > 7 {
			shaShort = sha[:7]
		}

		commit, _ := item["commit"].(map[string]any)
		message, _ := commit["message"].(string)
		// Take first line of message
		if idx := strings.Index(message, "\n"); idx > 0 {
			message = message[:idx]
		}

		author := ""
		if a, ok := item["author"].(map[string]any); ok {
			author, _ = a["login"].(string)
		}
		if author == "" {
			if ca, ok := commit["author"].(map[string]any); ok {
				author, _ = ca["name"].(string)
			}
		}

		date := ""
		if ca, ok := commit["author"].(map[string]any); ok {
			date, _ = ca["date"].(string)
			if len(date) > 10 {
				date = date[:10]
			}
		}

		metadata := map[string]string{
			"sha":          sha,
			"sha_short":    shaShort,
			"message":      message,
			"author":       author,
			"date":         date,
			"owner":        owner,
			"repo":         repo,
			"type":         "commit",
			"content_type": contentType,
		}

		filename := g.FormatFilename(filenameFormat, metadata)
		resultID := fmt.Sprintf("commit:%s/%s@%s:%s", owner, repo, sha, contentType)

		results = append(results, sources.QueryResult{
			ID:       resultID,
			Filename: filename,
			Metadata: metadata,
			Size:     5 * 1024 * 1024, // 5MB for diffs
			Mtime:    sources.NowUnix(),
		})
	}

	hasMore := len(commits) == limit
	nextPageToken := ""
	if hasMore {
		nextPageToken = fmt.Sprintf("page:%d", page+1)
	}

	return results, nextPageToken, hasMore
}

// ============================================================================
// Releases
// ============================================================================

// listReleases fetches releases from a repository
func (g *GitHubProvider) listReleases(ctx context.Context, token, query string, limit, page int, filenameFormat, contentType string) ([]sources.QueryResult, string, bool) {
	owner, repo := g.parseRepoFromQuery(query)
	if owner == "" || repo == "" {
		return nil, "", false
	}

	// Check for "latest" modifier
	if strings.Contains(query, "latest") {
		return g.getLatestRelease(ctx, token, owner, repo, filenameFormat, contentType)
	}

	url := fmt.Sprintf("%s/repos/%s/%s/releases?per_page=%d&page=%d", githubAPIBase, owner, repo, limit, page)

	var releases []map[string]any
	if err := g.requestURL(ctx, token, "GET", url, &releases); err != nil {
		return nil, "", false
	}

	if filenameFormat == "" {
		filenameFormat = "{tag}_{name}.md"
	}

	// Filter prereleases if specified
	excludePrerelease := strings.Contains(query, "prerelease:false")

	results := make([]sources.QueryResult, 0, len(releases))
	for _, item := range releases {
		if excludePrerelease {
			if prerelease, _ := item["prerelease"].(bool); prerelease {
				continue
			}
		}

		id := formatInt(item["id"])
		tag, _ := item["tag_name"].(string)
		name, _ := item["name"].(string)
		if name == "" {
			name = tag
		}
		draft, _ := item["draft"].(bool)
		prerelease, _ := item["prerelease"].(bool)

		author := ""
		if a, ok := item["author"].(map[string]any); ok {
			author, _ = a["login"].(string)
		}

		publishedAt, _ := item["published_at"].(string)
		date := ""
		if len(publishedAt) > 10 {
			date = publishedAt[:10]
		}

		status := "released"
		if draft {
			status = "draft"
		} else if prerelease {
			status = "prerelease"
		}

		metadata := map[string]string{
			"id":           id,
			"tag":          tag,
			"name":         name,
			"author":       author,
			"date":         date,
			"status":       status,
			"owner":        owner,
			"repo":         repo,
			"type":         "release",
			"content_type": contentType,
		}

		filename := g.FormatFilename(filenameFormat, metadata)
		resultID := fmt.Sprintf("release:%s/%s@%s:%s", owner, repo, tag, contentType)

		results = append(results, sources.QueryResult{
			ID:       resultID,
			Filename: filename,
			Metadata: metadata,
			Size:     1 * 1024 * 1024, // 1MB
			Mtime:    sources.NowUnix(),
		})
	}

	hasMore := len(releases) == limit
	nextPageToken := ""
	if hasMore {
		nextPageToken = fmt.Sprintf("page:%d", page+1)
	}

	return results, nextPageToken, hasMore
}

func (g *GitHubProvider) getLatestRelease(ctx context.Context, token, owner, repo, filenameFormat, contentType string) ([]sources.QueryResult, string, bool) {
	url := fmt.Sprintf("%s/repos/%s/%s/releases/latest", githubAPIBase, owner, repo)

	var item map[string]any
	if err := g.requestURL(ctx, token, "GET", url, &item); err != nil {
		return nil, "", false
	}

	if filenameFormat == "" {
		filenameFormat = "{tag}_{name}.md"
	}

	id := formatInt(item["id"])
	tag, _ := item["tag_name"].(string)
	name, _ := item["name"].(string)
	if name == "" {
		name = tag
	}

	author := ""
	if a, ok := item["author"].(map[string]any); ok {
		author, _ = a["login"].(string)
	}

	metadata := map[string]string{
		"id":           id,
		"tag":          tag,
		"name":         name,
		"author":       author,
		"owner":        owner,
		"repo":         repo,
		"type":         "release",
		"content_type": contentType,
	}

	filename := g.FormatFilename(filenameFormat, metadata)
	resultID := fmt.Sprintf("release:%s/%s@%s:%s", owner, repo, tag, contentType)

	return []sources.QueryResult{{
		ID:       resultID,
		Filename: filename,
		Metadata: metadata,
		Size:     1 * 1024 * 1024,
		Mtime:    sources.NowUnix(),
	}}, "", false
}

// ============================================================================
// Workflow Runs (CI/CD)
// ============================================================================

// listWorkflowRuns fetches GitHub Actions workflow runs
func (g *GitHubProvider) listWorkflowRuns(ctx context.Context, token, query string, limit, page int, filenameFormat, contentType string) ([]sources.QueryResult, string, bool) {
	owner, repo := g.parseRepoFromQuery(query)
	if owner == "" || repo == "" {
		return nil, "", false
	}

	url := fmt.Sprintf("%s/repos/%s/%s/actions/runs?per_page=%d&page=%d", githubAPIBase, owner, repo, limit, page)

	// Parse optional filters
	if strings.Contains(query, "status:") {
		if status := g.extractQueryParam(query, "status:"); status != "" {
			// Map friendly names to API values
			switch status {
			case "success":
				url += "&status=success"
			case "failure", "failed":
				url += "&status=failure"
			case "in_progress", "running":
				url += "&status=in_progress"
			case "pending", "queued":
				url += "&status=queued"
			default:
				url += "&status=" + status
			}
		}
	}
	if strings.Contains(query, "branch:") {
		if branch := g.extractQueryParam(query, "branch:"); branch != "" {
			url += "&branch=" + branch
		}
	}
	if strings.Contains(query, "event:") {
		if event := g.extractQueryParam(query, "event:"); event != "" {
			// Map friendly names
			switch event {
			case "pr", "pull_request":
				url += "&event=pull_request"
			default:
				url += "&event=" + event
			}
		}
	}

	var response struct {
		TotalCount   int              `json:"total_count"`
		WorkflowRuns []map[string]any `json:"workflow_runs"`
	}
	if err := g.requestURL(ctx, token, "GET", url, &response); err != nil {
		return nil, "", false
	}

	if filenameFormat == "" {
		filenameFormat = "{id}_{name}_{status}.md"
	}

	results := make([]sources.QueryResult, 0, len(response.WorkflowRuns))
	for _, item := range response.WorkflowRuns {
		id := formatInt(item["id"])
		name, _ := item["name"].(string)
		status, _ := item["status"].(string)
		conclusion, _ := item["conclusion"].(string)
		branch, _ := item["head_branch"].(string)
		event, _ := item["event"].(string)
		runNumber := formatInt(item["run_number"])

		// Use conclusion if available (more descriptive than status)
		displayStatus := status
		if conclusion != "" {
			displayStatus = conclusion
		}

		// Get trigger info
		actor := ""
		if a, ok := item["actor"].(map[string]any); ok {
			actor, _ = a["login"].(string)
		}

		createdAt, _ := item["created_at"].(string)
		date := ""
		if len(createdAt) > 10 {
			date = createdAt[:10]
		}

		metadata := map[string]string{
			"id":           id,
			"name":         name,
			"status":       displayStatus,
			"branch":       branch,
			"event":        event,
			"run_number":   runNumber,
			"actor":        actor,
			"date":         date,
			"owner":        owner,
			"repo":         repo,
			"type":         "workflow",
			"content_type": contentType,
		}

		filename := g.FormatFilename(filenameFormat, metadata)
		resultID := fmt.Sprintf("workflow:%s/%s#%s:%s", owner, repo, id, contentType)

		results = append(results, sources.QueryResult{
			ID:       resultID,
			Filename: filename,
			Metadata: metadata,
			Size:     10 * 1024 * 1024, // 10MB for logs
			Mtime:    sources.NowUnix(),
		})
	}

	hasMore := page*limit < response.TotalCount && len(response.WorkflowRuns) == limit
	nextPageToken := ""
	if hasMore {
		nextPageToken = fmt.Sprintf("page:%d", page+1)
	}

	return results, nextPageToken, hasMore
}

// ============================================================================
// Files
// ============================================================================

// listFiles fetches file contents or directory listings
func (g *GitHubProvider) listFiles(ctx context.Context, token, query string, limit int, filenameFormat, contentType string) ([]sources.QueryResult, string, bool) {
	owner, repo := g.parseRepoFromQuery(query)
	if owner == "" || repo == "" {
		return nil, "", false
	}

	path := g.extractQueryParam(query, "path:")
	if path == "" {
		path = "README.md" // Default to README
	}

	ref := g.extractQueryParam(query, "ref:")
	if ref == "" {
		ref = "HEAD"
	}

	url := fmt.Sprintf("%s/repos/%s/%s/contents/%s?ref=%s", githubAPIBase, owner, repo, path, ref)

	// Try to fetch - could be file or directory
	var content any
	if err := g.requestURL(ctx, token, "GET", url, &content); err != nil {
		return nil, "", false
	}

	if filenameFormat == "" {
		filenameFormat = "{path}"
	}

	results := make([]sources.QueryResult, 0)

	// Check if it's a directory (array) or file (object)
	switch v := content.(type) {
	case []any:
		// Directory listing
		for i, item := range v {
			if i >= limit {
				break
			}
			if file, ok := item.(map[string]any); ok {
				name, _ := file["name"].(string)
				filePath, _ := file["path"].(string)
				fileType, _ := file["type"].(string)
				size := int64(0)
				if s, ok := file["size"].(float64); ok {
					size = int64(s)
				}

				metadata := map[string]string{
					"name":         name,
					"path":         filePath,
					"file_type":    fileType,
					"owner":        owner,
					"repo":         repo,
					"ref":          ref,
					"type":         "file",
					"content_type": contentType,
				}

				filename := g.FormatFilename(filenameFormat, metadata)
				resultID := fmt.Sprintf("file:%s/%s@%s:%s:%s", owner, repo, ref, filePath, contentType)

				if size == 0 && fileType == "dir" {
					size = 4096 // Directory marker
				} else if size == 0 {
					size = 1 * 1024 * 1024 // 1MB default for files
				}

				results = append(results, sources.QueryResult{
					ID:       resultID,
					Filename: filename,
					Metadata: metadata,
					Size:     size,
					Mtime:    sources.NowUnix(),
				})
			}
		}
	case map[string]any:
		// Single file
		name, _ := v["name"].(string)
		filePath, _ := v["path"].(string)
		size := int64(1 * 1024 * 1024)
		if s, ok := v["size"].(float64); ok {
			size = int64(s)
		}

		metadata := map[string]string{
			"name":         name,
			"path":         filePath,
			"owner":        owner,
			"repo":         repo,
			"ref":          ref,
			"type":         "file",
			"content_type": contentType,
		}

		filename := g.FormatFilename(filenameFormat, metadata)
		resultID := fmt.Sprintf("file:%s/%s@%s:%s:%s", owner, repo, ref, filePath, contentType)

		results = append(results, sources.QueryResult{
			ID:       resultID,
			Filename: filename,
			Metadata: metadata,
			Size:     size,
			Mtime:    sources.NowUnix(),
		})
	}

	return results, "", false
}

// ============================================================================
// Branches
// ============================================================================

// listBranches fetches branches from a repository
func (g *GitHubProvider) listBranches(ctx context.Context, token, query string, limit, page int, filenameFormat string) ([]sources.QueryResult, string, bool) {
	owner, repo := g.parseRepoFromQuery(query)
	if owner == "" || repo == "" {
		return nil, "", false
	}

	url := fmt.Sprintf("%s/repos/%s/%s/branches?per_page=%d&page=%d", githubAPIBase, owner, repo, limit, page)

	// Check for protected filter
	if strings.Contains(query, "protected:true") {
		url += "&protected=true"
	}

	var branches []map[string]any
	if err := g.requestURL(ctx, token, "GET", url, &branches); err != nil {
		return nil, "", false
	}

	if filenameFormat == "" {
		filenameFormat = "{name}.md"
	}

	results := make([]sources.QueryResult, 0, len(branches))
	for _, item := range branches {
		name, _ := item["name"].(string)
		protected, _ := item["protected"].(bool)

		sha := ""
		if commit, ok := item["commit"].(map[string]any); ok {
			sha, _ = commit["sha"].(string)
			if len(sha) > 7 {
				sha = sha[:7]
			}
		}

		protectedStr := "no"
		if protected {
			protectedStr = "yes"
		}

		metadata := map[string]string{
			"name":      name,
			"sha":       sha,
			"protected": protectedStr,
			"owner":     owner,
			"repo":      repo,
			"type":      "branch",
		}

		filename := g.FormatFilename(filenameFormat, metadata)
		resultID := fmt.Sprintf("branch:%s/%s@%s", owner, repo, name)

		results = append(results, sources.QueryResult{
			ID:       resultID,
			Filename: filename,
			Metadata: metadata,
			Size:     64 * 1024, // 64KB
			Mtime:    sources.NowUnix(),
		})
	}

	hasMore := len(branches) == limit
	nextPageToken := ""
	if hasMore {
		nextPageToken = fmt.Sprintf("page:%d", page+1)
	}

	return results, nextPageToken, hasMore
}

// extractQueryParam extracts a parameter value from the query string
func (g *GitHubProvider) extractQueryParam(query, param string) string {
	idx := strings.Index(query, param)
	if idx == -1 {
		return ""
	}
	start := idx + len(param)
	end := start
	for end < len(query) && query[end] != ' ' {
		end++
	}
	return query[start:end]
}

// ReadResult fetches the content by ID.
// ID formats:
//   - "repo:owner/repo" - repository
//   - "pr:owner/repo#number:content_type" - pull request
//   - "issue:owner/repo#number:content_type" - issue
//   - "commit:owner/repo@sha:content_type" - commit
//   - "release:owner/repo@tag:content_type" - release
//   - "workflow:owner/repo#id:content_type" - workflow run
//   - "file:owner/repo@ref:path:content_type" - file
//   - "branch:owner/repo@name" - branch
func (g *GitHubProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	token := pctx.Credentials.AccessToken

	// Parse the result ID by type prefix
	if strings.HasPrefix(resultID, "repo:") {
		fullName := strings.TrimPrefix(resultID, "repo:")
		return g.fetchRepoMarkdown(ctx, token, fullName)
	}

	if strings.HasPrefix(resultID, "commit:") {
		return g.readCommitResult(ctx, token, resultID)
	}

	if strings.HasPrefix(resultID, "release:") {
		return g.readReleaseResult(ctx, token, resultID)
	}

	if strings.HasPrefix(resultID, "workflow:") {
		return g.readWorkflowResult(ctx, token, resultID)
	}

	if strings.HasPrefix(resultID, "file:") {
		return g.readFileResult(ctx, token, resultID)
	}

	if strings.HasPrefix(resultID, "branch:") {
		return g.readBranchResult(ctx, token, resultID)
	}

	// Parse issue/PR ID: "type:owner/repo#number:content_type"
	parts := strings.SplitN(resultID, ":", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid result ID format: %s", resultID)
	}

	itemType := parts[0]
	contentType := "markdown"
	if len(parts) == 3 {
		contentType = parts[2]
	}

	// Parse owner/repo#number
	repoAndNumber := parts[1]
	hashIdx := strings.LastIndex(repoAndNumber, "#")
	if hashIdx == -1 {
		return nil, fmt.Errorf("invalid result ID format: %s", resultID)
	}

	ownerRepo := repoAndNumber[:hashIdx]
	number := repoAndNumber[hashIdx+1:]

	ownerRepoParts := strings.SplitN(ownerRepo, "/", 2)
	if len(ownerRepoParts) != 2 {
		return nil, fmt.Errorf("invalid owner/repo format: %s", ownerRepo)
	}
	owner := ownerRepoParts[0]
	repo := ownerRepoParts[1]

	if itemType == "pr" {
		return g.fetchPRContent(ctx, token, owner, repo, number, contentType)
	}
	return g.fetchIssueContent(ctx, token, owner, repo, number, contentType)
}

// readCommitResult fetches commit content
// ID format: "commit:owner/repo@sha:content_type"
func (g *GitHubProvider) readCommitResult(ctx context.Context, token, resultID string) ([]byte, error) {
	// Parse: commit:owner/repo@sha:content_type
	rest := strings.TrimPrefix(resultID, "commit:")
	parts := strings.SplitN(rest, ":", 2)
	contentType := "markdown"
	if len(parts) == 2 {
		contentType = parts[1]
	}

	// Parse owner/repo@sha
	atIdx := strings.LastIndex(parts[0], "@")
	if atIdx == -1 {
		return nil, fmt.Errorf("invalid commit ID: %s", resultID)
	}
	ownerRepo := parts[0][:atIdx]
	sha := parts[0][atIdx+1:]

	ownerRepoParts := strings.SplitN(ownerRepo, "/", 2)
	if len(ownerRepoParts) != 2 {
		return nil, fmt.Errorf("invalid owner/repo: %s", ownerRepo)
	}

	return g.fetchCommitContent(ctx, token, ownerRepoParts[0], ownerRepoParts[1], sha, contentType)
}

// readReleaseResult fetches release content
// ID format: "release:owner/repo@tag:content_type"
func (g *GitHubProvider) readReleaseResult(ctx context.Context, token, resultID string) ([]byte, error) {
	rest := strings.TrimPrefix(resultID, "release:")
	parts := strings.SplitN(rest, ":", 2)
	contentType := "markdown"
	if len(parts) == 2 {
		contentType = parts[1]
	}

	atIdx := strings.LastIndex(parts[0], "@")
	if atIdx == -1 {
		return nil, fmt.Errorf("invalid release ID: %s", resultID)
	}
	ownerRepo := parts[0][:atIdx]
	tag := parts[0][atIdx+1:]

	ownerRepoParts := strings.SplitN(ownerRepo, "/", 2)
	if len(ownerRepoParts) != 2 {
		return nil, fmt.Errorf("invalid owner/repo: %s", ownerRepo)
	}

	return g.fetchReleaseContent(ctx, token, ownerRepoParts[0], ownerRepoParts[1], tag, contentType)
}

// readWorkflowResult fetches workflow run content
// ID format: "workflow:owner/repo#id:content_type"
func (g *GitHubProvider) readWorkflowResult(ctx context.Context, token, resultID string) ([]byte, error) {
	rest := strings.TrimPrefix(resultID, "workflow:")
	parts := strings.SplitN(rest, ":", 2)
	contentType := "markdown"
	if len(parts) == 2 {
		contentType = parts[1]
	}

	hashIdx := strings.LastIndex(parts[0], "#")
	if hashIdx == -1 {
		return nil, fmt.Errorf("invalid workflow ID: %s", resultID)
	}
	ownerRepo := parts[0][:hashIdx]
	runID := parts[0][hashIdx+1:]

	ownerRepoParts := strings.SplitN(ownerRepo, "/", 2)
	if len(ownerRepoParts) != 2 {
		return nil, fmt.Errorf("invalid owner/repo: %s", ownerRepo)
	}

	return g.fetchWorkflowContent(ctx, token, ownerRepoParts[0], ownerRepoParts[1], runID, contentType)
}

// readFileResult fetches file content
// ID format: "file:owner/repo@ref:path:content_type"
func (g *GitHubProvider) readFileResult(ctx context.Context, token, resultID string) ([]byte, error) {
	rest := strings.TrimPrefix(resultID, "file:")

	// Parse backwards: last : is content_type, second-to-last starts path
	lastColon := strings.LastIndex(rest, ":")
	if lastColon == -1 {
		return nil, fmt.Errorf("invalid file ID: %s", resultID)
	}
	contentType := rest[lastColon+1:]
	rest = rest[:lastColon]

	// Find owner/repo@ref:path
	atIdx := strings.Index(rest, "@")
	if atIdx == -1 {
		return nil, fmt.Errorf("invalid file ID: %s", resultID)
	}
	ownerRepo := rest[:atIdx]
	refAndPath := rest[atIdx+1:]

	// Split ref:path
	colonIdx := strings.Index(refAndPath, ":")
	if colonIdx == -1 {
		return nil, fmt.Errorf("invalid file ID: %s", resultID)
	}
	ref := refAndPath[:colonIdx]
	path := refAndPath[colonIdx+1:]

	ownerRepoParts := strings.SplitN(ownerRepo, "/", 2)
	if len(ownerRepoParts) != 2 {
		return nil, fmt.Errorf("invalid owner/repo: %s", ownerRepo)
	}

	return g.fetchFileContent(ctx, token, ownerRepoParts[0], ownerRepoParts[1], ref, path, contentType)
}

// readBranchResult fetches branch info
// ID format: "branch:owner/repo@name"
func (g *GitHubProvider) readBranchResult(ctx context.Context, token, resultID string) ([]byte, error) {
	rest := strings.TrimPrefix(resultID, "branch:")

	atIdx := strings.LastIndex(rest, "@")
	if atIdx == -1 {
		return nil, fmt.Errorf("invalid branch ID: %s", resultID)
	}
	ownerRepo := rest[:atIdx]
	branch := rest[atIdx+1:]

	ownerRepoParts := strings.SplitN(ownerRepo, "/", 2)
	if len(ownerRepoParts) != 2 {
		return nil, fmt.Errorf("invalid owner/repo: %s", ownerRepo)
	}

	return g.fetchBranchContent(ctx, token, ownerRepoParts[0], ownerRepoParts[1], branch)
}

// fetchPRContent fetches PR content in the specified format
func (g *GitHubProvider) fetchPRContent(ctx context.Context, token, owner, repo, number, contentType string) ([]byte, error) {
	switch contentType {
	case "diff":
		return g.fetchPRDiff(ctx, token, owner, repo, number)
	case "json":
		return g.fetchPRJSON(ctx, token, owner, repo, number)
	default: // markdown
		return g.fetchPRMarkdown(ctx, token, owner, repo, number)
	}
}

// fetchPRDiff fetches the PR diff and wraps it in markdown for preview support
func (g *GitHubProvider) fetchPRDiff(ctx context.Context, token, owner, repo, number string) ([]byte, error) {
	// First get PR metadata for the header
	prURL := fmt.Sprintf("%s/repos/%s/%s/pulls/%s", githubAPIBase, owner, repo, number)

	var pr struct {
		Number    int    `json:"number"`
		Title     string `json:"title"`
		State     string `json:"state"`
		Body      string `json:"body"`
		HTMLURL   string `json:"html_url"`
		User      struct {
			Login string `json:"login"`
		} `json:"user"`
		Head struct {
			Ref string `json:"ref"`
		} `json:"head"`
		Base struct {
			Ref string `json:"ref"`
		} `json:"base"`
		Additions    int `json:"additions"`
		Deletions    int `json:"deletions"`
		ChangedFiles int `json:"changed_files"`
	}

	if err := g.requestURL(ctx, token, "GET", prURL, &pr); err != nil {
		return nil, err
	}

	// Fetch the actual diff
	req, err := http.NewRequestWithContext(ctx, "GET", prURL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github.diff")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("github API: %s", resp.Status)
	}

	diffContent, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Wrap in markdown with header, description, and syntax-highlighted diff
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("# PR #%d: %s\n\n", pr.Number, pr.Title))
	buf.WriteString(fmt.Sprintf("**Status:** %s | **Author:** @%s\n", pr.State, pr.User.Login))
	buf.WriteString(fmt.Sprintf("**Branch:** `%s` → `%s`\n", pr.Head.Ref, pr.Base.Ref))
	buf.WriteString(fmt.Sprintf("**Changes:** +%d -%d across %d files\n", pr.Additions, pr.Deletions, pr.ChangedFiles))
	buf.WriteString(fmt.Sprintf("**URL:** %s\n\n", pr.HTMLURL))

	// Include PR description if present
	if pr.Body != "" {
		buf.WriteString("## Description\n\n")
		buf.WriteString(pr.Body)
		buf.WriteString("\n\n")
	}

	buf.WriteString("---\n\n")
	buf.WriteString("## Diff\n\n")
	buf.WriteString("```diff\n")
	buf.Write(diffContent)
	if len(diffContent) > 0 && diffContent[len(diffContent)-1] != '\n' {
		buf.WriteString("\n")
	}
	buf.WriteString("```\n")

	return []byte(buf.String()), nil
}

// fetchPRJSON fetches PR metadata as JSON
func (g *GitHubProvider) fetchPRJSON(ctx context.Context, token, owner, repo, number string) ([]byte, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%s", githubAPIBase, owner, repo, number)

	var pr map[string]any
	if err := g.requestURL(ctx, token, "GET", url, &pr); err != nil {
		return nil, err
	}

	return json.MarshalIndent(pr, "", "  ")
}

// fetchPRMarkdown fetches PR as formatted markdown
func (g *GitHubProvider) fetchPRMarkdown(ctx context.Context, token, owner, repo, number string) ([]byte, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%s", githubAPIBase, owner, repo, number)

	var pr struct {
		Number    int    `json:"number"`
		Title     string `json:"title"`
		State     string `json:"state"`
		Body      string `json:"body"`
		HTMLURL   string `json:"html_url"`
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
		User      struct {
			Login string `json:"login"`
		} `json:"user"`
		Head struct {
			Ref string `json:"ref"`
		} `json:"head"`
		Base struct {
			Ref string `json:"ref"`
		} `json:"base"`
		Additions    int `json:"additions"`
		Deletions    int `json:"deletions"`
		ChangedFiles int `json:"changed_files"`
	}

	if err := g.requestURL(ctx, token, "GET", url, &pr); err != nil {
		return nil, err
	}

	// Format as markdown
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("# PR #%d: %s\n\n", pr.Number, pr.Title))
	buf.WriteString(fmt.Sprintf("**Status:** %s\n", pr.State))
	buf.WriteString(fmt.Sprintf("**Author:** @%s\n", pr.User.Login))
	buf.WriteString(fmt.Sprintf("**Branch:** `%s` → `%s`\n", pr.Head.Ref, pr.Base.Ref))
	buf.WriteString(fmt.Sprintf("**Changes:** +%d -%d in %d files\n", pr.Additions, pr.Deletions, pr.ChangedFiles))
	buf.WriteString(fmt.Sprintf("**URL:** %s\n\n", pr.HTMLURL))
	buf.WriteString("---\n\n")
	if pr.Body != "" {
		buf.WriteString(pr.Body)
	} else {
		buf.WriteString("*No description provided.*")
	}
	buf.WriteString("\n")

	return []byte(buf.String()), nil
}

// fetchIssueContent fetches issue content in the specified format
func (g *GitHubProvider) fetchIssueContent(ctx context.Context, token, owner, repo, number, contentType string) ([]byte, error) {
	if contentType == "json" {
		return g.fetchIssueJSON(ctx, token, owner, repo, number)
	}
	return g.fetchIssueMarkdown(ctx, token, owner, repo, number)
}

// fetchIssueJSON fetches issue metadata as JSON
func (g *GitHubProvider) fetchIssueJSON(ctx context.Context, token, owner, repo, number string) ([]byte, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/issues/%s", githubAPIBase, owner, repo, number)

	var issue map[string]any
	if err := g.requestURL(ctx, token, "GET", url, &issue); err != nil {
		return nil, err
	}

	return json.MarshalIndent(issue, "", "  ")
}

// fetchIssueMarkdown fetches issue as formatted markdown
func (g *GitHubProvider) fetchIssueMarkdown(ctx context.Context, token, owner, repo, number string) ([]byte, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/issues/%s", githubAPIBase, owner, repo, number)

	var issue struct {
		Number    int    `json:"number"`
		Title     string `json:"title"`
		State     string `json:"state"`
		Body      string `json:"body"`
		HTMLURL   string `json:"html_url"`
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
		User      struct {
			Login string `json:"login"`
		} `json:"user"`
		Labels []struct {
			Name string `json:"name"`
		} `json:"labels"`
		Comments int `json:"comments"`
	}

	if err := g.requestURL(ctx, token, "GET", url, &issue); err != nil {
		return nil, err
	}

	// Format as markdown
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("# Issue #%d: %s\n\n", issue.Number, issue.Title))
	buf.WriteString(fmt.Sprintf("**Status:** %s\n", issue.State))
	buf.WriteString(fmt.Sprintf("**Author:** @%s\n", issue.User.Login))
	if len(issue.Labels) > 0 {
		labels := make([]string, len(issue.Labels))
		for i, l := range issue.Labels {
			labels[i] = l.Name
		}
		buf.WriteString(fmt.Sprintf("**Labels:** %s\n", strings.Join(labels, ", ")))
	}
	buf.WriteString(fmt.Sprintf("**Comments:** %d\n", issue.Comments))
	buf.WriteString(fmt.Sprintf("**URL:** %s\n\n", issue.HTMLURL))
	buf.WriteString("---\n\n")
	if issue.Body != "" {
		buf.WriteString(issue.Body)
	} else {
		buf.WriteString("*No description provided.*")
	}
	buf.WriteString("\n")

	return []byte(buf.String()), nil
}

// ============================================================================
// New Content Fetchers
// ============================================================================

// fetchRepoMarkdown fetches repo info as nicely formatted markdown
func (g *GitHubProvider) fetchRepoMarkdown(ctx context.Context, token, fullName string) ([]byte, error) {
	url := fmt.Sprintf("%s/repos/%s", githubAPIBase, fullName)

	var repo struct {
		Name            string `json:"name"`
		FullName        string `json:"full_name"`
		Description     string `json:"description"`
		Private         bool   `json:"private"`
		HTMLURL         string `json:"html_url"`
		Language        string `json:"language"`
		StargazersCount int    `json:"stargazers_count"`
		ForksCount      int    `json:"forks_count"`
		OpenIssuesCount int    `json:"open_issues_count"`
		DefaultBranch   string `json:"default_branch"`
		CreatedAt       string `json:"created_at"`
		UpdatedAt       string `json:"updated_at"`
		Topics          []string `json:"topics"`
		License         *struct {
			Name string `json:"name"`
		} `json:"license"`
		Owner struct {
			Login string `json:"login"`
		} `json:"owner"`
	}

	if err := g.requestURL(ctx, token, "GET", url, &repo); err != nil {
		return nil, err
	}

	// Try to fetch README
	readmeContent := ""
	readmeURL := fmt.Sprintf("%s/repos/%s/readme", githubAPIBase, fullName)
	var readme struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	if err := g.requestURL(ctx, token, "GET", readmeURL, &readme); err == nil && readme.Encoding == "base64" {
		if decoded, err := base64Decode(readme.Content); err == nil {
			readmeContent = string(decoded)
		}
	}

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("# %s\n\n", repo.FullName))

	if repo.Description != "" {
		buf.WriteString(fmt.Sprintf("> %s\n\n", repo.Description))
	}

	visibility := "Public"
	if repo.Private {
		visibility = "Private"
	}

	buf.WriteString(fmt.Sprintf("**Owner:** @%s | **Visibility:** %s\n", repo.Owner.Login, visibility))
	buf.WriteString(fmt.Sprintf("**Language:** %s | **Default Branch:** `%s`\n", repo.Language, repo.DefaultBranch))
	buf.WriteString(fmt.Sprintf("**Stars:** %d | **Forks:** %d | **Open Issues:** %d\n", repo.StargazersCount, repo.ForksCount, repo.OpenIssuesCount))

	if repo.License != nil {
		buf.WriteString(fmt.Sprintf("**License:** %s\n", repo.License.Name))
	}

	if len(repo.Topics) > 0 {
		buf.WriteString(fmt.Sprintf("**Topics:** %s\n", strings.Join(repo.Topics, ", ")))
	}

	buf.WriteString(fmt.Sprintf("**URL:** %s\n\n", repo.HTMLURL))

	if readmeContent != "" {
		buf.WriteString("---\n\n")
		buf.WriteString("## README\n\n")
		buf.WriteString(readmeContent)
	}

	return []byte(buf.String()), nil
}

// fetchCommitContent fetches commit info
func (g *GitHubProvider) fetchCommitContent(ctx context.Context, token, owner, repo, sha, contentType string) ([]byte, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/commits/%s", githubAPIBase, owner, repo, sha)

	if contentType == "json" {
		var commit map[string]any
		if err := g.requestURL(ctx, token, "GET", url, &commit); err != nil {
			return nil, err
		}
		return json.MarshalIndent(commit, "", "  ")
	}

	// Fetch as markdown with diff
	var commit struct {
		SHA     string `json:"sha"`
		HTMLURL string `json:"html_url"`
		Commit  struct {
			Message string `json:"message"`
			Author  struct {
				Name  string `json:"name"`
				Email string `json:"email"`
				Date  string `json:"date"`
			} `json:"author"`
		} `json:"commit"`
		Author *struct {
			Login string `json:"login"`
		} `json:"author"`
		Stats struct {
			Additions int `json:"additions"`
			Deletions int `json:"deletions"`
			Total     int `json:"total"`
		} `json:"stats"`
		Files []struct {
			Filename  string `json:"filename"`
			Status    string `json:"status"`
			Additions int    `json:"additions"`
			Deletions int    `json:"deletions"`
			Patch     string `json:"patch"`
		} `json:"files"`
	}

	if err := g.requestURL(ctx, token, "GET", url, &commit); err != nil {
		return nil, err
	}

	var buf strings.Builder
	shaShort := commit.SHA
	if len(shaShort) > 7 {
		shaShort = shaShort[:7]
	}

	buf.WriteString(fmt.Sprintf("# Commit %s\n\n", shaShort))
	buf.WriteString(fmt.Sprintf("**Message:** %s\n\n", commit.Commit.Message))

	author := commit.Commit.Author.Name
	if commit.Author != nil && commit.Author.Login != "" {
		author = "@" + commit.Author.Login
	}
	buf.WriteString(fmt.Sprintf("**Author:** %s\n", author))
	buf.WriteString(fmt.Sprintf("**Date:** %s\n", commit.Commit.Author.Date))
	buf.WriteString(fmt.Sprintf("**Changes:** +%d -%d (%d total)\n", commit.Stats.Additions, commit.Stats.Deletions, commit.Stats.Total))
	buf.WriteString(fmt.Sprintf("**URL:** %s\n\n", commit.HTMLURL))

	if contentType == "diff" && len(commit.Files) > 0 {
		buf.WriteString("---\n\n## Files Changed\n\n")
		for _, f := range commit.Files {
			buf.WriteString(fmt.Sprintf("### %s (%s: +%d -%d)\n\n", f.Filename, f.Status, f.Additions, f.Deletions))
			if f.Patch != "" {
				buf.WriteString("```diff\n")
				buf.WriteString(f.Patch)
				buf.WriteString("\n```\n\n")
			}
		}
	} else if len(commit.Files) > 0 {
		buf.WriteString("---\n\n## Files Changed\n\n")
		for _, f := range commit.Files {
			buf.WriteString(fmt.Sprintf("- `%s` (%s: +%d -%d)\n", f.Filename, f.Status, f.Additions, f.Deletions))
		}
	}

	return []byte(buf.String()), nil
}

// fetchReleaseContent fetches release info
func (g *GitHubProvider) fetchReleaseContent(ctx context.Context, token, owner, repo, tag, contentType string) ([]byte, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/releases/tags/%s", githubAPIBase, owner, repo, tag)

	if contentType == "json" {
		var release map[string]any
		if err := g.requestURL(ctx, token, "GET", url, &release); err != nil {
			return nil, err
		}
		return json.MarshalIndent(release, "", "  ")
	}

	var release struct {
		TagName     string `json:"tag_name"`
		Name        string `json:"name"`
		Body        string `json:"body"`
		Draft       bool   `json:"draft"`
		Prerelease  bool   `json:"prerelease"`
		HTMLURL     string `json:"html_url"`
		PublishedAt string `json:"published_at"`
		Author      struct {
			Login string `json:"login"`
		} `json:"author"`
		Assets []struct {
			Name          string `json:"name"`
			Size          int64  `json:"size"`
			DownloadCount int    `json:"download_count"`
			DownloadURL   string `json:"browser_download_url"`
		} `json:"assets"`
	}

	if err := g.requestURL(ctx, token, "GET", url, &release); err != nil {
		return nil, err
	}

	var buf strings.Builder
	name := release.Name
	if name == "" {
		name = release.TagName
	}

	buf.WriteString(fmt.Sprintf("# Release: %s\n\n", name))
	buf.WriteString(fmt.Sprintf("**Tag:** `%s`\n", release.TagName))
	buf.WriteString(fmt.Sprintf("**Author:** @%s\n", release.Author.Login))
	buf.WriteString(fmt.Sprintf("**Published:** %s\n", release.PublishedAt))

	status := "Stable"
	if release.Draft {
		status = "Draft"
	} else if release.Prerelease {
		status = "Pre-release"
	}
	buf.WriteString(fmt.Sprintf("**Status:** %s\n", status))
	buf.WriteString(fmt.Sprintf("**URL:** %s\n\n", release.HTMLURL))

	if release.Body != "" {
		buf.WriteString("---\n\n## Release Notes\n\n")
		buf.WriteString(release.Body)
		buf.WriteString("\n\n")
	}

	if len(release.Assets) > 0 {
		buf.WriteString("---\n\n## Assets\n\n")
		for _, a := range release.Assets {
			sizeMB := float64(a.Size) / (1024 * 1024)
			buf.WriteString(fmt.Sprintf("- **%s** (%.2f MB, %d downloads)\n", a.Name, sizeMB, a.DownloadCount))
		}
	}

	return []byte(buf.String()), nil
}

// fetchWorkflowContent fetches workflow run info
func (g *GitHubProvider) fetchWorkflowContent(ctx context.Context, token, owner, repo, runID, contentType string) ([]byte, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/actions/runs/%s", githubAPIBase, owner, repo, runID)

	if contentType == "json" {
		var run map[string]any
		if err := g.requestURL(ctx, token, "GET", url, &run); err != nil {
			return nil, err
		}
		return json.MarshalIndent(run, "", "  ")
	}

	var run struct {
		ID           int64  `json:"id"`
		Name         string `json:"name"`
		HeadBranch   string `json:"head_branch"`
		HeadSHA      string `json:"head_sha"`
		Status       string `json:"status"`
		Conclusion   string `json:"conclusion"`
		HTMLURL      string `json:"html_url"`
		Event        string `json:"event"`
		RunNumber    int    `json:"run_number"`
		RunAttempt   int    `json:"run_attempt"`
		CreatedAt    string `json:"created_at"`
		UpdatedAt    string `json:"updated_at"`
		RunStartedAt string `json:"run_started_at"`
		Actor        struct {
			Login string `json:"login"`
		} `json:"actor"`
		TriggeringActor struct {
			Login string `json:"login"`
		} `json:"triggering_actor"`
	}

	if err := g.requestURL(ctx, token, "GET", url, &run); err != nil {
		return nil, err
	}

	// Fetch jobs for this run
	jobsURL := fmt.Sprintf("%s/repos/%s/%s/actions/runs/%s/jobs", githubAPIBase, owner, repo, runID)
	var jobsResp struct {
		Jobs []struct {
			ID          int64  `json:"id"`
			Name        string `json:"name"`
			Status      string `json:"status"`
			Conclusion  string `json:"conclusion"`
			StartedAt   string `json:"started_at"`
			CompletedAt string `json:"completed_at"`
			Steps       []struct {
				Name       string `json:"name"`
				Status     string `json:"status"`
				Conclusion string `json:"conclusion"`
				Number     int    `json:"number"`
			} `json:"steps"`
		} `json:"jobs"`
	}
	g.requestURL(ctx, token, "GET", jobsURL, &jobsResp)

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("# Workflow Run: %s\n\n", run.Name))

	status := run.Status
	if run.Conclusion != "" {
		status = run.Conclusion
	}

	statusEmoji := "⏳"
	switch status {
	case "success":
		statusEmoji = "✅"
	case "failure":
		statusEmoji = "❌"
	case "cancelled":
		statusEmoji = "⛔"
	case "skipped":
		statusEmoji = "⏭️"
	}

	buf.WriteString(fmt.Sprintf("**Status:** %s %s\n", statusEmoji, status))
	buf.WriteString(fmt.Sprintf("**Run:** #%d (attempt %d)\n", run.RunNumber, run.RunAttempt))
	buf.WriteString(fmt.Sprintf("**Branch:** `%s`\n", run.HeadBranch))
	buf.WriteString(fmt.Sprintf("**Commit:** `%s`\n", run.HeadSHA[:7]))
	buf.WriteString(fmt.Sprintf("**Event:** %s\n", run.Event))
	buf.WriteString(fmt.Sprintf("**Triggered by:** @%s\n", run.TriggeringActor.Login))
	buf.WriteString(fmt.Sprintf("**Started:** %s\n", run.RunStartedAt))
	buf.WriteString(fmt.Sprintf("**URL:** %s\n\n", run.HTMLURL))

	if len(jobsResp.Jobs) > 0 {
		buf.WriteString("---\n\n## Jobs\n\n")
		for _, job := range jobsResp.Jobs {
			jobStatus := job.Status
			if job.Conclusion != "" {
				jobStatus = job.Conclusion
			}
			jobEmoji := "⏳"
			switch jobStatus {
			case "success":
				jobEmoji = "✅"
			case "failure":
				jobEmoji = "❌"
			case "cancelled":
				jobEmoji = "⛔"
			case "skipped":
				jobEmoji = "⏭️"
			}

			buf.WriteString(fmt.Sprintf("### %s %s\n\n", jobEmoji, job.Name))
			if len(job.Steps) > 0 {
				for _, step := range job.Steps {
					stepStatus := step.Status
					if step.Conclusion != "" {
						stepStatus = step.Conclusion
					}
					stepEmoji := "⏳"
					switch stepStatus {
					case "success":
						stepEmoji = "✅"
					case "failure":
						stepEmoji = "❌"
					case "skipped":
						stepEmoji = "⏭️"
					}
					buf.WriteString(fmt.Sprintf("- %s %s\n", stepEmoji, step.Name))
				}
				buf.WriteString("\n")
			}
		}
	}

	return []byte(buf.String()), nil
}

// fetchFileContent fetches file content
func (g *GitHubProvider) fetchFileContent(ctx context.Context, token, owner, repo, ref, path, contentType string) ([]byte, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/contents/%s?ref=%s", githubAPIBase, owner, repo, path, ref)

	var file struct {
		Name     string `json:"name"`
		Path     string `json:"path"`
		SHA      string `json:"sha"`
		Size     int64  `json:"size"`
		Type     string `json:"type"`
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
		HTMLURL  string `json:"html_url"`
	}

	if err := g.requestURL(ctx, token, "GET", url, &file); err != nil {
		return nil, err
	}

	if file.Type == "dir" {
		return nil, fmt.Errorf("path is a directory, not a file")
	}

	// Decode content
	content := []byte{}
	if file.Encoding == "base64" {
		var err error
		content, err = base64Decode(file.Content)
		if err != nil {
			return nil, fmt.Errorf("failed to decode file content: %w", err)
		}
	}

	if contentType == "raw" {
		return content, nil
	}

	// Wrap in markdown
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("# %s\n\n", file.Path))
	buf.WriteString(fmt.Sprintf("**Size:** %d bytes | **SHA:** `%s`\n", file.Size, file.SHA[:7]))
	buf.WriteString(fmt.Sprintf("**URL:** %s\n\n", file.HTMLURL))
	buf.WriteString("---\n\n")

	// Detect language for syntax highlighting
	lang := detectLanguage(file.Name)
	buf.WriteString(fmt.Sprintf("```%s\n", lang))
	buf.Write(content)
	if len(content) > 0 && content[len(content)-1] != '\n' {
		buf.WriteString("\n")
	}
	buf.WriteString("```\n")

	return []byte(buf.String()), nil
}

// fetchBranchContent fetches branch info
func (g *GitHubProvider) fetchBranchContent(ctx context.Context, token, owner, repo, branch string) ([]byte, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/branches/%s", githubAPIBase, owner, repo, branch)

	var branchInfo struct {
		Name      string `json:"name"`
		Protected bool   `json:"protected"`
		Commit    struct {
			SHA    string `json:"sha"`
			Commit struct {
				Message string `json:"message"`
				Author  struct {
					Name string `json:"name"`
					Date string `json:"date"`
				} `json:"author"`
			} `json:"commit"`
			Author *struct {
				Login string `json:"login"`
			} `json:"author"`
		} `json:"commit"`
		Protection *struct {
			RequiredStatusChecks *struct {
				Strict   bool     `json:"strict"`
				Contexts []string `json:"contexts"`
			} `json:"required_status_checks"`
			RequiredPullRequestReviews *struct {
				RequiredApprovingReviewCount int `json:"required_approving_review_count"`
			} `json:"required_pull_request_reviews"`
		} `json:"protection"`
	}

	if err := g.requestURL(ctx, token, "GET", url, &branchInfo); err != nil {
		return nil, err
	}

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("# Branch: %s\n\n", branchInfo.Name))

	protectedStr := "No"
	if branchInfo.Protected {
		protectedStr = "Yes"
	}
	buf.WriteString(fmt.Sprintf("**Protected:** %s\n\n", protectedStr))

	buf.WriteString("## Latest Commit\n\n")
	shaShort := branchInfo.Commit.SHA
	if len(shaShort) > 7 {
		shaShort = shaShort[:7]
	}
	buf.WriteString(fmt.Sprintf("**SHA:** `%s`\n", shaShort))

	author := branchInfo.Commit.Commit.Author.Name
	if branchInfo.Commit.Author != nil && branchInfo.Commit.Author.Login != "" {
		author = "@" + branchInfo.Commit.Author.Login
	}
	buf.WriteString(fmt.Sprintf("**Author:** %s\n", author))
	buf.WriteString(fmt.Sprintf("**Date:** %s\n", branchInfo.Commit.Commit.Author.Date))
	buf.WriteString(fmt.Sprintf("**Message:** %s\n", branchInfo.Commit.Commit.Message))

	if branchInfo.Protected && branchInfo.Protection != nil {
		buf.WriteString("\n## Protection Rules\n\n")
		if branchInfo.Protection.RequiredStatusChecks != nil {
			buf.WriteString("**Required Status Checks:**\n")
			if branchInfo.Protection.RequiredStatusChecks.Strict {
				buf.WriteString("- Require branches to be up to date\n")
			}
			for _, ctx := range branchInfo.Protection.RequiredStatusChecks.Contexts {
				buf.WriteString(fmt.Sprintf("- %s\n", ctx))
			}
		}
		if branchInfo.Protection.RequiredPullRequestReviews != nil {
			buf.WriteString(fmt.Sprintf("**Required Approvals:** %d\n",
				branchInfo.Protection.RequiredPullRequestReviews.RequiredApprovingReviewCount))
		}
	}

	return []byte(buf.String()), nil
}

// base64Decode decodes a base64 string, handling line breaks
func base64Decode(s string) ([]byte, error) {
	// Remove line breaks that GitHub adds
	s = strings.ReplaceAll(s, "\n", "")
	s = strings.ReplaceAll(s, "\r", "")

	// Standard library base64 decode
	return base64StdDecode(s)
}

// base64StdDecode is a simple base64 decoder
func base64StdDecode(s string) ([]byte, error) {
	const base64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	
	// Build lookup table
	lookup := make([]int, 256)
	for i := range lookup {
		lookup[i] = -1
	}
	for i, c := range base64Chars {
		lookup[c] = i
	}
	lookup['='] = 0

	// Decode
	result := make([]byte, 0, len(s)*3/4)
	var buffer uint32
	var bufferLen int

	for _, c := range s {
		if c == '=' {
			break
		}
		val := lookup[c]
		if val < 0 {
			continue // Skip invalid chars
		}
		buffer = (buffer << 6) | uint32(val)
		bufferLen += 6
		if bufferLen >= 8 {
			bufferLen -= 8
			result = append(result, byte(buffer>>bufferLen))
			buffer &= (1 << bufferLen) - 1
		}
	}

	return result, nil
}

// detectLanguage detects programming language from filename
func detectLanguage(filename string) string {
	ext := strings.ToLower(filename)
	if idx := strings.LastIndex(ext, "."); idx >= 0 {
		ext = ext[idx+1:]
	}

	langMap := map[string]string{
		"go":    "go",
		"py":    "python",
		"js":    "javascript",
		"ts":    "typescript",
		"jsx":   "jsx",
		"tsx":   "tsx",
		"rs":    "rust",
		"rb":    "ruby",
		"java":  "java",
		"c":     "c",
		"cpp":   "cpp",
		"h":     "c",
		"hpp":   "cpp",
		"cs":    "csharp",
		"php":   "php",
		"swift": "swift",
		"kt":    "kotlin",
		"scala": "scala",
		"sh":    "bash",
		"bash":  "bash",
		"zsh":   "bash",
		"yml":   "yaml",
		"yaml":  "yaml",
		"json":  "json",
		"xml":   "xml",
		"html":  "html",
		"css":   "css",
		"scss":  "scss",
		"less":  "less",
		"sql":   "sql",
		"md":    "markdown",
		"txt":   "text",
		"toml":  "toml",
		"ini":   "ini",
		"cfg":   "ini",
		"conf":  "ini",
		"dockerfile": "dockerfile",
		"makefile":   "makefile",
	}

	// Check for special filenames
	lower := strings.ToLower(filename)
	if lower == "dockerfile" {
		return "dockerfile"
	}
	if lower == "makefile" {
		return "makefile"
	}

	if lang, ok := langMap[ext]; ok {
		return lang
	}
	return ""
}

// FormatFilename generates a filename from metadata using the format template.
func (g *GitHubProvider) FormatFilename(format string, metadata map[string]string) string {
	result := format
	for key, value := range metadata {
		placeholder := "{" + key + "}"
		result = strings.ReplaceAll(result, placeholder, sources.SanitizeFilename(value))
	}
	// Remove any remaining placeholders
	for strings.Contains(result, "{") && strings.Contains(result, "}") {
		start := strings.Index(result, "{")
		end := strings.Index(result, "}")
		if start < end {
			result = result[:start] + result[end+1:]
		} else {
			break
		}
	}
	return result
}

// requestURL makes an HTTP request to the given URL
func (g *GitHubProvider) requestURL(ctx context.Context, token, method, url string, result any) error {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		var ghErr struct {
			Message string `json:"message"`
		}
		json.Unmarshal(body, &ghErr)
		if ghErr.Message != "" {
			return fmt.Errorf("github API: %s", ghErr.Message)
		}
		return fmt.Errorf("github API: %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// formatInt formats a number avoiding scientific notation
func formatInt(v any) string {
	switch n := v.(type) {
	case float64:
		return fmt.Sprintf("%d", int64(n))
	case int:
		return fmt.Sprintf("%d", n)
	case int64:
		return fmt.Sprintf("%d", n)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// urlEncode encodes a string for use in a URL query parameter
func urlEncode(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, " ", "+"), ":", "%3A")
}

// extractOwnerRepo extracts owner and repo from a repository_url
func extractOwnerRepo(url string) (owner, repo string) {
	// URL format: https://api.github.com/repos/owner/repo
	parts := strings.Split(url, "/repos/")
	if len(parts) == 2 {
		ownerRepo := strings.Split(parts[1], "/")
		if len(ownerRepo) >= 2 {
			return ownerRepo[0], ownerRepo[1]
		}
	}
	return "unknown", "unknown"
}
