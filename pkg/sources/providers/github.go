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

	// Detect search type from query
	searchType := g.detectSearchType(spec.Query)

	// Get content type from metadata or default based on search type
	contentType := spec.Metadata["content_type"]
	if contentType == "" {
		if searchType == "repos" {
			contentType = "json"
		} else {
			contentType = "markdown"
		}
	}

	var results []sources.QueryResult
	var nextPageToken string
	var hasMore bool

	switch searchType {
	case "repos":
		results, nextPageToken, hasMore = g.searchRepositories(ctx, token, spec.Query, limit, page, spec.FilenameFormat, contentType)
	case "issues", "prs":
		results, nextPageToken, hasMore = g.searchIssues(ctx, token, spec.Query, searchType, limit, page, spec.FilenameFormat, contentType)
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
	if strings.Contains(q, "language:") || strings.Contains(q, "stars:") || strings.Contains(q, "forks:") || strings.Contains(q, "topic:") {
		return "repos"
	}
	if strings.Contains(q, "is:pr") {
		return "prs"
	}
	if strings.Contains(q, "is:issue") {
		return "issues"
	}
	// Default to prs if repo: is present
	if strings.Contains(q, "repo:") {
		return "prs"
	}
	return "repos"
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

// ReadResult fetches the content of an issue/PR/repo by its ID.
// ID format: "type:owner/repo#number:content_type" for issues/PRs
//
//	"repo:owner/repo" for repositories
func (g *GitHubProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	token := pctx.Credentials.AccessToken

	// Parse the result ID
	if strings.HasPrefix(resultID, "repo:") {
		// Repository content
		fullName := strings.TrimPrefix(resultID, "repo:")
		return g.fetchRepoContent(ctx, token, fullName)
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

// fetchRepoContent fetches repository metadata as JSON
func (g *GitHubProvider) fetchRepoContent(ctx context.Context, token, fullName string) ([]byte, error) {
	url := fmt.Sprintf("%s/repos/%s", githubAPIBase, fullName)

	var repo map[string]any
	if err := g.requestURL(ctx, token, "GET", url, &repo); err != nil {
		return nil, err
	}

	return json.MarshalIndent(repo, "", "  ")
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
