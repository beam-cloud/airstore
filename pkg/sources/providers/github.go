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
	return "github"
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
		"name":         result["name"],
		"full_name":    result["full_name"],
		"description":  result["description"],
		"private":      result["private"],
		"language":     result["language"],
		"stars":        result["stargazers_count"],
		"forks":        result["forks_count"],
		"open_issues":  result["open_issues_count"],
		"default_branch": result["default_branch"],
		"url":          result["html_url"],
		"clone_url":    result["clone_url"],
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
