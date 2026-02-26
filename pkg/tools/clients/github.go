package clients

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	githubAPIBase        = "https://api.github.com"
	githubCmdListRepos   = "list-repos"
	githubCmdGetRepo     = "get-repo"
	githubCmdListPRs     = "list-prs"
	githubCmdGetPR       = "get-pr"
	githubCmdListPRFiles = "list-pr-files"
	githubCmdCommentPR   = "comment-pr"
	githubCmdReviewPR    = "review-pr"
	githubCmdListIssues  = "list-issues"
	githubCmdGetIssue    = "get-issue"
	githubCmdCreateIssue = "create-issue"
)

type GitHubClient struct {
	api *oauthHTTPClient
}

func NewGitHubClient() *GitHubClient {
	return &GitHubClient{
		api: newOAuthHTTPClient("github", githubAPIBase, map[string]string{
			"Accept":               "application/vnd.github+json",
			"X-GitHub-Api-Version": "2022-11-28",
		}),
	}
}

func (g *GitHubClient) Name() types.IntegrationName {
	return types.GitHub
}

func (g *GitHubClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, _ io.Writer) error {
	return ExecuteOAuthCommand(ctx, "github", command, args, creds, map[string]OAuthCommandHandler{
		githubCmdListRepos: func(ctx context.Context, token string, args map[string]any) (any, error) {
			owner := GetStringArg(args, "owner", "")
			limit := GetIntArg(args, "limit", 30)
			return g.listRepos(ctx, token, owner, limit)
		},
		githubCmdGetRepo: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "owner", "repo")
			if err != nil {
				return nil, err
			}
			return g.getRepo(ctx, token, required["owner"], required["repo"])
		},
		githubCmdListPRs: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "owner", "repo")
			if err != nil {
				return nil, err
			}
			state := GetStringArg(args, "state", "open")
			limit := GetIntArg(args, "limit", 30)
			return g.listPRs(ctx, token, required["owner"], required["repo"], state, limit)
		},
		githubCmdGetPR: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "owner", "repo")
			if err != nil {
				return nil, err
			}
			number, err := RequirePositiveIntArg(args, "number")
			if err != nil {
				return nil, err
			}
			return g.getPR(ctx, token, required["owner"], required["repo"], number)
		},
		githubCmdListPRFiles: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "owner", "repo")
			if err != nil {
				return nil, err
			}
			number, err := RequirePositiveIntArg(args, "number")
			if err != nil {
				return nil, err
			}
			limit := GetIntArg(args, "limit", 100)
			return g.listPRFiles(ctx, token, required["owner"], required["repo"], number, limit)
		},
		githubCmdCommentPR: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "owner", "repo", "body")
			if err != nil {
				return nil, err
			}
			number, err := RequirePositiveIntArg(args, "number")
			if err != nil {
				return nil, err
			}
			return g.commentPR(ctx, token, required["owner"], required["repo"], number, required["body"])
		},
		githubCmdReviewPR: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "owner", "repo", "body")
			if err != nil {
				return nil, err
			}
			number, err := RequirePositiveIntArg(args, "number")
			if err != nil {
				return nil, err
			}
			event := GetStringArg(args, "event", "COMMENT")
			return g.reviewPR(ctx, token, required["owner"], required["repo"], number, required["body"], event)
		},
		githubCmdListIssues: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "owner", "repo")
			if err != nil {
				return nil, err
			}
			state := GetStringArg(args, "state", "open")
			limit := GetIntArg(args, "limit", 30)
			return g.listIssues(ctx, token, required["owner"], required["repo"], state, limit)
		},
		githubCmdGetIssue: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "owner", "repo")
			if err != nil {
				return nil, err
			}
			number, err := RequirePositiveIntArg(args, "number")
			if err != nil {
				return nil, err
			}
			return g.getIssue(ctx, token, required["owner"], required["repo"], number)
		},
		githubCmdCreateIssue: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "owner", "repo", "title")
			if err != nil {
				return nil, err
			}
			body := GetStringArg(args, "body", "")
			return g.createIssue(ctx, token, required["owner"], required["repo"], required["title"], body)
		},
	}, stdout)
}

func (g *GitHubClient) request(ctx context.Context, token, method, path string, result any) error {
	return g.requestJSON(ctx, token, method, path, nil, result)
}

func (g *GitHubClient) requestJSON(ctx context.Context, token, method, path string, payload any, result any) error {
	return g.api.RequestJSON(ctx, token, method, path, payload, result)
}

// API methods

func (g *GitHubClient) listRepos(ctx context.Context, token, owner string, limit int) (any, error) {
	var repos []RepoInfo

	// If owner specified, get their repos
	path := "/user/repos?per_page=" + fmt.Sprint(limit) + "&sort=updated"
	if owner != "" {
		path = "/users/" + owner + "/repos?per_page=" + fmt.Sprint(limit) + "&sort=updated"
	}

	var rawRepos []map[string]any
	if err := g.request(ctx, token, "GET", path, &rawRepos); err != nil {
		return nil, err
	}

	for _, r := range rawRepos {
		repos = append(repos, RepoInfo{
			Name:        getString(r, "name"),
			FullName:    getString(r, "full_name"),
			Description: getString(r, "description"),
			Private:     getBool(r, "private"),
			Language:    getString(r, "language"),
			Stars:       getInt(r, "stargazers_count"),
			URL:         getString(r, "html_url"),
		})
	}

	return map[string]any{
		"repos": repos,
		"count": len(repos),
	}, nil
}

func (g *GitHubClient) getRepo(ctx context.Context, token, owner, repo string) (any, error) {
	var result map[string]any
	if err := g.request(ctx, token, "GET", "/repos/"+owner+"/"+repo, &result); err != nil {
		return nil, err
	}

	return RepoInfo{
		Name:        getString(result, "name"),
		FullName:    getString(result, "full_name"),
		Description: getString(result, "description"),
		Private:     getBool(result, "private"),
		Language:    getString(result, "language"),
		Stars:       getInt(result, "stargazers_count"),
		Forks:       getInt(result, "forks_count"),
		OpenIssues:  getInt(result, "open_issues_count"),
		URL:         getString(result, "html_url"),
		CloneURL:    getString(result, "clone_url"),
	}, nil
}

func (g *GitHubClient) listPRs(ctx context.Context, token, owner, repo, state string, limit int) (any, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls?state=%s&per_page=%d", owner, repo, state, limit)

	var rawPRs []map[string]any
	if err := g.request(ctx, token, "GET", path, &rawPRs); err != nil {
		return nil, err
	}

	var prs []PRInfo
	for _, p := range rawPRs {
		user := ""
		if u, ok := p["user"].(map[string]any); ok {
			user = getString(u, "login")
		}
		prs = append(prs, PRInfo{
			Number: getInt(p, "number"),
			Title:  getString(p, "title"),
			State:  getString(p, "state"),
			User:   user,
			Draft:  getBool(p, "draft"),
			URL:    getString(p, "html_url"),
		})
	}

	return map[string]any{
		"owner":         owner,
		"repo":          repo,
		"pull_requests": prs,
		"count":         len(prs),
	}, nil
}

func (g *GitHubClient) getPR(ctx context.Context, token, owner, repo string, number int) (any, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, number)

	var result map[string]any
	if err := g.request(ctx, token, "GET", path, &result); err != nil {
		return nil, err
	}

	user := ""
	if u, ok := result["user"].(map[string]any); ok {
		user = getString(u, "login")
	}

	return PRInfo{
		Number:    getInt(result, "number"),
		Title:     getString(result, "title"),
		State:     getString(result, "state"),
		Body:      getString(result, "body"),
		User:      user,
		Draft:     getBool(result, "draft"),
		Mergeable: getBool(result, "mergeable"),
		URL:       getString(result, "html_url"),
	}, nil
}

func (g *GitHubClient) listPRFiles(ctx context.Context, token, owner, repo string, number, limit int) (any, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/files?per_page=%d", owner, repo, number, limit)
	var rawFiles []map[string]any
	if err := g.request(ctx, token, "GET", path, &rawFiles); err != nil {
		return nil, err
	}

	files := make([]PRFileInfo, 0, len(rawFiles))
	for _, file := range rawFiles {
		files = append(files, PRFileInfo{
			Filename:  getString(file, "filename"),
			Status:    getString(file, "status"),
			Additions: getInt(file, "additions"),
			Deletions: getInt(file, "deletions"),
			Changes:   getInt(file, "changes"),
			Patch:     getString(file, "patch"),
			BlobURL:   getString(file, "blob_url"),
		})
	}

	return map[string]any{
		"owner":  owner,
		"repo":   repo,
		"number": number,
		"files":  files,
		"count":  len(files),
	}, nil
}

func (g *GitHubClient) commentPR(ctx context.Context, token, owner, repo string, number int, body string) (any, error) {
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/comments", owner, repo, number)
	payload := map[string]any{"body": body}

	var result map[string]any
	if err := g.requestJSON(ctx, token, "POST", path, payload, &result); err != nil {
		return nil, err
	}

	return map[string]any{
		"owner":      owner,
		"repo":       repo,
		"number":     number,
		"comment_id": getInt(result, "id"),
		"url":        getString(result, "html_url"),
		"created_at": getString(result, "created_at"),
	}, nil
}

func (g *GitHubClient) reviewPR(ctx context.Context, token, owner, repo string, number int, body, event string) (any, error) {
	normalizedEvent, err := normalizeReviewEvent(event)
	if err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews", owner, repo, number)
	payload := map[string]any{
		"body":  body,
		"event": normalizedEvent,
	}

	var result map[string]any
	if err := g.requestJSON(ctx, token, "POST", path, payload, &result); err != nil {
		return nil, err
	}

	return map[string]any{
		"owner":        owner,
		"repo":         repo,
		"number":       number,
		"review_id":    getInt(result, "id"),
		"event":        normalizedEvent,
		"state":        getString(result, "state"),
		"url":          getString(result, "html_url"),
		"submitted_at": getString(result, "submitted_at"),
	}, nil
}

func (g *GitHubClient) listIssues(ctx context.Context, token, owner, repo, state string, limit int) (any, error) {
	path := fmt.Sprintf("/repos/%s/%s/issues?state=%s&per_page=%d", owner, repo, state, limit)

	var rawIssues []map[string]any
	if err := g.request(ctx, token, "GET", path, &rawIssues); err != nil {
		return nil, err
	}

	var issues []IssueInfo
	for _, i := range rawIssues {
		// Skip PRs (they appear in issues endpoint too)
		if _, ok := i["pull_request"]; ok {
			continue
		}
		user := ""
		if u, ok := i["user"].(map[string]any); ok {
			user = getString(u, "login")
		}
		var labels []string
		if lbls, ok := i["labels"].([]any); ok {
			for _, l := range lbls {
				if lm, ok := l.(map[string]any); ok {
					labels = append(labels, getString(lm, "name"))
				}
			}
		}
		issues = append(issues, IssueInfo{
			Number:   getInt(i, "number"),
			Title:    getString(i, "title"),
			State:    getString(i, "state"),
			User:     user,
			Labels:   labels,
			Comments: getInt(i, "comments"),
			URL:      getString(i, "html_url"),
		})
	}

	return map[string]any{
		"owner":  owner,
		"repo":   repo,
		"issues": issues,
		"count":  len(issues),
	}, nil
}

func (g *GitHubClient) getIssue(ctx context.Context, token, owner, repo string, number int) (any, error) {
	path := fmt.Sprintf("/repos/%s/%s/issues/%d", owner, repo, number)

	var result map[string]any
	if err := g.request(ctx, token, "GET", path, &result); err != nil {
		return nil, err
	}

	user := ""
	if u, ok := result["user"].(map[string]any); ok {
		user = getString(u, "login")
	}
	var labels []string
	if lbls, ok := result["labels"].([]any); ok {
		for _, l := range lbls {
			if lm, ok := l.(map[string]any); ok {
				labels = append(labels, getString(lm, "name"))
			}
		}
	}

	return IssueInfo{
		Number:   getInt(result, "number"),
		Title:    getString(result, "title"),
		State:    getString(result, "state"),
		Body:     getString(result, "body"),
		User:     user,
		Labels:   labels,
		Comments: getInt(result, "comments"),
		URL:      getString(result, "html_url"),
	}, nil
}

func (g *GitHubClient) createIssue(ctx context.Context, token, owner, repo, title, body string) (any, error) {
	path := fmt.Sprintf("/repos/%s/%s/issues", owner, repo)
	payload := map[string]any{
		"title": title,
	}
	if body != "" {
		payload["body"] = body
	}
	var result map[string]any
	if err := g.requestJSON(ctx, token, "POST", path, payload, &result); err != nil {
		return nil, err
	}

	return map[string]any{
		"owner":  owner,
		"repo":   repo,
		"number": getInt(result, "number"),
		"title":  getString(result, "title"),
		"url":    getString(result, "html_url"),
		"state":  getString(result, "state"),
	}, nil
}

// Response types

type RepoInfo struct {
	Name        string `json:"name"`
	FullName    string `json:"full_name"`
	Description string `json:"description,omitempty"`
	Private     bool   `json:"private"`
	Language    string `json:"language,omitempty"`
	Stars       int    `json:"stars"`
	Forks       int    `json:"forks,omitempty"`
	OpenIssues  int    `json:"open_issues,omitempty"`
	URL         string `json:"url"`
	CloneURL    string `json:"clone_url,omitempty"`
}

type PRInfo struct {
	Number    int    `json:"number"`
	Title     string `json:"title"`
	State     string `json:"state"`
	Body      string `json:"body,omitempty"`
	User      string `json:"user"`
	Draft     bool   `json:"draft"`
	Mergeable bool   `json:"mergeable,omitempty"`
	URL       string `json:"url"`
}

type PRFileInfo struct {
	Filename  string `json:"filename"`
	Status    string `json:"status"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
	Changes   int    `json:"changes"`
	Patch     string `json:"patch,omitempty"`
	BlobURL   string `json:"blob_url,omitempty"`
}

type IssueInfo struct {
	Number   int      `json:"number"`
	Title    string   `json:"title"`
	State    string   `json:"state"`
	Body     string   `json:"body,omitempty"`
	User     string   `json:"user"`
	Labels   []string `json:"labels,omitempty"`
	Comments int      `json:"comments"`
	URL      string   `json:"url"`
}

// Helpers

func getString(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getInt(m map[string]any, key string) int {
	if v, ok := m[key].(float64); ok {
		return int(v)
	}
	return 0
}

func getBool(m map[string]any, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func normalizeReviewEvent(event string) (string, error) {
	candidate := strings.ToUpper(strings.TrimSpace(event))
	if candidate == "" {
		candidate = "COMMENT"
	}
	switch candidate {
	case "COMMENT", "APPROVE", "REQUEST_CHANGES":
		return candidate, nil
	default:
		return "", fmt.Errorf("event must be one of COMMENT, APPROVE, REQUEST_CHANGES")
	}
}
