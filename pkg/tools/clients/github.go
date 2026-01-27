package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	githubAPIBase       = "https://api.github.com"
	githubCmdListRepos  = "list-repos"
	githubCmdGetRepo    = "get-repo"
	githubCmdListPRs    = "list-prs"
	githubCmdGetPR      = "get-pr"
	githubCmdListIssues = "list-issues"
	githubCmdGetIssue   = "get-issue"
)

type GitHubClient struct {
	httpClient *http.Client
}

func NewGitHubClient() *GitHubClient {
	return &GitHubClient{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (g *GitHubClient) Name() types.ToolName {
	return types.ToolGitHub
}

func (g *GitHubClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, stderr io.Writer) error {
	if creds == nil || creds.AccessToken == "" {
		return g.outputError(stdout, "github: not connected. Run: cli connection add <workspace> github --token <pat>")
	}

	var result any
	var err error

	switch command {
	case githubCmdListRepos:
		owner := GetStringArg(args, "owner", "")
		limit := GetIntArg(args, "limit", 30)
		result, err = g.listRepos(ctx, creds.AccessToken, owner, limit)

	case githubCmdGetRepo:
		owner := GetStringArg(args, "owner", "")
		repo := GetStringArg(args, "repo", "")
		if owner == "" || repo == "" {
			return g.outputError(stdout, "owner and repo are required")
		}
		result, err = g.getRepo(ctx, creds.AccessToken, owner, repo)

	case githubCmdListPRs:
		owner := GetStringArg(args, "owner", "")
		repo := GetStringArg(args, "repo", "")
		state := GetStringArg(args, "state", "open")
		limit := GetIntArg(args, "limit", 30)
		if owner == "" || repo == "" {
			return g.outputError(stdout, "owner and repo are required")
		}
		result, err = g.listPRs(ctx, creds.AccessToken, owner, repo, state, limit)

	case githubCmdGetPR:
		owner := GetStringArg(args, "owner", "")
		repo := GetStringArg(args, "repo", "")
		number := GetIntArg(args, "number", 0)
		if owner == "" || repo == "" || number == 0 {
			return g.outputError(stdout, "owner, repo, and number are required")
		}
		result, err = g.getPR(ctx, creds.AccessToken, owner, repo, number)

	case githubCmdListIssues:
		owner := GetStringArg(args, "owner", "")
		repo := GetStringArg(args, "repo", "")
		state := GetStringArg(args, "state", "open")
		limit := GetIntArg(args, "limit", 30)
		if owner == "" || repo == "" {
			return g.outputError(stdout, "owner and repo are required")
		}
		result, err = g.listIssues(ctx, creds.AccessToken, owner, repo, state, limit)

	case githubCmdGetIssue:
		owner := GetStringArg(args, "owner", "")
		repo := GetStringArg(args, "repo", "")
		number := GetIntArg(args, "number", 0)
		if owner == "" || repo == "" || number == 0 {
			return g.outputError(stdout, "owner, repo, and number are required")
		}
		result, err = g.getIssue(ctx, creds.AccessToken, owner, repo, number)

	default:
		return fmt.Errorf("unknown command: %s", command)
	}

	if err != nil {
		return g.outputError(stdout, err.Error())
	}

	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(result)
}

func (g *GitHubClient) outputError(w io.Writer, msg string) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(map[string]any{"error": true, "message": msg})
}

func (g *GitHubClient) request(ctx context.Context, token, method, path string, result any) error {
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
