package providers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/sources/clients"
	"github.com/beam-cloud/airstore/pkg/types"
)

// ConfluenceProvider implements sources.Provider, sources.QueryExecutor,
// sources.NativeBrowsable, and sources.ResourceLister for Confluence Cloud.
//
// Filesystem layout:
//
//	/sources/confluence/
//	├── views/
//	│   ├── recent.json
//	│   └── spaces.json
//	├── spaces/{space-key}/
//	│   ├── meta.json
//	│   └── recent-pages.json
//	├── pages/{page-id}/
//	│   ├── meta.json
//	│   ├── content.md
//	│   └── children.json
//	└── {smart-query-views}/
type ConfluenceProvider struct {
	client     *clients.ConfluenceClient
	httpClient *http.Client
}

func NewConfluenceProvider() *ConfluenceProvider {
	return &ConfluenceProvider{
		client:     clients.NewConfluenceClient(),
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

func (p *ConfluenceProvider) Name() string {
	return types.Confluence.String()
}

// IsNativeBrowsable returns true — Confluence exposes a native file tree.
func (p *ConfluenceProvider) IsNativeBrowsable() bool { return true }

// DefaultResourceType implements sources.ResourceLister.
func (p *ConfluenceProvider) DefaultResourceType() string { return "spaces" }

// ListResources implements sources.ResourceLister.
func (p *ConfluenceProvider) ListResources(ctx context.Context, pctx *sources.ProviderContext, resourceType string) ([]sources.Resource, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	spaces, err := p.client.ListSpaces(ctx, pctx.Credentials, 250)
	if err != nil {
		return nil, err
	}

	resources := make([]sources.Resource, 0, len(spaces))
	for _, s := range spaces {
		resources = append(resources, sources.Resource{
			ID:   s.Key,
			Name: fmt.Sprintf("%s (%s)", s.Name, s.Key),
		})
	}
	return resources, nil
}

// Compile-time interface checks
var (
	_ sources.Provider       = (*ConfluenceProvider)(nil)
	_ sources.QueryExecutor  = (*ConfluenceProvider)(nil)
	_ sources.NativeBrowsable = (*ConfluenceProvider)(nil)
	_ sources.ResourceLister  = (*ConfluenceProvider)(nil)
)

// ---------------------------------------------------------------------------
// Provider interface
// ---------------------------------------------------------------------------

func (p *ConfluenceProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if path == "" {
		return sources.DirInfo(), nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "views":
		return p.statViews(parts[1:])
	case "spaces":
		return p.statSpaces(ctx, pctx, parts[1:])
	case "pages":
		return p.statPages(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

func (p *ConfluenceProvider) ReadDir(ctx context.Context, pctx *sources.ProviderContext, path string) ([]sources.DirEntry, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if path == "" {
		return []sources.DirEntry{
			{Name: "views", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
			{Name: "spaces", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
			{Name: "pages", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
		}, nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "views":
		return p.readdirViews(parts[1:])
	case "spaces":
		return p.readdirSpaces(ctx, pctx, parts[1:])
	case "pages":
		return p.readdirPages(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

func (p *ConfluenceProvider) Read(ctx context.Context, pctx *sources.ProviderContext, path string, offset, length int64) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "views":
		return p.readViews(ctx, pctx, parts[1:], offset, length)
	case "spaces":
		return p.readSpaces(ctx, pctx, parts[1:], offset, length)
	case "pages":
		return p.readPages(ctx, pctx, parts[1:], offset, length)
	default:
		return nil, sources.ErrNotFound
	}
}

func (p *ConfluenceProvider) Readlink(ctx context.Context, pctx *sources.ProviderContext, path string) (string, error) {
	return "", sources.ErrNotFound
}

func (p *ConfluenceProvider) Search(ctx context.Context, pctx *sources.ProviderContext, query string, limit int) ([]sources.SearchResult, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if limit <= 0 {
		limit = 50
	}

	results, _, err := p.client.SearchCQL(ctx, pctx.Credentials, query, limit)
	if err != nil {
		return nil, err
	}

	searchResults := make([]sources.SearchResult, 0, len(results))
	for _, r := range results {
		mtime := sources.NowUnix()
		if t, err := time.Parse(time.RFC3339, r.LastModified); err == nil {
			mtime = t.Unix()
		}

		filename := fmt.Sprintf("%s_%s_%s.md",
			sources.SanitizeFilename(r.Space.Key),
			sources.SanitizeFilename(r.Title),
			r.ID,
		)

		searchResults = append(searchResults, sources.SearchResult{
			Name:    filename,
			Id:      r.ID,
			Mode:    sources.ModeFile,
			Mtime:   mtime,
			Preview: r.Excerpt,
		})
	}

	return searchResults, nil
}

// ---------------------------------------------------------------------------
// Views: /views/recent.json, /views/spaces.json
// ---------------------------------------------------------------------------

func (p *ConfluenceProvider) statViews(parts []string) (*sources.FileInfo, error) {
	if len(parts) == 0 {
		return sources.DirInfo(), nil
	}

	switch parts[0] {
	case "recent.json", "spaces.json":
		return &sources.FileInfo{
			Size:  0,
			Mode:  sources.ModeFile,
			Mtime: sources.NowUnix(),
		}, nil
	default:
		return nil, sources.ErrNotFound
	}
}

func (p *ConfluenceProvider) readdirViews(parts []string) ([]sources.DirEntry, error) {
	if len(parts) == 0 {
		return []sources.DirEntry{
			{Name: "recent.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "spaces.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil
	}
	return nil, sources.ErrNotDir
}

func (p *ConfluenceProvider) readViews(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) == 0 {
		return nil, sources.ErrIsDir
	}

	var data []byte
	var err error

	switch parts[0] {
	case "recent.json":
		data, err = p.fetchRecentPages(ctx, pctx)
	case "spaces.json":
		data, err = p.fetchSpaces(ctx, pctx)
	default:
		return nil, sources.ErrNotFound
	}

	if err != nil {
		return nil, err
	}
	return sliceData(data, offset, length), nil
}

func (p *ConfluenceProvider) fetchRecentPages(ctx context.Context, pctx *sources.ProviderContext) ([]byte, error) {
	pages, err := p.client.GetRecentPages(ctx, pctx.Credentials, 50)
	if err != nil {
		return nil, err
	}

	simplified := make([]map[string]any, 0, len(pages))
	for _, page := range pages {
		entry := map[string]any{
			"id":      page.ID,
			"title":   page.Title,
			"spaceId": page.SpaceID,
			"status":  page.Status,
		}
		if page.Version != nil {
			entry["version"] = page.Version.Number
			entry["lastModified"] = page.Version.CreatedAt
		}
		simplified = append(simplified, entry)
	}

	return jsonMarshal(map[string]any{
		"pages": simplified,
		"count": len(simplified),
	})
}

func (p *ConfluenceProvider) fetchSpaces(ctx context.Context, pctx *sources.ProviderContext) ([]byte, error) {
	spaces, err := p.client.ListSpaces(ctx, pctx.Credentials, 250)
	if err != nil {
		return nil, err
	}

	simplified := make([]map[string]any, 0, len(spaces))
	for _, s := range spaces {
		simplified = append(simplified, map[string]any{
			"id":   s.ID,
			"key":  s.Key,
			"name": s.Name,
			"type": s.Type,
		})
	}

	return jsonMarshal(map[string]any{
		"spaces": simplified,
		"count":  len(simplified),
	})
}

// ---------------------------------------------------------------------------
// Spaces: /spaces/{space-key}/meta.json, /spaces/{space-key}/recent-pages.json
// ---------------------------------------------------------------------------

func (p *ConfluenceProvider) statSpaces(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	switch len(parts) {
	case 0:
		return sources.DirInfo(), nil
	case 1:
		// /spaces/{space-key}
		return sources.DirInfo(), nil
	case 2:
		// /spaces/{space-key}/<file>
		switch parts[1] {
		case "meta.json", "recent-pages.json":
			return &sources.FileInfo{
				Size:  0,
				Mode:  sources.ModeFile,
				Mtime: sources.NowUnix(),
			}, nil
		default:
			return nil, sources.ErrNotFound
		}
	default:
		return nil, sources.ErrNotFound
	}
}

func (p *ConfluenceProvider) readdirSpaces(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	switch len(parts) {
	case 0:
		// List all spaces
		spaces, err := p.client.ListSpaces(ctx, pctx.Credentials, 250)
		if err != nil {
			return nil, err
		}

		entries := make([]sources.DirEntry, 0, len(spaces))
		for _, s := range spaces {
			entries = append(entries, sources.DirEntry{
				Name:  s.Key,
				Mode:  sources.ModeDir,
				IsDir: true,
				Mtime: sources.NowUnix(),
			})
		}
		return entries, nil

	case 1:
		// /spaces/{space-key} — list files
		return []sources.DirEntry{
			{Name: "meta.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "recent-pages.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil

	default:
		return nil, sources.ErrNotDir
	}
}

func (p *ConfluenceProvider) readSpaces(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) < 2 {
		return nil, sources.ErrIsDir
	}

	spaceKey := parts[0]
	file := parts[1]

	var data []byte
	var err error

	switch file {
	case "meta.json":
		data, err = p.fetchSpaceMeta(ctx, pctx, spaceKey)
	case "recent-pages.json":
		data, err = p.fetchSpaceRecentPages(ctx, pctx, spaceKey)
	default:
		return nil, sources.ErrNotFound
	}

	if err != nil {
		return nil, err
	}
	return sliceData(data, offset, length), nil
}

func (p *ConfluenceProvider) fetchSpaceMeta(ctx context.Context, pctx *sources.ProviderContext, spaceKey string) ([]byte, error) {
	// Resolve space key to space ID by listing and filtering
	spaces, err := p.client.ListSpaces(ctx, pctx.Credentials, 250)
	if err != nil {
		return nil, err
	}

	for _, s := range spaces {
		if strings.EqualFold(s.Key, spaceKey) {
			return jsonMarshal(map[string]any{
				"id":          s.ID,
				"key":         s.Key,
				"name":        s.Name,
				"type":        s.Type,
				"status":      s.Status,
				"description": s.Description,
				"homepageId":  s.HomepageID,
			})
		}
	}

	return nil, sources.ErrNotFound
}

func (p *ConfluenceProvider) fetchSpaceRecentPages(ctx context.Context, pctx *sources.ProviderContext, spaceKey string) ([]byte, error) {
	// Resolve space key to space ID
	spaces, err := p.client.ListSpaces(ctx, pctx.Credentials, 250)
	if err != nil {
		return nil, err
	}

	var spaceID string
	for _, s := range spaces {
		if strings.EqualFold(s.Key, spaceKey) {
			spaceID = s.ID
			break
		}
	}
	if spaceID == "" {
		return nil, sources.ErrNotFound
	}

	pages, err := p.client.ListPages(ctx, pctx.Credentials, spaceID, 50)
	if err != nil {
		return nil, err
	}

	simplified := make([]map[string]any, 0, len(pages))
	for _, page := range pages {
		entry := map[string]any{
			"id":     page.ID,
			"title":  page.Title,
			"status": page.Status,
		}
		if page.Version != nil {
			entry["version"] = page.Version.Number
			entry["lastModified"] = page.Version.CreatedAt
		}
		simplified = append(simplified, entry)
	}

	return jsonMarshal(map[string]any{
		"pages": simplified,
		"count": len(simplified),
	})
}

// ---------------------------------------------------------------------------
// Pages: /pages/{page-id}/meta.json, content.md, children.json
// ---------------------------------------------------------------------------

func (p *ConfluenceProvider) statPages(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	switch len(parts) {
	case 0:
		return sources.DirInfo(), nil
	case 1:
		// /pages/{page-id}
		return sources.DirInfo(), nil
	case 2:
		// /pages/{page-id}/<file>
		switch parts[1] {
		case "meta.json", "content.md", "children.json":
			return &sources.FileInfo{
				Size:  0,
				Mode:  sources.ModeFile,
				Mtime: sources.NowUnix(),
			}, nil
		default:
			return nil, sources.ErrNotFound
		}
	default:
		return nil, sources.ErrNotFound
	}
}

func (p *ConfluenceProvider) readdirPages(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	switch len(parts) {
	case 0:
		// List recent pages as browsable entries
		pages, err := p.client.GetRecentPages(ctx, pctx.Credentials, 50)
		if err != nil {
			return nil, err
		}

		entries := make([]sources.DirEntry, 0, len(pages))
		for _, page := range pages {
			entries = append(entries, sources.DirEntry{
				Name:  page.ID,
				Mode:  sources.ModeDir,
				IsDir: true,
				Mtime: sources.NowUnix(),
			})
		}
		return entries, nil

	case 1:
		// /pages/{page-id} — list files
		return []sources.DirEntry{
			{Name: "meta.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "content.md", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "children.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil

	default:
		return nil, sources.ErrNotDir
	}
}

func (p *ConfluenceProvider) readPages(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) < 2 {
		return nil, sources.ErrIsDir
	}

	pageID := parts[0]
	file := parts[1]

	var data []byte
	var err error

	switch file {
	case "meta.json":
		data, err = p.fetchPageMeta(ctx, pctx, pageID)
	case "content.md":
		data, err = p.fetchPageContent(ctx, pctx, pageID)
	case "children.json":
		data, err = p.fetchPageChildren(ctx, pctx, pageID)
	default:
		return nil, sources.ErrNotFound
	}

	if err != nil {
		return nil, err
	}
	return sliceData(data, offset, length), nil
}

func (p *ConfluenceProvider) fetchPageMeta(ctx context.Context, pctx *sources.ProviderContext, pageID string) ([]byte, error) {
	page, err := p.client.GetPage(ctx, pctx.Credentials, pageID)
	if err != nil {
		return nil, err
	}

	meta := map[string]any{
		"id":       page.ID,
		"title":    page.Title,
		"spaceId":  page.SpaceID,
		"status":   page.Status,
		"authorId": page.AuthorID,
	}

	if page.ParentID != "" {
		meta["parentId"] = page.ParentID
		meta["parentType"] = page.ParentType
	}
	if page.Version != nil {
		meta["version"] = page.Version.Number
		meta["lastModified"] = page.Version.CreatedAt
	}

	return jsonMarshal(meta)
}

func (p *ConfluenceProvider) fetchPageContent(ctx context.Context, pctx *sources.ProviderContext, pageID string) ([]byte, error) {
	page, err := p.client.GetPage(ctx, pctx.Credentials, pageID)
	if err != nil {
		return nil, err
	}

	storageBody := ""
	if page.Body != nil && page.Body.Storage != nil {
		storageBody = page.Body.Storage.Value
	}

	// Add title as heading
	var md strings.Builder
	md.WriteString("# ")
	md.WriteString(page.Title)
	md.WriteString("\n\n")

	if storageBody != "" {
		md.WriteString(clients.StorageToMarkdown(storageBody))
	}

	return []byte(md.String()), nil
}

func (p *ConfluenceProvider) fetchPageChildren(ctx context.Context, pctx *sources.ProviderContext, pageID string) ([]byte, error) {
	children, err := p.client.GetPageChildren(ctx, pctx.Credentials, pageID, 50)
	if err != nil {
		return nil, err
	}

	simplified := make([]map[string]any, 0, len(children))
	for _, child := range children {
		simplified = append(simplified, map[string]any{
			"id":    child.ID,
			"title": child.Title,
		})
	}

	return jsonMarshal(map[string]any{
		"children": simplified,
		"count":    len(simplified),
	})
}

// ---------------------------------------------------------------------------
// QueryExecutor interface
// ---------------------------------------------------------------------------

func (p *ConfluenceProvider) ExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	limit := spec.Limit
	if limit <= 0 {
		limit = 50
	}

	results, nextLink, err := p.client.SearchCQL(ctx, pctx.Credentials, spec.Query, limit)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return &sources.QueryResponse{
			Results: []sources.QueryResult{},
			HasMore: false,
		}, nil
	}

	filenameFormat := spec.FilenameFormat
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat("confluence")
	}

	queryResults := make([]sources.QueryResult, 0, len(results))
	for _, r := range results {
		mtime := sources.NowUnix()
		modDate := ""
		if t, err := time.Parse(time.RFC3339, r.LastModified); err == nil {
			mtime = t.Unix()
			modDate = t.Format("2006-01-02")
		}

		metadata := map[string]string{
			"id":    r.ID,
			"title": r.Title,
			"type":  r.Type,
			"space": r.Space.Key,
			"date":  modDate,
			"url":   r.URL,
		}

		filename := p.FormatFilename(filenameFormat, metadata)

		queryResults = append(queryResults, sources.QueryResult{
			ID:       r.ID,
			Filename: filename,
			Metadata: metadata,
			Mtime:    mtime,
		})
	}

	return &sources.QueryResponse{
		Results:       queryResults,
		NextPageToken: nextLink,
		HasMore:       nextLink != "",
	}, nil
}

func (p *ConfluenceProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}
	return p.fetchPageContent(ctx, pctx, resultID)
}

func (p *ConfluenceProvider) FormatFilename(format string, metadata map[string]string) string {
	if format == "" {
		format = "{space}_{title}_{id}.md"
	}

	result := format
	for key, value := range metadata {
		placeholder := "{" + key + "}"
		safeValue := sources.SanitizeFilename(value)
		if key != "id" && len(safeValue) > 50 {
			safeValue = safeValue[:50]
		}
		result = strings.ReplaceAll(result, placeholder, safeValue)
	}

	if result == "" {
		if id, ok := metadata["id"]; ok {
			result = id + ".md"
		} else {
			result = "unknown.md"
		}
	}

	return result
}
