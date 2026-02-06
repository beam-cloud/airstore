package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/sources/clients"
	"github.com/beam-cloud/airstore/pkg/types"
)

// PostHogProvider implements sources.Provider, sources.NativeBrowsable,
// and sources.CredentialValidator for PostHog integration.
// It exposes PostHog resources as a read-only filesystem under /sources/posthog/.
//
// Filesystem layout:
//
//	/sources/posthog/
//	  {project-id}_{project-name}/
//	    events/
//	      {event-id}.json
//	    feature-flags/
//	      {flag-key}.json
//	    insights/
//	      {insight-short-id}_{insight-name}.json
//	    cohorts/
//	      {cohort-id}_{cohort-name}.json
type PostHogProvider struct{}

func NewPostHogProvider() *PostHogProvider {
	return &PostHogProvider{}
}

func (p *PostHogProvider) Name() string { return types.ToolPostHog.String() }

// IsNativeBrowsable returns true — PostHog exposes a native file tree.
func (p *PostHogProvider) IsNativeBrowsable() bool { return true }

// ValidateCredentials checks that the API key is valid by calling the projects endpoint.
func (p *PostHogProvider) ValidateCredentials(ctx context.Context, creds *types.IntegrationCredentials) error {
	if creds.APIKey == "" {
		return fmt.Errorf("api_key is required")
	}
	client := p.newClient(creds)
	_, err := client.ListProjects(ctx)
	if err != nil {
		return fmt.Errorf("credential validation failed: %w", err)
	}
	return nil
}

func (p *PostHogProvider) checkAuth(pctx *sources.ProviderContext) error {
	if pctx.Credentials == nil || pctx.Credentials.APIKey == "" {
		return sources.ErrNotConnected
	}
	return nil
}

func (p *PostHogProvider) newClient(creds *types.IntegrationCredentials) *clients.PostHogClient {
	host := ""
	if creds.Extra != nil {
		host = creds.Extra["host"]
	}
	return clients.NewPostHogClient(creds.APIKey, host)
}

// subcategories are the fixed directories under each project.
var posthogSubcategories = []string{"events", "feature-flags", "insights", "cohorts"}

// Stat returns file/directory attributes for a path within the integration.
func (p *PostHogProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if err := p.checkAuth(pctx); err != nil {
		return nil, err
	}
	if path == "" {
		return sources.DirInfo(), nil
	}

	parts := strings.Split(path, "/")

	switch len(parts) {
	case 1:
		// Project directory — validate it exists
		client := p.newClient(pctx.Credentials)
		projects, err := client.ListProjects(ctx)
		if err != nil {
			return nil, err
		}
		for _, proj := range projects {
			if posthogProjectDirName(proj) == parts[0] {
				return sources.DirInfo(), nil
			}
		}
		return nil, sources.ErrNotFound

	case 2:
		// Subcategory directory
		for _, sub := range posthogSubcategories {
			if parts[1] == sub {
				return sources.DirInfo(), nil
			}
		}
		return nil, sources.ErrNotFound

	case 3:
		// Individual resource file — just report it as a file
		return &sources.FileInfo{
			Mode:  sources.ModeFile,
			Mtime: sources.NowUnix(),
		}, nil

	default:
		return nil, sources.ErrNotFound
	}
}

// ReadDir lists directory contents.
func (p *PostHogProvider) ReadDir(ctx context.Context, pctx *sources.ProviderContext, path string) ([]sources.DirEntry, error) {
	if err := p.checkAuth(pctx); err != nil {
		return nil, err
	}

	client := p.newClient(pctx.Credentials)

	// Root — list projects
	if path == "" {
		projects, err := client.ListProjects(ctx)
		if err != nil {
			return nil, err
		}
		entries := make([]sources.DirEntry, 0, len(projects))
		for _, proj := range projects {
			entries = append(entries, sources.DirEntry{
				Name:  posthogProjectDirName(proj),
				Mode:  sources.ModeDir,
				IsDir: true,
				Mtime: sources.NowUnix(),
			})
		}
		return entries, nil
	}

	parts := strings.Split(path, "/")

	if len(parts) == 1 {
		// Project root — list subcategories
		entries := make([]sources.DirEntry, 0, len(posthogSubcategories))
		for _, sub := range posthogSubcategories {
			entries = append(entries, sources.DirEntry{
				Name:  sub,
				Mode:  sources.ModeDir,
				IsDir: true,
				Mtime: sources.NowUnix(),
			})
		}
		return entries, nil
	}

	if len(parts) == 2 {
		projectID, err := posthogParseProjectID(parts[0])
		if err != nil {
			return nil, sources.ErrNotFound
		}

		switch parts[1] {
		case "events":
			return p.listEvents(ctx, client, projectID)
		case "feature-flags":
			return p.listFeatureFlags(ctx, client, projectID)
		case "insights":
			return p.listInsights(ctx, client, projectID)
		case "cohorts":
			return p.listCohorts(ctx, client, projectID)
		}
	}

	return nil, sources.ErrNotFound
}

// Read reads file content.
func (p *PostHogProvider) Read(ctx context.Context, pctx *sources.ProviderContext, path string, offset, length int64) ([]byte, error) {
	if err := p.checkAuth(pctx); err != nil {
		return nil, err
	}

	parts := strings.Split(path, "/")
	if len(parts) != 3 {
		return nil, sources.ErrNotFound
	}

	projectID, err := posthogParseProjectID(parts[0])
	if err != nil {
		return nil, sources.ErrNotFound
	}

	client := p.newClient(pctx.Credentials)
	var data []byte

	switch parts[1] {
	case "events":
		data, err = p.readEvent(ctx, client, projectID, parts[2])
	case "feature-flags":
		data, err = p.readFeatureFlag(ctx, client, projectID, parts[2])
	case "insights":
		data, err = p.readInsight(ctx, client, projectID, parts[2])
	case "cohorts":
		data, err = p.readCohort(ctx, client, projectID, parts[2])
	default:
		return nil, sources.ErrNotFound
	}

	if err != nil {
		return nil, err
	}
	return sliceBytes(data, offset, length), nil
}

func (p *PostHogProvider) Readlink(_ context.Context, _ *sources.ProviderContext, _ string) (string, error) {
	return "", sources.ErrNotFound
}

func (p *PostHogProvider) Search(_ context.Context, _ *sources.ProviderContext, _ string, _ int) ([]sources.SearchResult, error) {
	return nil, sources.ErrSearchNotSupported
}

// Compile-time interface assertion for QueryExecutor
var _ sources.QueryExecutor = (*PostHogProvider)(nil)

// ExecuteQuery runs a PostHog search query and returns results.
// Implements the sources.QueryExecutor interface for smart folder queries.
func (p *PostHogProvider) ExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	if err := p.checkAuth(pctx); err != nil {
		return nil, err
	}

	client := p.newClient(pctx.Credentials)

	// Resolve project ID
	projectID := 0
	if pidStr, ok := spec.Metadata["project_id"]; ok {
		if pid, err := strconv.Atoi(pidStr); err == nil && pid > 0 {
			projectID = pid
		}
	}
	if projectID == 0 {
		projects, err := client.ListProjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list projects: %w", err)
		}
		if len(projects) == 0 {
			return &sources.QueryResponse{Results: []sources.QueryResult{}}, nil
		}
		projectID = projects[0].ID
	}

	limit := spec.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	searchType := spec.Metadata["search_type"]
	filenameFormat := spec.FilenameFormat
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat("posthog")
	}

	switch searchType {
	case "feature-flags":
		return p.executeFeatureFlagQuery(ctx, client, projectID, spec.Query, filenameFormat)
	case "insights":
		return p.executeInsightQuery(ctx, client, projectID, spec.Query, filenameFormat)
	case "cohorts":
		return p.executeCohortQuery(ctx, client, projectID, spec.Query, filenameFormat)
	default:
		return p.executeEventQuery(ctx, client, projectID, spec.Query, limit, filenameFormat)
	}
}

func (p *PostHogProvider) executeFeatureFlagQuery(ctx context.Context, client *clients.PostHogClient, projectID int, query, filenameFormat string) (*sources.QueryResponse, error) {
	var flags []clients.PostHogFeatureFlag
	var err error
	if query != "" {
		flags, err = client.SearchFeatureFlags(ctx, projectID, query)
	} else {
		flags, err = client.ListFeatureFlags(ctx, projectID)
	}
	if err != nil {
		return nil, err
	}

	results := make([]sources.QueryResult, 0, len(flags))
	for _, f := range flags {
		metadata := map[string]string{
			"id":     strconv.Itoa(f.ID),
			"key":    f.Key,
			"name":   f.Name,
			"active": strconv.FormatBool(f.Active),
		}
		results = append(results, sources.QueryResult{
			ID:       fmt.Sprintf("%d:feature-flags:%d", projectID, f.ID),
			Filename: p.FormatFilename(filenameFormat, metadata),
			Metadata: metadata,
			Mtime:    sources.NowUnix(),
		})
	}

	return &sources.QueryResponse{Results: results}, nil
}

func (p *PostHogProvider) executeInsightQuery(ctx context.Context, client *clients.PostHogClient, projectID int, query, filenameFormat string) (*sources.QueryResponse, error) {
	var insights []clients.PostHogInsight
	var err error
	if query != "" {
		insights, err = client.SearchInsights(ctx, projectID, query)
	} else {
		insights, err = client.ListInsights(ctx, projectID)
	}
	if err != nil {
		return nil, err
	}

	results := make([]sources.QueryResult, 0, len(insights))
	for _, ins := range insights {
		metadata := map[string]string{
			"id":       strconv.Itoa(ins.ID),
			"short_id": ins.ShortID,
			"name":     ins.Name,
		}
		results = append(results, sources.QueryResult{
			ID:       fmt.Sprintf("%d:insights:%s", projectID, ins.ShortID),
			Filename: p.FormatFilename(filenameFormat, metadata),
			Metadata: metadata,
			Mtime:    sources.NowUnix(),
		})
	}

	return &sources.QueryResponse{Results: results}, nil
}

func (p *PostHogProvider) executeCohortQuery(ctx context.Context, client *clients.PostHogClient, projectID int, query, filenameFormat string) (*sources.QueryResponse, error) {
	var cohorts []clients.PostHogCohort
	var err error
	if query != "" {
		cohorts, err = client.SearchCohorts(ctx, projectID, query)
	} else {
		cohorts, err = client.ListCohorts(ctx, projectID)
	}
	if err != nil {
		return nil, err
	}

	results := make([]sources.QueryResult, 0, len(cohorts))
	for _, co := range cohorts {
		metadata := map[string]string{
			"id":    strconv.Itoa(co.ID),
			"name":  co.Name,
			"count": strconv.Itoa(co.Count),
		}
		results = append(results, sources.QueryResult{
			ID:       fmt.Sprintf("%d:cohorts:%d", projectID, co.ID),
			Filename: p.FormatFilename(filenameFormat, metadata),
			Metadata: metadata,
			Mtime:    sources.NowUnix(),
		})
	}

	return &sources.QueryResponse{Results: results}, nil
}

func (p *PostHogProvider) executeEventQuery(ctx context.Context, client *clients.PostHogClient, projectID int, query string, limit int, filenameFormat string) (*sources.QueryResponse, error) {
	events, err := client.SearchEvents(ctx, projectID, query, limit)
	if err != nil {
		return nil, err
	}

	results := make([]sources.QueryResult, 0, len(events))
	for _, ev := range events {
		date := ""
		if ev.Timestamp != "" && len(ev.Timestamp) >= 10 {
			date = ev.Timestamp[:10]
		}
		metadata := map[string]string{
			"id":    ev.ID,
			"event": ev.Event,
			"date":  date,
		}
		results = append(results, sources.QueryResult{
			ID:       fmt.Sprintf("%d:events:%s", projectID, ev.ID),
			Filename: p.FormatFilename(filenameFormat, metadata),
			Metadata: metadata,
			Mtime:    sources.NowUnix(),
		})
	}

	return &sources.QueryResponse{Results: results}, nil
}

// ReadResult fetches content for a specific PostHog resource by its composite ID.
// Composite ID format: "{projectID}:{resourceType}:{resourceID}"
func (p *PostHogProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if err := p.checkAuth(pctx); err != nil {
		return nil, err
	}

	parts := strings.SplitN(resultID, ":", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid result ID format: %s", resultID)
	}

	projectID, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid project ID in result ID: %s", resultID)
	}
	resourceType := parts[1]
	resourceID := parts[2]

	client := p.newClient(pctx.Credentials)

	switch resourceType {
	case "feature-flags":
		flagID, err := strconv.Atoi(resourceID)
		if err != nil {
			return nil, fmt.Errorf("invalid flag ID: %s", resourceID)
		}
		flag, err := client.GetFeatureFlag(ctx, projectID, flagID)
		if err != nil {
			if errors.Is(err, clients.ErrResourceNotFound) {
				return nil, sources.ErrNotFound
			}
			return nil, err
		}
		return jsonMarshalIndent(flag)

	case "insights":
		insight, err := client.GetInsightByShortID(ctx, projectID, resourceID)
		if err != nil {
			if errors.Is(err, clients.ErrResourceNotFound) {
				return nil, sources.ErrNotFound
			}
			return nil, err
		}
		return jsonMarshalIndent(insight)

	case "cohorts":
		cohortID, err := strconv.Atoi(resourceID)
		if err != nil {
			return nil, fmt.Errorf("invalid cohort ID: %s", resourceID)
		}
		cohort, err := client.GetCohort(ctx, projectID, cohortID)
		if err != nil {
			if errors.Is(err, clients.ErrResourceNotFound) {
				return nil, sources.ErrNotFound
			}
			return nil, err
		}
		return jsonMarshalIndent(cohort)

	case "events":
		// No single-event GET endpoint; search and match by ID
		events, err := client.SearchEvents(ctx, projectID, "", 100)
		if err != nil {
			return nil, err
		}
		for _, ev := range events {
			if ev.ID == resourceID {
				return jsonMarshalIndent(ev)
			}
		}
		return nil, sources.ErrNotFound

	default:
		return nil, fmt.Errorf("unknown resource type: %s", resourceType)
	}
}

// FormatFilename generates a filename from metadata using a format template.
// Supported placeholders: {id}, {key}, {name}, {event}, {date}, {short_id}, {active}, {count}
func (p *PostHogProvider) FormatFilename(format string, metadata map[string]string) string {
	if format == "" {
		format = "{id}.json"
	}

	result := format
	for key, value := range metadata {
		placeholder := "{" + key + "}"
		safeValue := sources.SanitizeFilename(value)
		// Truncate long values (except id, short_id)
		if key != "id" && key != "short_id" && len(safeValue) > 40 {
			safeValue = safeValue[:40]
		}
		result = strings.ReplaceAll(result, placeholder, safeValue)
	}

	// Ensure .json extension
	if !strings.Contains(result, ".") {
		result += ".json"
	}

	if result == "" || result == ".json" {
		if id, ok := metadata["id"]; ok {
			result = id + ".json"
		} else {
			result = "result.json"
		}
	}

	return result
}

// --- ReadDir helpers ---

func (p *PostHogProvider) listEvents(ctx context.Context, client *clients.PostHogClient, projectID int) ([]sources.DirEntry, error) {
	events, err := client.ListEvents(ctx, projectID, 100)
	if err != nil {
		return nil, err
	}
	entries := make([]sources.DirEntry, 0, len(events))
	for _, ev := range events {
		name := sources.SanitizeFilename(ev.ID) + ".json"
		entries = append(entries, sources.DirEntry{
			Name:  name,
			Mode:  sources.ModeFile,
			Mtime: sources.NowUnix(),
		})
	}
	return entries, nil
}

func (p *PostHogProvider) listFeatureFlags(ctx context.Context, client *clients.PostHogClient, projectID int) ([]sources.DirEntry, error) {
	flags, err := client.ListFeatureFlags(ctx, projectID)
	if err != nil {
		return nil, err
	}
	entries := make([]sources.DirEntry, 0, len(flags))
	for _, f := range flags {
		name := sources.SanitizeFilename(f.Key) + ".json"
		entries = append(entries, sources.DirEntry{
			Name:  name,
			Mode:  sources.ModeFile,
			Mtime: sources.NowUnix(),
		})
	}
	return entries, nil
}

func (p *PostHogProvider) listInsights(ctx context.Context, client *clients.PostHogClient, projectID int) ([]sources.DirEntry, error) {
	insights, err := client.ListInsights(ctx, projectID)
	if err != nil {
		return nil, err
	}
	entries := make([]sources.DirEntry, 0, len(insights))
	for _, ins := range insights {
		safeName := sanitizeAndTruncate(ins.Name)
		name := fmt.Sprintf("%s_%s.json", ins.ShortID, safeName)
		entries = append(entries, sources.DirEntry{
			Name:  name,
			Mode:  sources.ModeFile,
			Mtime: sources.NowUnix(),
		})
	}
	return entries, nil
}

func (p *PostHogProvider) listCohorts(ctx context.Context, client *clients.PostHogClient, projectID int) ([]sources.DirEntry, error) {
	cohorts, err := client.ListCohorts(ctx, projectID)
	if err != nil {
		return nil, err
	}
	entries := make([]sources.DirEntry, 0, len(cohorts))
	for _, co := range cohorts {
		safeName := sanitizeAndTruncate(co.Name)
		name := fmt.Sprintf("%d_%s.json", co.ID, safeName)
		entries = append(entries, sources.DirEntry{
			Name:  name,
			Mode:  sources.ModeFile,
			Mtime: sources.NowUnix(),
		})
	}
	return entries, nil
}

// --- Read helpers ---

func (p *PostHogProvider) readEvent(ctx context.Context, client *clients.PostHogClient, projectID int, filename string) ([]byte, error) {
	id := strings.TrimSuffix(filename, ".json")
	events, err := client.ListEvents(ctx, projectID, 100)
	if err != nil {
		return nil, err
	}
	for _, ev := range events {
		if sources.SanitizeFilename(ev.ID) == id {
			return jsonMarshalIndent(ev)
		}
	}
	return nil, sources.ErrNotFound
}

func (p *PostHogProvider) readFeatureFlag(ctx context.Context, client *clients.PostHogClient, projectID int, filename string) ([]byte, error) {
	key := strings.TrimSuffix(filename, ".json")
	flags, err := client.ListFeatureFlags(ctx, projectID)
	if err != nil {
		return nil, err
	}
	for _, f := range flags {
		if sources.SanitizeFilename(f.Key) == key {
			return jsonMarshalIndent(f)
		}
	}
	return nil, sources.ErrNotFound
}

func (p *PostHogProvider) readInsight(ctx context.Context, client *clients.PostHogClient, projectID int, filename string) ([]byte, error) {
	name := strings.TrimSuffix(filename, ".json")
	// Extract shortID from filename prefix (format: {shortID}_{name}.json)
	idx := strings.Index(name, "_")
	if idx < 0 {
		return nil, sources.ErrNotFound
	}
	shortID := name[:idx]

	insight, err := client.GetInsightByShortID(ctx, projectID, shortID)
	if err != nil {
		if errors.Is(err, clients.ErrResourceNotFound) {
			return nil, sources.ErrNotFound
		}
		return nil, err
	}
	return jsonMarshalIndent(insight)
}

func (p *PostHogProvider) readCohort(ctx context.Context, client *clients.PostHogClient, projectID int, filename string) ([]byte, error) {
	name := strings.TrimSuffix(filename, ".json")
	// Extract cohort ID from filename prefix (format: {id}_{name}.json)
	idx := strings.Index(name, "_")
	if idx < 0 {
		return nil, sources.ErrNotFound
	}
	cohortID, err := strconv.Atoi(name[:idx])
	if err != nil {
		return nil, sources.ErrNotFound
	}

	cohort, err := client.GetCohort(ctx, projectID, cohortID)
	if err != nil {
		if errors.Is(err, clients.ErrResourceNotFound) {
			return nil, sources.ErrNotFound
		}
		return nil, err
	}
	return jsonMarshalIndent(cohort)
}

// --- Helpers ---

func posthogProjectDirName(proj clients.PostHogProject) string {
	safeName := sanitizeAndTruncate(proj.Name)
	return fmt.Sprintf("%d_%s", proj.ID, safeName)
}

func posthogParseProjectID(dirName string) (int, error) {
	idx := strings.Index(dirName, "_")
	if idx < 0 {
		return strconv.Atoi(dirName)
	}
	return strconv.Atoi(dirName[:idx])
}

func jsonMarshalIndent(v any) ([]byte, error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func sliceBytes(data []byte, offset, length int64) []byte {
	if offset >= int64(len(data)) {
		return nil
	}
	end := int64(len(data))
	if length > 0 && offset+length < end {
		end = offset + length
	}
	return data[offset:end]
}

func sanitizeAndTruncate(name string) string {
	safe := sources.SanitizeFilename(name)
	if len(safe) > 50 {
		return safe[:50]
	}
	return safe
}
