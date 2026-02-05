package services

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/sources"
	baml "github.com/beam-cloud/airstore/pkg/sources/queries/baml_client"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"
)

const credCacheTTL = 5 * time.Minute // Cache credentials for 5 minutes

// cachedCreds holds cached credentials with expiration
type cachedCreds struct {
	creds     *types.IntegrationCredentials
	expiresAt time.Time
}

// gmailQuerySpec represents the JSON structure for a Gmail query specification
type gmailQuerySpec struct {
	GmailQuery     string `json:"gmail_query"`
	Limit          int    `json:"limit"`
	FilenameFormat string `json:"filename_format"`
}

// buildGmailQuerySpec creates a JSON query spec string for Gmail queries
func buildGmailQuerySpec(query string, limit int, filenameFormat string) string {
	spec := gmailQuerySpec{
		GmailQuery:     query,
		Limit:          limit,
		FilenameFormat: filenameFormat,
	}
	data, _ := json.Marshal(spec)
	return string(data)
}

// SourceService implements the gRPC SourceService for integration access.
type SourceService struct {
	pb.UnimplementedSourceServiceServer
	registry      *sources.Registry
	backend       repository.BackendRepository
	fsStore       repository.FilesystemStore
	cache         *sources.SourceCache
	rateLimiter   *sources.RateLimiter
	oauthRegistry *oauth.Registry
	credCache     sync.Map // map[string]*cachedCreds - caches credentials by "workspaceId:integration"
	queryGroup    singleflight.Group
	hookStream    *common.EventStream  // optional: for emitting source change events
	seenTracker   *hooks.SeenTracker   // optional: for detecting new query results
}

// SourceServiceOption configures optional dependencies on SourceService.
type SourceServiceOption func(*SourceService)

// WithHookStream sets the event stream for hook event emission.
func WithHookStream(stream *common.EventStream) SourceServiceOption {
	return func(s *SourceService) { s.hookStream = stream }
}

// WithSeenTracker sets the seen tracker for change detection.
func WithSeenTracker(tracker *hooks.SeenTracker) SourceServiceOption {
	return func(s *SourceService) { s.seenTracker = tracker }
}

// NewSourceService creates a new SourceService.
func NewSourceService(registry *sources.Registry, backend repository.BackendRepository, fsStore repository.FilesystemStore, opts ...SourceServiceOption) *SourceService {
	s := &SourceService{
		registry:    registry,
		backend:     backend,
		fsStore:     fsStore,
		cache:       sources.NewSourceCache(sources.DefaultCacheTTL, sources.DefaultCacheSize),
		rateLimiter: sources.NewRateLimiter(sources.DefaultRateLimitConfig()),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// NewSourceServiceWithOAuth creates a SourceService with OAuth refresh support.
func NewSourceServiceWithOAuth(registry *sources.Registry, backend repository.BackendRepository, fsStore repository.FilesystemStore, oauthRegistry *oauth.Registry, opts ...SourceServiceOption) *SourceService {
	s := &SourceService{
		registry:      registry,
		backend:       backend,
		fsStore:       fsStore,
		cache:         sources.NewSourceCache(sources.DefaultCacheTTL, sources.DefaultCacheSize),
		rateLimiter:   sources.NewRateLimiter(sources.DefaultRateLimitConfig()),
		oauthRegistry: oauthRegistry,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// InvalidateQueryCache invalidates cached results for a query path.
// Call this before ReadDir when refresh=true to force re-execution.
func (s *SourceService) InvalidateQueryCache(ctx context.Context, workspaceId uint, queryPath string) error {
	if s.fsStore == nil {
		return nil
	}
	log.Info().Uint("workspace_id", workspaceId).Str("path", queryPath).Msg("invalidating query cache")
	return s.fsStore.InvalidateQuery(ctx, workspaceId, queryPath)
}

// RefreshSmartQuery forces re-execution of a smart query, bypassing all caches.
// Returns the fresh results from the provider (e.g., Gmail API).
func (s *SourceService) RefreshSmartQuery(ctx context.Context, queryPath string) ([]repository.QueryResult, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get provider context: %w", err)
	}

	// Look up the query definition
	query, err := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, queryPath)
	if err != nil {
		return nil, fmt.Errorf("query lookup failed: %w", err)
	}
	if query == nil {
		return nil, fmt.Errorf("query not found: %s", queryPath)
	}

	// Load credentials
	pctx, connected := s.loadCredentials(ctx, pctx, query.Integration)
	if !connected {
		return nil, fmt.Errorf("not connected to %s", query.Integration)
	}

	log.Info().
		Str("path", queryPath).
		Str("integration", query.Integration).
		Str("query_spec", query.QuerySpec).
		Msg("refreshing smart query - executing provider query")

	// Execute the query fresh (this calls the Gmail/GDrive/etc API)
	results, err := s.executeAndCacheQuery(ctx, pctx, query)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	log.Info().
		Str("path", queryPath).
		Int("results", len(results)).
		Msg("smart query refresh complete")

	return results, nil
}

// Stat returns file/directory attributes for a source path
func (s *SourceService) Stat(ctx context.Context, req *pb.SourceStatRequest) (*pb.SourceStatResponse, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.SourceStatResponse{Ok: false, Error: err.Error()}, nil
	}

	path := cleanPath(req.Path)

	// Root /sources directory
	if path == "" {
		return &pb.SourceStatResponse{
			Ok: true,
			Info: &pb.SourceFileInfo{
				Mode:  sources.ModeDir,
				IsDir: true,
				Mtime: sources.NowUnix(),
			},
		}, nil
	}

	// Parse integration name from path
	integration, relPath := splitIntegrationPath(path)

	provider := s.registry.Get(integration)
	if provider == nil {
		return &pb.SourceStatResponse{Ok: false, Error: "integration not found"}, nil
	}

	// Get credentials for this integration
	pctx, connected := s.loadCredentials(ctx, pctx, integration)

	// Handle root of integration (e.g., /sources/github)
	if relPath == "" {
		return &pb.SourceStatResponse{
			Ok: true,
			Info: &pb.SourceFileInfo{
				Mode:  sources.ModeDir,
				IsDir: true,
				Mtime: sources.NowUnix(),
			},
		}, nil
	}

	// Handle README.md specially (no caching needed, cheap to generate)
	if relPath == types.SourceStatusFile {
		workspaceId := ""
		scope := ""
		if connected && pctx.Credentials != nil {
			scope = "shared" // TODO: detect personal vs shared
		}
		data := sources.GenerateSourceReadme(integration, connected, scope, workspaceId)
		return &pb.SourceStatResponse{
			Ok: true,
			Info: &pb.SourceFileInfo{
				Size:  int64(len(data)),
				Mode:  sources.ModeFile,
				Mtime: sources.NowUnix(),
			},
		}, nil
	}

	// Check cache first
	cacheKey := sources.CacheKey(pctx.WorkspaceId, integration, relPath, "stat")
	if info, ok := s.cache.GetInfo(cacheKey); ok {
		return &pb.SourceStatResponse{
			Ok: true,
			Info: &pb.SourceFileInfo{
				Size:   info.Size,
				Mode:   info.Mode,
				Mtime:  info.Mtime,
				IsDir:  info.IsDir,
				IsLink: info.IsLink,
			},
		}, nil
	}

	// Rate limit check
	if err := s.rateLimiter.Wait(ctx, pctx.WorkspaceId, integration); err != nil {
		return &pb.SourceStatResponse{Ok: false, Error: "rate limited"}, nil
	}

	// Use singleflight to coalesce concurrent requests
	result, err := s.cache.DoOnce(cacheKey, func() (any, error) {
		return provider.Stat(ctx, pctx, relPath)
	})
	if err != nil {
		return &pb.SourceStatResponse{Ok: false, Error: err.Error()}, nil
	}

	info := result.(*sources.FileInfo)

	// Cache the result
	s.cache.SetInfo(cacheKey, info)

	return &pb.SourceStatResponse{
		Ok: true,
		Info: &pb.SourceFileInfo{
			Size:   info.Size,
			Mode:   info.Mode,
			Mtime:  info.Mtime,
			IsDir:  info.IsDir,
			IsLink: info.IsLink,
		},
	}, nil
}

// ReadDir lists directory contents
func (s *SourceService) ReadDir(ctx context.Context, req *pb.SourceReadDirRequest) (*pb.SourceReadDirResponse, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.SourceReadDirResponse{Ok: false, Error: err.Error()}, nil
	}

	path := cleanPath(req.Path)

	// Root /sources directory - list all integrations
	if path == "" {
		entries := make([]*pb.SourceDirEntry, 0, len(s.registry.List()))
		for _, name := range s.registry.List() {
			entries = append(entries, &pb.SourceDirEntry{
				Name:  name,
				Mode:  sources.ModeDir,
				IsDir: true,
				Mtime: sources.NowUnix(),
			})
		}
		return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
	}

	// Parse integration name from path
	integration, relPath := splitIntegrationPath(path)

	provider := s.registry.Get(integration)
	if provider == nil {
		return &pb.SourceReadDirResponse{Ok: false, Error: "integration not found"}, nil
	}

	// Get credentials for this integration
	pctx, connected := s.loadCredentials(ctx, pctx, integration)

	// Integration root (e.g., /sources/gmail) - show README.md + smart queries only
	if relPath == "" {
		return s.readDirIntegrationRoot(ctx, pctx, integration, connected)
	}

	// Check if this path is a smart query
	queryPath := "/sources/" + path
	query, err := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, queryPath)
	if err != nil {
		log.Debug().Err(err).Str("path", queryPath).Msg("query lookup error")
	}

	// If it's a smart query folder, return its materialized results
	if query != nil && query.OutputFormat == types.QueryOutputFolder {
		return s.readDirSmartQuery(ctx, pctx, query, connected)
	}

	// Check for .query.as metadata file
	if strings.HasSuffix(relPath, ".query.as") {
		// .query.as files are handled by Read, not ReadDir
		return &pb.SourceReadDirResponse{Ok: false, Error: "not a directory"}, nil
	}

	// For any other path, check if parent is a smart query
	parentPath := "/sources/" + integration
	if idx := strings.LastIndex(relPath, "/"); idx > 0 {
		parentPath = "/sources/" + integration + "/" + relPath[:idx]
	}

	parentQuery, _ := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, parentPath)
	if parentQuery != nil {
		// This is inside a smart query result - no subdirectories
		return &pb.SourceReadDirResponse{Ok: true, Entries: []*pb.SourceDirEntry{}}, nil
	}

	// Unknown path - return empty
	return &pb.SourceReadDirResponse{Ok: true, Entries: []*pb.SourceDirEntry{}}, nil
}

// getQueryChildCount returns the number of children for a smart query folder.
// Uses cached results if available, returns 1 (for .query.as) if not cached.
func (s *SourceService) getQueryChildCount(ctx context.Context, workspaceId uint, queryPath string) int {
	if s.fsStore == nil {
		return 1 // At least .query.as file
	}

	results, err := s.fsStore.GetQueryResults(ctx, workspaceId, queryPath)
	if err != nil || results == nil {
		return 1 // At least .query.as file
	}
	return len(results) + 1 // Results + .query.as
}

// readDirIntegrationRoot returns entries for integration root: README.md + smart queries
func (s *SourceService) readDirIntegrationRoot(ctx context.Context, pctx *sources.ProviderContext, integration string, connected bool) (*pb.SourceReadDirResponse, error) {
	// Generate README.md to get its size
	scope := ""
	if connected && pctx.Credentials != nil {
		scope = "shared"
	}
	statusData := sources.GenerateSourceReadme(integration, connected, scope, "")

	entries := []*pb.SourceDirEntry{
		{Name: types.SourceStatusFile, Mode: sources.ModeFile, Size: int64(len(statusData)), Mtime: sources.NowUnix()},
	}

	// List smart queries for this integration
	parentPath := "/sources/" + integration
	queries, err := s.fsStore.ListQueries(ctx, pctx.WorkspaceId, parentPath)
	if err != nil {
		log.Warn().Err(err).Str("path", parentPath).Msg("failed to list smart queries")
	} else {
		for _, q := range queries {
			// Only include direct children
			if q.Path == parentPath {
				continue
			}
			// Extract name from path
			name := strings.TrimPrefix(q.Path, parentPath+"/")
			if strings.Contains(name, "/") {
				continue // Skip nested entries
			}

			if q.OutputFormat == types.QueryOutputFolder {
				childCount := s.getQueryChildCount(ctx, pctx.WorkspaceId, q.Path)
				entries = append(entries, &pb.SourceDirEntry{
					Name:       name,
					Mode:       sources.ModeDir,
					IsDir:      true,
					Mtime:      q.UpdatedAt.Unix(),
					ChildCount: int32(childCount),
				})
				continue
			}

			// Single-file query
			filename := q.Name
			if q.FileExt != "" {
				filename = q.Name + q.FileExt
			}
			entries = append(entries, &pb.SourceDirEntry{
				Name:  filename,
				Mode:  sources.ModeFile,
				IsDir: false,
				Mtime: q.UpdatedAt.Unix(),
			})

			// Add hidden metadata file for single-file queries
			queryMeta := s.generateQueryMetaJSON(q)
			entries = append(entries, &pb.SourceDirEntry{
				Name:  "." + filename + ".query.as",
				Mode:  sources.ModeFile | 0444, // Read-only
				Size:  int64(len(queryMeta)),
				Mtime: q.UpdatedAt.Unix(),
			})
		}
	}

	return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
}

// readDirSmartQuery returns materialized results for a smart query folder
func (s *SourceService) readDirSmartQuery(ctx context.Context, pctx *sources.ProviderContext, query *types.FilesystemQuery, connected bool) (*pb.SourceReadDirResponse, error) {
	entries := []*pb.SourceDirEntry{}

	// Always include .query.as metadata file
	queryMeta := s.generateQueryMetaJSON(query)
	entries = append(entries, &pb.SourceDirEntry{
		Name:  ".query.as",
		Mode:  sources.ModeFile | 0444,
		Size:  int64(len(queryMeta)),
		Mtime: query.UpdatedAt.Unix(),
	})

	if !connected {
		return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
	}

	results, err := s.getOrExecuteQuery(ctx, pctx, query)
	if err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to execute smart query")
		return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
	}

	// Convert results to directory entries
	for _, r := range results {
		entries = append(entries, &pb.SourceDirEntry{
			Name:     r.Filename,
			Mode:     sources.ModeFile,
			Size:     r.Size,
			Mtime:    r.Mtime,
			ResultId: r.ID,
		})
	}

	return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
}

func (s *SourceService) executeAndCacheQuery(ctx context.Context, pctx *sources.ProviderContext, query *types.FilesystemQuery) ([]repository.QueryResult, error) {
	provider := s.registry.Get(query.Integration)
	if provider == nil {
		return nil, fmt.Errorf("provider not found: %s", query.Integration)
	}

	executor, ok := provider.(sources.QueryExecutor)
	if !ok {
		return nil, fmt.Errorf("provider does not support queries: %s", query.Integration)
	}

	spec := parseQuerySpec(query.Integration, query.QuerySpec)
	if spec.Query == "" {
		return nil, fmt.Errorf("empty query spec for %s", query.Integration)
	}

	log.Info().
		Str("integration", query.Integration).
		Str("path", query.Path).
		Str("query", spec.Query).
		Int("limit", spec.Limit).
		Int("max_results", spec.MaxResults).
		Msg("executing provider query")

	// Fetch all pages synchronously
	var allResults []repository.QueryResult
	seenIDs := make(map[string]bool)
	pageNum := 0

	for {
		pageNum++
		queryResp, err := executor.ExecuteQuery(ctx, pctx, spec)
		if err != nil {
			return nil, fmt.Errorf("query execution failed (page %d): %w", pageNum, err)
		}

		// Convert and dedupe
		for _, qr := range queryResp.Results {
			if qr.ID != "" && !seenIDs[qr.ID] {
				seenIDs[qr.ID] = true
				filename := qr.Filename
				if filename == "" {
					filename = executor.FormatFilename(spec.FilenameFormat, qr.Metadata)
				}
				allResults = append(allResults, repository.QueryResult{
					ID:       qr.ID,
					Filename: filename,
					Metadata: qr.Metadata,
					Size:     qr.Size,
					Mtime:    qr.Mtime,
				})
			}
		}

		log.Debug().
			Str("path", query.Path).
			Int("page", pageNum).
			Int("page_results", len(queryResp.Results)).
			Int("total_unique", len(allResults)).
			Bool("has_more", queryResp.HasMore).
			Msg("fetched page")

		// Stop if no more pages or hit max
		if !queryResp.HasMore || queryResp.NextPageToken == "" || len(allResults) >= spec.MaxResults {
			break
		}
		spec.PageToken = queryResp.NextPageToken
	}

	// Cap at max
	if len(allResults) > spec.MaxResults {
		allResults = allResults[:spec.MaxResults]
	}

	log.Info().
		Str("integration", query.Integration).
		Str("path", query.Path).
		Int("total_results", len(allResults)).
		Int("pages", pageNum).
		Msg("query complete")

	// Detect new results for hook triggers
	if s.seenTracker != nil && s.hookStream != nil && len(allResults) > 0 {
		seenKey := common.Keys.HookSeen(pctx.WorkspaceId, types.GeneratePathID(query.Path))
		ids := make([]string, len(allResults))
		for i, r := range allResults {
			ids[i] = r.ID
		}
		if newIDs, err := s.seenTracker.Diff(ctx, seenKey, ids); err == nil && len(newIDs) > 0 {
			s.hookStream.Emit(ctx, map[string]any{
				"event":        hooks.EventSourceChange,
				"workspace_id": fmt.Sprintf("%d", pctx.WorkspaceId),
				"path":         query.Path,
				"integration":  query.Integration,
				"new_count":    fmt.Sprintf("%d", len(newIDs)),
			})
			log.Debug().
				Str("path", query.Path).
				Int("new_results", len(newIDs)).
				Msg("source change detected")
		}
	}

	// Cache results
	ttl := time.Duration(query.CacheTTL) * time.Second
	if ttl == 0 {
		ttl = 5 * time.Minute
	}
	if err := s.fsStore.StoreQueryResults(ctx, pctx.WorkspaceId, query.Path, allResults, ttl); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to cache query results")
	}

	// Update last_executed timestamp
	now := time.Now()
	query.LastExecuted = &now
	if err := s.fsStore.UpdateQuery(ctx, query); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to update query timestamp")
	}

	return allResults, nil
}

func (s *SourceService) getOrExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, query *types.FilesystemQuery) ([]repository.QueryResult, error) {
	if results, err := s.fsStore.GetQueryResults(ctx, pctx.WorkspaceId, query.Path); err == nil && len(results) > 0 {
		return results, nil
	}

	key := fmt.Sprintf("%d:%s", pctx.WorkspaceId, query.Path)
	value, err, _ := s.queryGroup.Do(key, func() (any, error) {
		if results, err := s.fsStore.GetQueryResults(ctx, pctx.WorkspaceId, query.Path); err == nil && len(results) > 0 {
			return results, nil
		}
		return s.executeAndCacheQuery(ctx, pctx, query)
	})
	if err != nil {
		return nil, err
	}
	results, ok := value.([]repository.QueryResult)
	if !ok {
		return nil, fmt.Errorf("unexpected query result type for %s", query.Path)
	}
	return results, nil
}

// generateQueryMetaJSON creates the JSON content for a .query.as metadata file
func (s *SourceService) generateQueryMetaJSON(query *types.FilesystemQuery) []byte {
	data, _ := json.MarshalIndent(map[string]interface{}{
		"id":              query.Id,
		"external_id":     query.ExternalId,
		"workspace_id":    query.WorkspaceId,
		"integration":     query.Integration,
		"path":            query.Path,
		"name":            query.Name,
		"query_spec":      query.QuerySpec,
		"filename_format": query.FilenameFormat,
		"guidance":        query.Guidance,
		"output_format":   query.OutputFormat,
		"file_ext":        query.FileExt,
		"cache_ttl":       query.CacheTTL,
		"created_at":      query.CreatedAt,
		"updated_at":      query.UpdatedAt,
	}, "", "  ")
	return data
}

// Read reads file content
func (s *SourceService) Read(ctx context.Context, req *pb.SourceReadRequest) (*pb.SourceReadResponse, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.SourceReadResponse{Ok: false, Error: err.Error()}, nil
	}

	cleanedPath := cleanPath(req.Path)

	if cleanedPath == "" {
		return &pb.SourceReadResponse{Ok: false, Error: "is a directory"}, nil
	}

	// Parse integration name from path
	integration, relPath := splitIntegrationPath(cleanedPath)

	provider := s.registry.Get(integration)
	if provider == nil {
		return &pb.SourceReadResponse{Ok: false, Error: "integration not found"}, nil
	}

	// Get credentials for this integration
	pctx, connected := s.loadCredentials(ctx, pctx, integration)

	// Handle README.md
	if relPath == types.SourceStatusFile {
		scope := ""
		if connected && pctx.Credentials != nil {
			scope = "shared"
		}
		data := sources.GenerateSourceReadme(integration, connected, scope, "")
		return readSlice(data, req.Offset, req.Length), nil
	}

	if relPath == "" {
		return &pb.SourceReadResponse{Ok: false, Error: "is a directory"}, nil
	}

	// Handle .query.as metadata files
	if relPath == ".query.as" || strings.HasSuffix(relPath, "/.query.as") {
		// Get the parent query path
		queryPath := "/sources/" + integration
		if relPath != ".query.as" {
			queryPath = "/sources/" + integration + "/" + strings.TrimSuffix(relPath, "/.query.as")
		}

		query, err := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, queryPath)
		if err != nil || query == nil {
			return &pb.SourceReadResponse{Ok: false, Error: "query not found"}, nil
		}

		data := s.generateQueryMetaJSON(query)
		return readSlice(data, req.Offset, req.Length), nil
	}

	// Handle .{filename}.query.as metadata files for single-file queries
	base := path.Base(relPath)
	if strings.HasPrefix(base, ".") && strings.HasSuffix(base, ".query.as") {
		filename := strings.TrimPrefix(strings.TrimSuffix(base, ".query.as"), ".")
		dir := path.Dir(relPath)
		queryPath := "/sources/" + integration
		if dir != "." && dir != "" {
			queryPath += "/" + dir
		}
		queryPath += "/" + filename

		query, err := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, queryPath)
		if err != nil || query == nil || query.OutputFormat != types.QueryOutputFile {
			return &pb.SourceReadResponse{Ok: false, Error: "query not found"}, nil
		}

		data := s.generateQueryMetaJSON(query)
		return readSlice(data, req.Offset, req.Length), nil
	}

	// Check if this is a smart query result file
	// Path format: /sources/gmail/unread-emails/filename.txt
	// We need to find the parent query and the result filename
	queryPath, filename := s.findQueryAndFilename(ctx, pctx.WorkspaceId, integration, relPath)
	if queryPath != "" {
		return s.readSmartQueryResult(ctx, pctx, queryPath, filename, req.Offset, req.Length)
	}

	// Not a smart query result - return not found
	return &pb.SourceReadResponse{Ok: false, Error: "file not found"}, nil
}

// findQueryAndFilename finds the query path and filename for a smart query result
func (s *SourceService) findQueryAndFilename(ctx context.Context, workspaceId uint, integration, relPath string) (string, string) {
	// Try different path splits to find a matching query
	parts := strings.Split(relPath, "/")

	for i := len(parts) - 1; i > 0; i-- {
		parentPath := "/sources/" + integration + "/" + strings.Join(parts[:i], "/")
		query, err := s.fsStore.GetQuery(ctx, workspaceId, parentPath)
		if err == nil && query != nil && query.OutputFormat == types.QueryOutputFolder {
			filename := strings.Join(parts[i:], "/")
			return query.Path, filename
		}
	}

	// Check if the direct parent is a query
	if len(parts) >= 1 {
		parentPath := "/sources/" + integration + "/" + strings.Join(parts[:len(parts)-1], "/")
		if len(parts) == 1 {
			parentPath = "/sources/" + integration
		}
		query, err := s.fsStore.GetQuery(ctx, workspaceId, parentPath)
		if err == nil && query != nil && query.OutputFormat == types.QueryOutputFolder {
			return query.Path, parts[len(parts)-1]
		}
	}

	return "", ""
}

// readSmartQueryResult reads content from a smart query result
func (s *SourceService) readSmartQueryResult(ctx context.Context, pctx *sources.ProviderContext, queryPath, filename string, offset, length int64) (*pb.SourceReadResponse, error) {
	query, err := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, queryPath)
	if err != nil || query == nil {
		return &pb.SourceReadResponse{Ok: false, Error: "query not found"}, nil
	}

	provider := s.registry.Get(query.Integration)
	if provider == nil {
		return &pb.SourceReadResponse{Ok: false, Error: "provider not found"}, nil
	}

	executor, ok := provider.(sources.QueryExecutor)
	if !ok {
		return &pb.SourceReadResponse{Ok: false, Error: "provider does not support queries"}, nil
	}

	// Find the result ID from filename
	results, err := s.getOrExecuteQuery(ctx, pctx, query)
	if err != nil {
		return &pb.SourceReadResponse{Ok: false, Error: "failed to get query results"}, nil
	}

	var resultID string
	for _, r := range results {
		if r.Filename == filename {
			resultID = r.ID
			break
		}
	}

	if resultID == "" {
		return &pb.SourceReadResponse{Ok: false, Error: "result not found"}, nil
	}

	// Try cached content first
	if content, err := s.fsStore.GetResultContent(ctx, pctx.WorkspaceId, queryPath, resultID); err == nil && len(content) > 0 {
		return readSlice(content, offset, length), nil
	}

	// Fetch content from provider
	content, err := executor.ReadResult(ctx, pctx, resultID)
	if err != nil {
		return &pb.SourceReadResponse{Ok: false, Error: err.Error()}, nil
	}

	// Cache the content
	if err := s.fsStore.StoreResultContent(ctx, pctx.WorkspaceId, queryPath, resultID, content); err != nil {
		log.Warn().Err(err).Str("path", queryPath).Str("result", resultID).Msg("failed to cache result content")
	}

	return readSlice(content, offset, length), nil
}

// Readlink reads a symbolic link target
func (s *SourceService) Readlink(ctx context.Context, req *pb.SourceReadlinkRequest) (*pb.SourceReadlinkResponse, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.SourceReadlinkResponse{Ok: false, Error: err.Error()}, nil
	}

	path := cleanPath(req.Path)

	if path == "" {
		return &pb.SourceReadlinkResponse{Ok: false, Error: "not a symlink"}, nil
	}

	// Parse integration name from path
	integration, relPath := splitIntegrationPath(path)

	provider := s.registry.Get(integration)
	if provider == nil {
		return &pb.SourceReadlinkResponse{Ok: false, Error: "integration not found"}, nil
	}

	pctx, _ = s.loadCredentials(ctx, pctx, integration)

	target, err := provider.Readlink(ctx, pctx, relPath)
	if err != nil {
		return &pb.SourceReadlinkResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.SourceReadlinkResponse{Ok: true, Target: target}, nil
}

// providerContext creates a ProviderContext from the gRPC context
func (s *SourceService) providerContext(ctx context.Context) (*sources.ProviderContext, error) {
	rc := auth.AuthInfoFromContext(ctx)
	if rc == nil {
		// Allow unauthenticated access with empty context (for local mode)
		return &sources.ProviderContext{}, nil
	}

	return &sources.ProviderContext{
		WorkspaceId: auth.WorkspaceId(ctx),
		MemberId:    auth.MemberId(ctx),
	}, nil
}

// loadCredentials fetches integration credentials from the backend (with caching)
// and refreshes Google OAuth tokens if expired
func (s *SourceService) loadCredentials(ctx context.Context, pctx *sources.ProviderContext, integration string) (*sources.ProviderContext, bool) {
	if s.backend == nil {
		return pctx, false
	}
	if pctx.WorkspaceId == 0 {
		return pctx, false
	}

	// Check cache first
	cacheKey := fmt.Sprintf("%d:%s", pctx.WorkspaceId, integration)
	if cached, ok := s.credCache.Load(cacheKey); ok {
		c := cached.(*cachedCreds)
		if time.Now().Before(c.expiresAt) {
			pctx.Credentials = c.creds
			return pctx, true
		}
		// Cache expired, delete it
		s.credCache.Delete(cacheKey)
	}

	// Cache miss - load from database
	conn, err := s.backend.GetConnection(ctx, pctx.WorkspaceId, pctx.MemberId, integration)
	if err != nil {
		log.Warn().Str("integration", integration).Err(err).Msg("connection lookup failed")
		return pctx, false
	}
	if conn == nil {
		return pctx, false
	}

	creds, err := repository.DecryptCredentials(conn)
	if err != nil {
		log.Warn().Str("integration", integration).Err(err).Msg("credential decrypt failed")
		return pctx, false
	}

	// Check if OAuth token needs refresh
	if s.oauthRegistry != nil && oauth.NeedsRefresh(creds) {
		if provider, err := s.oauthRegistry.GetProviderForIntegration(integration); err == nil {
			refreshed, err := provider.Refresh(ctx, creds.RefreshToken)
			if err != nil {
				log.Warn().Str("integration", integration).Str("provider", provider.Name()).Err(err).Msg("token refresh failed")
				// Continue with existing creds - they might still work
			} else {
				// Update stored credentials
				if _, err := s.backend.SaveConnection(ctx, conn.WorkspaceId, conn.MemberId, integration, refreshed, conn.Scope); err != nil {
					log.Warn().Str("integration", integration).Err(err).Msg("failed to persist refreshed token")
				}
				creds = refreshed
			}
		}
	}

	// Store in cache
	s.credCache.Store(cacheKey, &cachedCreds{
		creds:     creds,
		expiresAt: time.Now().Add(credCacheTTL),
	})

	pctx.Credentials = creds
	return pctx, true
}

// CreateSmartQuery creates a new smart query via LLM inference
func (s *SourceService) CreateSmartQuery(ctx context.Context, req *pb.CreateSmartQueryRequest) (*pb.CreateSmartQueryResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.CreateSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}
	workspaceId := auth.WorkspaceId(ctx)

	path := "/sources/" + req.Integration + "/" + req.Name
	if req.FileExt != "" {
		path += req.FileExt
	}

	querySpec, filenameFormat, err := s.inferQuerySpec(ctx, req.Integration, req.Name, req.Guidance)
	if err != nil {
		log.Warn().Err(err).Str("name", req.Name).Str("integration", req.Integration).Msg("BAML inference failed")
		return &pb.CreateSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}

	spec := parseQuerySpec(req.Integration, querySpec)
	if spec.Query == "" {
		return &pb.CreateSmartQueryResponse{Ok: false, Error: "invalid query spec from inference"}, nil
	}
	if filenameFormat == "" {
		filenameFormat = spec.FilenameFormat
	}
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat(req.Integration)
	}

	// Iterative refinement: only for gmail with guidance
	if req.Integration == "gmail" && req.Guidance != "" {
		pctx, err := s.providerContext(ctx)
		if err == nil {
			pctx, connected := s.loadCredentials(ctx, pctx, req.Integration)
			if connected {
				refinedSpec, err := s.refineGmailQueryWithResults(ctx, pctx, req.Guidance, spec.Query)
				if err != nil {
					log.Warn().Err(err).Msg("query refinement failed, using initial query")
				} else if refinedSpec != spec.Query {
					log.Info().Str("original", spec.Query).Str("refined", refinedSpec).Msg("refined gmail query")
					spec.Query = refinedSpec
					querySpec = buildGmailQuerySpec(refinedSpec, spec.Limit, filenameFormat)
				}
			}
		}
	}

	query := &types.FilesystemQuery{
		WorkspaceId:    workspaceId,
		Integration:    req.Integration,
		Path:           path,
		Name:           req.Name,
		QuerySpec:      querySpec,
		Guidance:       req.Guidance,
		OutputFormat:   types.QueryOutputFormat(req.OutputFormat),
		FileExt:        req.FileExt,
		FilenameFormat: filenameFormat,
		CacheTTL:       0,
	}

	created, err := s.fsStore.CreateQuery(ctx, query)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("failed to create query")
		return &pb.CreateSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}

	log.Info().Str("path", path).Str("query", querySpec).Msg("created filesystem query")

	return &pb.CreateSmartQueryResponse{
		Ok:    true,
		Query: smartQueryToProto(created),
	}, nil
}

// inferQuerySpec uses BAML to convert a folder name to a query spec
func (s *SourceService) inferQuerySpec(ctx context.Context, integration, name, guidance string) (string, string, error) {
	var guidancePtr *string

	// For time-relative guidance (e.g. "past week"), add a hidden UTC timestamp hint.
	// IMPORTANT: We do not store this hint in the query's Guidance field; it's only passed to the LLM.
	if integration == "gdrive" {
		now := time.Now().UTC()
		nowHint := fmt.Sprintf("Current time (UTC): %s\nCurrent date (UTC): %s", now.Format(time.RFC3339), now.Format("2006-01-02"))

		g := strings.TrimSpace(guidance)
		if g != "" {
			g += "\n"
		}
		g += nowHint
		guidancePtr = &g
	} else if guidance != "" {
		guidancePtr = &guidance
	}

	switch integration {
	case "gmail":
		result, err := baml.InferGmailQuery(ctx, name, guidancePtr)
		if err != nil {
			return "", "", err
		}
		data, _ := json.Marshal(result)
		return string(data), extractFilenameFormat(data), nil

	case "gdrive":
		result, err := baml.InferGDriveQuery(ctx, name, guidancePtr)
		if err != nil {
			return "", "", err
		}
		data, _ := json.Marshal(result)
		return string(data), extractFilenameFormat(data), nil

	case "notion":
		result, err := baml.InferNotionQuery(ctx, name, guidancePtr)
		if err != nil {
			return "", "", err
		}
		data, _ := json.Marshal(result)
		return string(data), extractFilenameFormat(data), nil

	case "github":
		result, err := baml.InferGitHubQuery(ctx, name, guidancePtr)
		if err != nil {
			return "", "", err
		}
		data, _ := json.Marshal(result)
		return string(data), extractFilenameFormat(data), nil

	case "slack":
		result, err := baml.InferSlackQuery(ctx, name, guidancePtr)
		if err != nil {
			return "", "", err
		}
		data, _ := json.Marshal(result)
		return string(data), extractFilenameFormat(data), nil

	case "linear":
		result, err := baml.InferLinearQuery(ctx, name, guidancePtr)
		if err != nil {
			return "", "", err
		}
		data, _ := json.Marshal(result)
		return string(data), extractFilenameFormat(data), nil

	default:
		return "", "", fmt.Errorf("unsupported integration: %s", integration)
	}
}

// refineGmailQueryWithResults executes the query, evaluates results, and refines up to 2 times
func (s *SourceService) refineGmailQueryWithResults(ctx context.Context, pctx *sources.ProviderContext, guidance, query string) (string, error) {
	const maxIterations = 2

	currentQuery := query
	for i := 0; i < maxIterations; i++ {
		results, err := s.executeQueryForEvaluation(ctx, pctx, "gmail", currentQuery, 20)
		if err != nil {
			return currentQuery, nil
		}

		sampleJSON := formatResultsForEvaluation(results)
		eval, err := baml.EvaluateGmailQueryResults(ctx, guidance, currentQuery, int64(len(results)), sampleJSON)
		if err != nil {
			return currentQuery, nil
		}

		if eval.Is_satisfactory || eval.Refined_query == nil {
			break
		}

		log.Info().
			Str("reasoning", eval.Reasoning).
			Str("old_query", currentQuery).
			Str("new_query", *eval.Refined_query).
			Int("iteration", i).
			Msg("refining gmail query")

		currentQuery = *eval.Refined_query
	}

	return currentQuery, nil
}

// executeQueryForEvaluation runs a lightweight query for evaluation purposes (no caching)
func (s *SourceService) executeQueryForEvaluation(ctx context.Context, pctx *sources.ProviderContext, integration, query string, limit int) ([]sources.QueryResult, error) {
	provider := s.registry.Get(integration)
	if provider == nil {
		return nil, fmt.Errorf("provider not found: %s", integration)
	}

	executor, ok := provider.(sources.QueryExecutor)
	if !ok {
		return nil, fmt.Errorf("provider does not support queries: %s", integration)
	}

	spec := sources.QuerySpec{
		Query: query,
		Limit: limit,
	}

	resp, err := executor.ExecuteQuery(ctx, pctx, spec)
	if err != nil {
		return nil, err
	}
	return resp.Results, nil
}

// formatResultsForEvaluation converts query results to JSON for BAML evaluation
func formatResultsForEvaluation(results []sources.QueryResult) string {
	type sampleResult struct {
		From    string `json:"from"`
		Subject string `json:"subject"`
		Snippet string `json:"snippet"`
	}

	// Take up to 10 sample results
	maxSamples := 10
	if len(results) < maxSamples {
		maxSamples = len(results)
	}

	samples := make([]sampleResult, maxSamples)
	for i := 0; i < maxSamples; i++ {
		r := results[i]
		samples[i] = sampleResult{
			From:    r.Metadata["from"],
			Subject: r.Metadata["subject"],
			Snippet: r.Metadata["snippet"],
		}
	}

	data, _ := json.Marshal(samples)
	return string(data)
}

// GetSmartQuery retrieves a smart query by path
func (s *SourceService) GetSmartQuery(ctx context.Context, req *pb.GetSmartQueryRequest) (*pb.GetSmartQueryResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.GetSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}

	query, err := s.fsStore.GetQuery(ctx, auth.WorkspaceId(ctx), req.Path)
	if err != nil {
		return &pb.GetSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}

	if query == nil {
		return &pb.GetSmartQueryResponse{Ok: true, Query: nil}, nil
	}

	return &pb.GetSmartQueryResponse{
		Ok:    true,
		Query: smartQueryToProto(query),
	}, nil
}

// ListSmartQueries lists queries under a parent path
func (s *SourceService) ListSmartQueries(ctx context.Context, req *pb.ListSmartQueriesRequest) (*pb.ListSmartQueriesResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.ListSmartQueriesResponse{Ok: false, Error: "unauthorized"}, nil
	}

	queries, err := s.fsStore.ListQueries(ctx, auth.WorkspaceId(ctx), req.ParentPath)
	if err != nil {
		return &pb.ListSmartQueriesResponse{Ok: false, Error: err.Error()}, nil
	}

	protoQueries := make([]*pb.SmartQuery, len(queries))
	for i, q := range queries {
		protoQueries[i] = smartQueryToProto(q)
	}

	return &pb.ListSmartQueriesResponse{
		Ok:      true,
		Queries: protoQueries,
	}, nil
}

// DeleteSmartQuery removes a smart query by external_id
func (s *SourceService) DeleteSmartQuery(ctx context.Context, req *pb.DeleteSmartQueryRequest) (*pb.DeleteSmartQueryResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.DeleteSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}

	// Get the query first to verify it belongs to this workspace
	query, err := s.fsStore.GetQueryByExternalId(ctx, req.ExternalId)
	if err != nil || query == nil {
		return &pb.DeleteSmartQueryResponse{Ok: false, Error: "query not found"}, nil
	}

	// Verify the query belongs to this workspace
	if query.WorkspaceId != auth.WorkspaceId(ctx) {
		return &pb.DeleteSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}

	// Invalidate cache for the query path before deletion
	if err := s.fsStore.InvalidateQuery(ctx, query.WorkspaceId, query.Path); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to invalidate query cache")
	}

	if err := s.fsStore.DeleteQuery(ctx, req.ExternalId); err != nil {
		return &pb.DeleteSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}

	log.Info().Str("external_id", req.ExternalId).Str("path", query.Path).Msg("deleted filesystem query")
	return &pb.DeleteSmartQueryResponse{Ok: true}, nil
}

// UpdateSmartQuery updates an existing query's name and/or guidance
func (s *SourceService) UpdateSmartQuery(ctx context.Context, req *pb.UpdateSmartQueryRequest) (*pb.UpdateSmartQueryResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.UpdateSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}
	workspaceId := auth.WorkspaceId(ctx)

	// Get the existing query by external_id
	query, err := s.fsStore.GetQueryByExternalId(ctx, req.ExternalId)
	if err != nil || query == nil {
		return &pb.UpdateSmartQueryResponse{Ok: false, Error: "query not found"}, nil
	}

	// Verify the query belongs to this workspace
	if query.WorkspaceId != workspaceId {
		return &pb.UpdateSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}

	oldPath := query.Path
	needsUpdate := false

	// Update name and recalculate path if name changed
	if req.Name != "" && req.Name != query.Name {
		query.Name = req.Name
		// Recalculate path: /sources/{integration}/{name}
		query.Path = "/sources/" + query.Integration + "/" + req.Name
		if query.FileExt != "" {
			query.Path += query.FileExt
		}
		needsUpdate = true
	}

	// Re-run LLM inference if guidance changed
	if req.Guidance != query.Guidance {
		query.Guidance = req.Guidance

		querySpec, filenameFormat, err := s.inferQuerySpec(ctx, query.Integration, query.Name, req.Guidance)
		if err != nil {
			log.Warn().Err(err).Str("name", query.Name).Str("integration", query.Integration).Msg("BAML inference failed during update")
			return &pb.UpdateSmartQueryResponse{Ok: false, Error: "failed to regenerate query: " + err.Error()}, nil
		}

		spec := parseQuerySpec(query.Integration, querySpec)
		if spec.Query == "" {
			return &pb.UpdateSmartQueryResponse{Ok: false, Error: "invalid query spec from inference"}, nil
		}

		query.QuerySpec = querySpec
		if filenameFormat != "" {
			query.FilenameFormat = filenameFormat
		}
		needsUpdate = true
	}

	if !needsUpdate {
		// Nothing to update
		return &pb.UpdateSmartQueryResponse{
			Ok:    true,
			Query: filesystemQueryToProto(query),
		}, nil
	}

	// Update the query in the store
	if err := s.fsStore.UpdateQuery(ctx, query); err != nil {
		log.Error().Err(err).Str("external_id", req.ExternalId).Msg("failed to update query")
		return &pb.UpdateSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}

	// Invalidate cache for the old path (if path changed) and new path
	if oldPath != query.Path {
		if err := s.fsStore.InvalidateQuery(ctx, workspaceId, oldPath); err != nil {
			log.Warn().Err(err).Str("path", oldPath).Msg("failed to invalidate old query cache")
		}
	}
	if err := s.fsStore.InvalidateQuery(ctx, workspaceId, query.Path); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to invalidate query cache")
	}

	log.Info().
		Str("external_id", req.ExternalId).
		Str("old_path", oldPath).
		Str("new_path", query.Path).
		Str("name", query.Name).
		Msg("updated filesystem query")

	return &pb.UpdateSmartQueryResponse{
		Ok:    true,
		Query: filesystemQueryToProto(query),
	}, nil
}

// ExecuteSmartQuery runs a query and returns materialized results
func (s *SourceService) ExecuteSmartQuery(ctx context.Context, req *pb.ExecuteSmartQueryRequest) (*pb.ExecuteSmartQueryResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}

	workspaceId := auth.WorkspaceId(ctx)
	query, err := s.fsStore.GetQuery(ctx, workspaceId, req.Path)
	if err != nil || query == nil {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "query not found"}, nil
	}

	// Get provider context with credentials
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}

	pctx, connected := s.loadCredentials(ctx, pctx, query.Integration)
	if !connected {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "not connected"}, nil
	}

	// Get provider
	provider := s.registry.Get(query.Integration)
	if provider == nil {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "integration not available"}, nil
	}

	// Check if provider implements QueryExecutor
	executor, hasExecutor := provider.(sources.QueryExecutor)

	// If requesting specific file content
	if req.Filename != "" {
		if !hasExecutor {
			return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "provider does not support queries"}, nil
		}

		// If ResultId is provided directly, use it
		resultId := req.ResultId

		// Otherwise, find the result ID from cached results
		if resultId == "" {
			results, err := s.getOrExecuteQuery(ctx, pctx, query)
			if err != nil {
				return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "failed to get query results: " + err.Error()}, nil
			}

			// Find matching result by filename
			for _, r := range results {
				if r.Filename == req.Filename {
					resultId = r.ID
					break
				}
			}

			if resultId == "" {
				return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "file not found in query results"}, nil
			}
		}

		// Try cached content first
		if content, err := s.fsStore.GetResultContent(ctx, workspaceId, req.Path, resultId); err == nil && len(content) > 0 {
			return &pb.ExecuteSmartQueryResponse{Ok: true, FileData: content}, nil
		}

		// Read content using QueryExecutor
		data, err := executor.ReadResult(ctx, pctx, resultId)
		if err != nil {
			return &pb.ExecuteSmartQueryResponse{Ok: false, Error: err.Error()}, nil
		}

		// Cache the content
		if err := s.fsStore.StoreResultContent(ctx, workspaceId, req.Path, resultId, data); err != nil {
			log.Warn().Err(err).Str("path", req.Path).Str("result", resultId).Msg("failed to cache query result content")
		}
		return &pb.ExecuteSmartQueryResponse{Ok: true, FileData: data}, nil
	}

	results, err := s.getOrExecuteQuery(ctx, pctx, query)
	if err != nil {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}

	entries := make([]*pb.SourceDirEntry, 0, len(results))
	for _, r := range results {
		entries = append(entries, &pb.SourceDirEntry{
			Name:     r.Filename,
			Mode:     sources.ModeFile,
			Size:     r.Size,
			Mtime:    r.Mtime,
			ResultId: r.ID,
		})
	}

	return &pb.ExecuteSmartQueryResponse{Ok: true, Entries: entries}, nil
}

// Default pagination settings
const (
	defaultPageSize   = 50  // Default number of results per page
	defaultMaxResults = 500 // Default max total results across all pages
)

// parseQuerySpec extracts the query string, limit, and filename format from a query spec JSON
func parseQuerySpec(integration, querySpec string) sources.QuerySpec {
	var spec struct {
		GmailQuery     string `json:"gmail_query"`
		GDriveQuery    string `json:"gdrive_query"`
		NotionQuery    string `json:"notion_query"`
		GitHubQuery    string `json:"github_query"`
		SlackQuery     string `json:"slack_query"`
		LinearQuery    string `json:"linear_query"`
		SearchType     string `json:"search_type"`
		ContentType    string `json:"content_type"`
		Limit          int    `json:"limit"`
		MaxResults     int    `json:"max_results"`
		FilenameFormat string `json:"filename_format"`
	}

	limit := defaultPageSize
	maxResults := defaultMaxResults
	if json.Unmarshal([]byte(querySpec), &spec) == nil {
		if spec.Limit > 0 {
			limit = spec.Limit
		}
		if spec.MaxResults > 0 {
			maxResults = spec.MaxResults
		}
	}

	// Ensure maxResults doesn't exceed the hard limit
	if maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	var query string
	switch integration {
	case "gmail":
		query = spec.GmailQuery
	case "gdrive":
		query = spec.GDriveQuery
	case "notion":
		query = spec.NotionQuery
	case "github":
		query = spec.GitHubQuery
	case "slack":
		query = spec.SlackQuery
	case "linear":
		query = spec.LinearQuery
	}

	filenameFormat := spec.FilenameFormat
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat(integration)
	}

	// Build metadata for provider-specific options
	metadata := make(map[string]string)
	if spec.SearchType != "" {
		metadata["search_type"] = spec.SearchType
	}
	if spec.ContentType != "" {
		metadata["content_type"] = spec.ContentType
	}

	return sources.QuerySpec{
		Query:          query,
		Limit:          limit,
		MaxResults:     maxResults,
		FilenameFormat: filenameFormat,
		Metadata:       metadata,
	}
}

func extractFilenameFormat(specJSON []byte) string {
	var spec struct {
		FilenameFormat string `json:"filename_format"`
	}
	if json.Unmarshal(specJSON, &spec) != nil {
		return ""
	}
	return spec.FilenameFormat
}

// smartQueryToProto converts a types.SmartQuery to pb.SmartQuery
func smartQueryToProto(q *types.SmartQuery) *pb.SmartQuery {
	if q == nil {
		return nil
	}
	return &pb.SmartQuery{
		ExternalId:   q.ExternalId,
		Integration:  q.Integration,
		Path:         q.Path,
		Name:         q.Name,
		QuerySpec:    q.QuerySpec,
		Guidance:     q.Guidance,
		OutputFormat: string(q.OutputFormat),
		FileExt:      q.FileExt,
		CacheTtl:     int32(q.CacheTTL),
		CreatedAt:    q.CreatedAt.Unix(),
		UpdatedAt:    q.UpdatedAt.Unix(),
	}
}

// filesystemQueryToProto converts a types.FilesystemQuery to pb.SmartQuery
// FilesystemQuery and SmartQuery are type aliases, so this is a convenience wrapper
func filesystemQueryToProto(q *types.FilesystemQuery) *pb.SmartQuery {
	return smartQueryToProto(q)
}

// cleanPath normalizes a path by removing leading/trailing slashes
func cleanPath(path string) string {
	return strings.Trim(path, "/")
}

// splitIntegrationPath splits a path into integration name and relative path
// e.g., "github/views/repos.json" -> ("github", "views/repos.json")
func splitIntegrationPath(path string) (integration, relPath string) {
	parts := strings.SplitN(path, "/", 2)
	integration = parts[0]
	if len(parts) > 1 {
		relPath = parts[1]
	}
	return
}

// readSlice returns a slice of data based on offset and length
func readSlice(data []byte, offset, length int64) *pb.SourceReadResponse {
	if offset >= int64(len(data)) {
		return &pb.SourceReadResponse{Ok: true, Data: nil}
	}

	end := int64(len(data))
	if length > 0 && offset+length < end {
		end = offset + length
	}

	return &pb.SourceReadResponse{Ok: true, Data: data[offset:end]}
}

// errorToCode maps errors to sensible errno values
func errorToCode(err error) int {
	if err == sources.ErrNotFound {
		return int(syscall.ENOENT)
	}
	if err == sources.ErrNotConnected {
		return int(syscall.EACCES)
	}
	if err == sources.ErrNotDir {
		return int(syscall.ENOTDIR)
	}
	if err == sources.ErrIsDir {
		return int(syscall.EISDIR)
	}
	return int(syscall.EIO)
}
