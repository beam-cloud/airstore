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

// SourceService implements the gRPC SourceService for integration access.
type SourceService struct {
	pb.UnimplementedSourceServiceServer
	registry    *sources.Registry
	backend     repository.BackendRepository
	fsStore     repository.FilesystemStore
	cache       *sources.SourceCache
	rateLimiter *sources.RateLimiter
	googleOAuth *oauth.GoogleClient
	credCache   sync.Map // map[string]*cachedCreds - caches credentials by "workspaceId:integration"
	queryGroup  singleflight.Group
}

// NewSourceService creates a new SourceService.
func NewSourceService(registry *sources.Registry, backend repository.BackendRepository, fsStore repository.FilesystemStore) *SourceService {
	return &SourceService{
		registry:    registry,
		backend:     backend,
		fsStore:     fsStore,
		cache:       sources.NewSourceCache(sources.DefaultCacheTTL, sources.DefaultCacheSize),
		rateLimiter: sources.NewRateLimiter(sources.DefaultRateLimitConfig()),
	}
}

// NewSourceServiceWithOAuth creates a SourceService with OAuth refresh support.
func NewSourceServiceWithOAuth(registry *sources.Registry, backend repository.BackendRepository, fsStore repository.FilesystemStore, googleOAuth *oauth.GoogleClient) *SourceService {
	return &SourceService{
		registry:    registry,
		backend:     backend,
		fsStore:     fsStore,
		cache:       sources.NewSourceCache(sources.DefaultCacheTTL, sources.DefaultCacheSize),
		rateLimiter: sources.NewRateLimiter(sources.DefaultRateLimitConfig()),
		googleOAuth: googleOAuth,
	}
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

	// Handle status.json specially (no caching needed, cheap to generate)
	if relPath == "status.json" {
		workspaceId := ""
		scope := ""
		if connected && pctx.Credentials != nil {
			scope = "shared" // TODO: detect personal vs shared
		}
		data := sources.GenerateStatusJSON(integration, connected, scope, workspaceId)
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

	// Integration root (e.g., /sources/gmail) - show status.json + smart queries only
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

	// Check for .query metadata file
	if strings.HasSuffix(relPath, ".query") {
		// .query files are handled by Read, not ReadDir
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

// readDirIntegrationRoot returns entries for integration root: status.json + smart queries
func (s *SourceService) readDirIntegrationRoot(ctx context.Context, pctx *sources.ProviderContext, integration string, connected bool) (*pb.SourceReadDirResponse, error) {
	// Generate status.json to get its size
	scope := ""
	if connected && pctx.Credentials != nil {
		scope = "shared"
	}
	statusData := sources.GenerateStatusJSON(integration, connected, scope, "")

	entries := []*pb.SourceDirEntry{
		{Name: "status.json", Mode: sources.ModeFile, Size: int64(len(statusData)), Mtime: sources.NowUnix()},
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
				entries = append(entries, &pb.SourceDirEntry{
					Name:  name,
					Mode:  sources.ModeDir,
					IsDir: true,
					Mtime: q.UpdatedAt.Unix(),
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
				Name:  "." + filename + ".query",
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

	// Always include .query metadata file
	queryMeta := s.generateQueryMetaJSON(query)
	entries = append(entries, &pb.SourceDirEntry{
		Name:  ".query",
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

// executeAndCacheQuery executes a smart query and caches the results
func (s *SourceService) executeAndCacheQuery(ctx context.Context, pctx *sources.ProviderContext, query *types.FilesystemQuery) ([]repository.QueryResult, error) {
	provider := s.registry.Get(query.Integration)
	if provider == nil {
		return nil, fmt.Errorf("provider not found: %s", query.Integration)
	}

	executor, ok := provider.(sources.QueryExecutor)
	if !ok {
		return nil, fmt.Errorf("provider does not support queries: %s", query.Integration)
	}

	// Parse query spec
	spec := parseQuerySpec(query.Integration, query.QuerySpec)
	if spec.Query == "" {
		return nil, fmt.Errorf("empty query spec for %s", query.Integration)
	}

	// Execute the query
	queryResults, err := executor.ExecuteQuery(ctx, pctx, spec)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	// Convert to repository format
	results := make([]repository.QueryResult, len(queryResults))
	filenameFormat := query.FilenameFormat
	if filenameFormat == "" {
		filenameFormat = spec.FilenameFormat
	}
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat(query.Integration)
	}
	for i, qr := range queryResults {
		filename := qr.Filename
		if filename == "" {
			filename = executor.FormatFilename(filenameFormat, qr.Metadata)
		}
		results[i] = repository.QueryResult{
			ID:       qr.ID,
			Filename: filename,
			Metadata: qr.Metadata,
			Size:     qr.Size,
			Mtime:    qr.Mtime,
		}
	}

	// Cache results
	ttl := time.Duration(query.CacheTTL) * time.Second
	if ttl == 0 {
		ttl = 5 * time.Minute // Default TTL
	}
	if err := s.fsStore.StoreQueryResults(ctx, pctx.WorkspaceId, query.Path, results, ttl); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to cache query results")
	}

	// Update last_executed timestamp
	now := time.Now()
	query.LastExecuted = &now
	if err := s.fsStore.UpdateQuery(ctx, query); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to update query timestamp")
	}

	return results, nil
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

// generateQueryMetaJSON creates the JSON content for a .query metadata file
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

	// Handle status.json
	if relPath == "status.json" {
		scope := ""
		if connected && pctx.Credentials != nil {
			scope = "shared"
		}
		data := sources.GenerateStatusJSON(integration, connected, scope, "")
		return readSlice(data, req.Offset, req.Length), nil
	}

	if relPath == "" {
		return &pb.SourceReadResponse{Ok: false, Error: "is a directory"}, nil
	}

	// Handle .query metadata files
	if relPath == ".query" || strings.HasSuffix(relPath, "/.query") {
		// Get the parent query path
		queryPath := "/sources/" + integration
		if relPath != ".query" {
			queryPath = "/sources/" + integration + "/" + strings.TrimSuffix(relPath, "/.query")
		}

		query, err := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, queryPath)
		if err != nil || query == nil {
			return &pb.SourceReadResponse{Ok: false, Error: "query not found"}, nil
		}

		data := s.generateQueryMetaJSON(query)
		return readSlice(data, req.Offset, req.Length), nil
	}

	// Handle .{filename}.query metadata files for single-file queries
	base := path.Base(relPath)
	if strings.HasPrefix(base, ".") && strings.HasSuffix(base, ".query") {
		filename := strings.TrimPrefix(strings.TrimSuffix(base, ".query"), ".")
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
	rc := auth.FromContext(ctx)
	if rc == nil {
		// Allow unauthenticated access with empty context (for local mode)
		return &sources.ProviderContext{}, nil
	}

	return &sources.ProviderContext{
		WorkspaceId: rc.WorkspaceId,
		MemberId:    rc.MemberId,
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

	// Check if Google OAuth token needs refresh
	if oauth.IsGoogleIntegration(integration) && oauth.NeedsRefresh(creds) && s.googleOAuth != nil {
		refreshed, err := s.googleOAuth.Refresh(ctx, creds.RefreshToken)
		if err != nil {
			log.Warn().Str("integration", integration).Err(err).Msg("token refresh failed")
			// Continue with existing creds - they might still work
		} else {
			// Update stored credentials
			if _, err := s.backend.SaveConnection(ctx, conn.WorkspaceId, conn.MemberId, integration, refreshed, conn.Scope); err != nil {
				log.Warn().Str("integration", integration).Err(err).Msg("failed to persist refreshed token")
			}
			creds = refreshed
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
	if !auth.HasWorkspace(ctx) {
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
	if guidance != "" {
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

	default:
		return "", "", fmt.Errorf("unsupported integration: %s", integration)
	}
}

// GetSmartQuery retrieves a smart query by path
func (s *SourceService) GetSmartQuery(ctx context.Context, req *pb.GetSmartQueryRequest) (*pb.GetSmartQueryResponse, error) {
	if !auth.HasWorkspace(ctx) {
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
	if !auth.HasWorkspace(ctx) {
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

// DeleteSmartQuery removes a smart query
func (s *SourceService) DeleteSmartQuery(ctx context.Context, req *pb.DeleteSmartQueryRequest) (*pb.DeleteSmartQueryResponse, error) {
	if !auth.HasWorkspace(ctx) {
		return &pb.DeleteSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}

	// Get the query first to verify it belongs to this workspace
	query, err := s.fsStore.GetQuery(ctx, auth.WorkspaceId(ctx), req.Path)
	if err != nil || query == nil {
		return &pb.DeleteSmartQueryResponse{Ok: false, Error: "query not found"}, nil
	}

	if err := s.fsStore.DeleteQuery(ctx, query.ExternalId); err != nil {
		return &pb.DeleteSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.DeleteSmartQueryResponse{Ok: true}, nil
}

// ExecuteSmartQuery runs a query and returns materialized results
func (s *SourceService) ExecuteSmartQuery(ctx context.Context, req *pb.ExecuteSmartQueryRequest) (*pb.ExecuteSmartQueryResponse, error) {
	if !auth.HasWorkspace(ctx) {
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

// parseQuerySpec extracts the query string, limit, and filename format from a query spec JSON
func parseQuerySpec(integration, querySpec string) sources.QuerySpec {
	var spec struct {
		GmailQuery     string `json:"gmail_query"`
		GDriveQuery    string `json:"gdrive_query"`
		NotionQuery    string `json:"notion_query"`
		Limit          int    `json:"limit"`
		FilenameFormat string `json:"filename_format"`
	}

	limit := 50
	if json.Unmarshal([]byte(querySpec), &spec) == nil && spec.Limit > 0 {
		limit = spec.Limit
	}

	var query string
	switch integration {
	case "gmail":
		query = spec.GmailQuery
	case "gdrive":
		query = spec.GDriveQuery
	case "notion":
		query = spec.NotionQuery
	}

	filenameFormat := spec.FilenameFormat
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat(integration)
	}

	return sources.QuerySpec{
		Query:          query,
		Limit:          limit,
		FilenameFormat: filenameFormat,
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
