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
	"github.com/beam-cloud/airstore/pkg/compression"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/instrumentation"
	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"
	grpcmd "google.golang.org/grpc/metadata"
)

// ---------------------------------------------------------------------------
// Service definition
// ---------------------------------------------------------------------------

const credCacheTTL = 5 * time.Minute  // credentials cache duration
const connCacheTTL = 10 * time.Second // connected-integrations cache duration

type cachedCreds struct {
	creds     *types.IntegrationCredentials
	expiresAt time.Time
}

type cachedConnSet struct {
	set       map[string]bool
	expiresAt time.Time
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
	credCache     sync.Map // map[string]*cachedCreds
	connCache     sync.Map // map[uint]*cachedConnSet
	queryGroup    singleflight.Group
	hookStream    common.EventEmitter
	seenTracker   *hooks.SeenTracker

	// Compression middleware (optional).
	compressor      compression.ContextCompressor
	compressedStore *compression.CompressedStore
	recorder        instrumentation.AccessRecorder
	compressionCfg  compression.Config
	passthroughOnce sync.Once
	passthroughComp compression.ContextCompressor
}

type SourceServiceOption func(*SourceService)

func WithHookStream(emitter common.EventEmitter) SourceServiceOption {
	return func(s *SourceService) { s.hookStream = emitter }
}

func WithSeenTracker(tracker *hooks.SeenTracker) SourceServiceOption {
	return func(s *SourceService) { s.seenTracker = tracker }
}

func WithRecorder(recorder instrumentation.AccessRecorder) SourceServiceOption {
	return func(s *SourceService) { s.recorder = recorder }
}

func WithCompressionMiddleware(
	compressor compression.ContextCompressor,
	store *compression.CompressedStore,
	cfg compression.Config,
) SourceServiceOption {
	return func(s *SourceService) {
		s.compressor = compressor
		s.compressedStore = store
		s.compressionCfg = cfg
	}
}

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

// ---------------------------------------------------------------------------
// FUSE operations
// ---------------------------------------------------------------------------

func (s *SourceService) Stat(ctx context.Context, req *pb.SourceStatRequest) (*pb.SourceStatResponse, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.SourceStatResponse{Ok: false, Error: err.Error()}, nil
	}

	p := cleanPath(req.Path)

	// Root /sources directory.
	if p == "" {
		return &pb.SourceStatResponse{
			Ok:   true,
			Info: &pb.SourceFileInfo{Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
		}, nil
	}

	integration, relPath := splitIntegrationPath(p)

	provider := s.registry.Get(integration)
	if provider == nil {
		return &pb.SourceStatResponse{Ok: false, Error: "integration not found"}, nil
	}
	if !s.isIntegrationVisible(ctx, pctx.WorkspaceId, integration) {
		return &pb.SourceStatResponse{Ok: false, Error: "integration not connected"}, nil
	}

	pctx, connected := s.loadCredentials(ctx, pctx, integration)

	// Integration root.
	if relPath == "" {
		return &pb.SourceStatResponse{
			Ok:   true,
			Info: &pb.SourceFileInfo{Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
		}, nil
	}

	// README.md (cheap to generate, never cached).
	if relPath == types.SourceStatusFile {
		scope := ""
		if connected && pctx.Credentials != nil {
			scope = "shared"
		}
		data := sources.GenerateSourceReadme(integration, connected, scope, "")
		return &pb.SourceStatResponse{
			Ok:   true,
			Info: &pb.SourceFileInfo{Size: int64(len(data)), Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil
	}

	// Check stat cache.
	cacheKey := sources.CacheKey(pctx.WorkspaceId, integration, relPath, "stat")
	if info, ok := s.cache.GetInfo(cacheKey); ok {
		return &pb.SourceStatResponse{
			Ok: true,
			Info: &pb.SourceFileInfo{
				Size: info.Size, Mode: info.Mode, Mtime: info.Mtime,
				IsDir: info.IsDir, IsLink: info.IsLink,
			},
		}, nil
	}

	// Source view result file or view folder.
	queryPath, filename := s.findQueryAndFilename(ctx, pctx.WorkspaceId, integration, relPath)
	if queryPath != "" {
		results, err := s.fsStore.GetQueryResults(ctx, pctx.WorkspaceId, queryPath)
		if err == nil {
			for _, r := range results {
				if r.Filename == filename {
					mtime := r.Mtime
					if mtime == 0 {
						mtime = sources.NowUnix()
					}
					return &pb.SourceStatResponse{
						Ok:   true,
						Info: &pb.SourceFileInfo{Size: r.Size, Mode: sources.ModeFile, Mtime: mtime},
					}, nil
				}
			}
		}
	} else {
		// Check if the path itself is a source view folder.
		qp := types.PathSources + "/" + integration + "/" + relPath
		if q, err := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, qp); err == nil && q != nil {
			return &pb.SourceStatResponse{
				Ok:   true,
				Info: &pb.SourceFileInfo{Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
			}, nil
		}
	}

	// Rate-limited provider stat with singleflight.
	if err := s.rateLimiter.Wait(ctx, pctx.WorkspaceId, integration); err != nil {
		return &pb.SourceStatResponse{Ok: false, Error: "rate limited"}, nil
	}
	result, err := s.cache.DoOnce(cacheKey, func() (any, error) {
		return provider.Stat(ctx, pctx, relPath)
	})
	if err != nil {
		return &pb.SourceStatResponse{Ok: false, Error: err.Error()}, nil
	}

	info := result.(*sources.FileInfo)
	s.cache.SetInfo(cacheKey, info)

	return &pb.SourceStatResponse{
		Ok: true,
		Info: &pb.SourceFileInfo{
			Size: info.Size, Mode: info.Mode, Mtime: info.Mtime,
			IsDir: info.IsDir, IsLink: info.IsLink,
		},
	}, nil
}

func (s *SourceService) ReadDir(ctx context.Context, req *pb.SourceReadDirRequest) (*pb.SourceReadDirResponse, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.SourceReadDirResponse{Ok: false, Error: err.Error()}, nil
	}

	p := cleanPath(req.Path)

	// Root /sources — list visible integrations.
	// Built-in integrations (AuthNone) are always shown; others require a connection.
	if p == "" {
		allNames := s.registry.List()
		entries := make([]*pb.SourceDirEntry, 0, len(allNames))
		for _, name := range allNames {
			if !s.isIntegrationVisible(ctx, pctx.WorkspaceId, name) {
				continue
			}
			entries = append(entries, &pb.SourceDirEntry{
				Name: name, Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix(),
			})
		}
		return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
	}

	integration, relPath := splitIntegrationPath(p)

	provider := s.registry.Get(integration)
	if provider == nil {
		return &pb.SourceReadDirResponse{Ok: false, Error: "integration not found"}, nil
	}
	if !s.isIntegrationVisible(ctx, pctx.WorkspaceId, integration) {
		return &pb.SourceReadDirResponse{Ok: false, Error: "integration not connected"}, nil
	}

	pctx, connected := s.loadCredentials(ctx, pctx, integration)

	// Integration root.
	if relPath == "" {
		return s.readDirIntegrationRoot(ctx, pctx, integration, connected)
	}

	// Source view folder.
	queryPath := types.PathSources + "/" + p
	query, err := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, queryPath)
	if err != nil {
		log.Debug().Err(err).Str("path", queryPath).Msg("query lookup error")
	}
	if query != nil && query.OutputFormat == types.ViewOutputFolder {
		return s.readDirView(ctx, pctx, query, connected)
	}

	// .query.as is a file, not a directory.
	if strings.HasSuffix(relPath, ".query.as") {
		return &pb.SourceReadDirResponse{Ok: false, Error: "not a directory"}, nil
	}

	// If parent is a source view, this is inside a result — no subdirectories.
	parentPath := types.PathSources + "/" + integration
	if idx := strings.LastIndex(relPath, "/"); idx > 0 {
		parentPath = types.PathSources + "/" + integration + "/" + relPath[:idx]
	}
	if parentQuery, _ := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, parentPath); parentQuery != nil {
		return &pb.SourceReadDirResponse{Ok: true, Entries: []*pb.SourceDirEntry{}}, nil
	}

	// NativeBrowsable provider.
	if nb, ok := provider.(sources.NativeBrowsable); ok && nb.IsNativeBrowsable() && connected {
		cacheKey := sources.CacheKey(pctx.WorkspaceId, integration, relPath, "readdir")
		nativeEntries, cacheHit := s.cache.GetEntries(cacheKey)

		if !cacheHit {
			if err := s.rateLimiter.Wait(ctx, pctx.WorkspaceId, integration); err != nil {
				return &pb.SourceReadDirResponse{Ok: false, Error: "rate limited"}, nil
			}
			result, err := s.cache.DoOnce(cacheKey, func() (any, error) {
				return provider.ReadDir(ctx, pctx, relPath)
			})
			if err != nil {
				return &pb.SourceReadDirResponse{Ok: true, Entries: []*pb.SourceDirEntry{}}, nil
			}
			nativeEntries = result.([]sources.DirEntry)
			s.cache.SetEntries(cacheKey, nativeEntries)
		}

		pbEntries := make([]*pb.SourceDirEntry, 0, len(nativeEntries))
		for _, e := range nativeEntries {
			pbEntries = append(pbEntries, &pb.SourceDirEntry{
				Name: e.Name, Mode: e.Mode, IsDir: e.IsDir, Size: e.Size, Mtime: e.Mtime,
			})
		}
		return &pb.SourceReadDirResponse{Ok: true, Entries: pbEntries}, nil
	}

	return &pb.SourceReadDirResponse{Ok: true, Entries: []*pb.SourceDirEntry{}}, nil
}

func (s *SourceService) Read(ctx context.Context, req *pb.SourceReadRequest) (*pb.SourceReadResponse, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.SourceReadResponse{Ok: false, Error: err.Error()}, nil
	}

	cleanedPath := cleanPath(req.Path)
	if cleanedPath == "" {
		return &pb.SourceReadResponse{Ok: false, Error: "is a directory"}, nil
	}

	integration, relPath := splitIntegrationPath(cleanedPath)

	provider := s.registry.Get(integration)
	if provider == nil {
		return &pb.SourceReadResponse{Ok: false, Error: "integration not found"}, nil
	}
	if !s.isIntegrationVisible(ctx, pctx.WorkspaceId, integration) {
		return &pb.SourceReadResponse{Ok: false, Error: "integration not connected"}, nil
	}

	pctx, connected := s.loadCredentials(ctx, pctx, integration)

	// README.md
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

	// .query.as metadata files (folder-level).
	if relPath == ".query.as" || strings.HasSuffix(relPath, "/.query.as") {
		queryPath := types.PathSources + "/" + integration
		if relPath != ".query.as" {
			queryPath = types.PathSources + "/" + integration + "/" + strings.TrimSuffix(relPath, "/.query.as")
		}
		query, err := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, queryPath)
		if err != nil || query == nil {
			return &pb.SourceReadResponse{Ok: false, Error: "query not found"}, nil
		}
		return readSlice(s.generateQueryMetaJSON(query), req.Offset, req.Length), nil
	}

	// .{filename}.query.as metadata files (single-file queries).
	base := path.Base(relPath)
	if strings.HasPrefix(base, ".") && strings.HasSuffix(base, ".query.as") {
		filename := strings.TrimPrefix(strings.TrimSuffix(base, ".query.as"), ".")
		dir := path.Dir(relPath)
		queryPath := types.PathSources + "/" + integration
		if dir != "." && dir != "" {
			queryPath += "/" + dir
		}
		queryPath += "/" + filename
		query, err := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, queryPath)
		if err != nil || query == nil || query.OutputFormat != types.ViewOutputFile {
			return &pb.SourceReadResponse{Ok: false, Error: "query not found"}, nil
		}
		return readSlice(s.generateQueryMetaJSON(query), req.Offset, req.Length), nil
	}

	// Source view result file.
	queryPath, filename := s.findQueryAndFilename(ctx, pctx.WorkspaceId, integration, relPath)
	if queryPath != "" {
		return s.readViewResult(ctx, pctx, queryPath, filename, req.Offset, req.Length)
	}

	// NativeBrowsable provider read.
	if nb, ok := provider.(sources.NativeBrowsable); ok && nb.IsNativeBrowsable() && connected {
		if err := s.rateLimiter.Wait(ctx, pctx.WorkspaceId, integration); err != nil {
			return &pb.SourceReadResponse{Ok: false, Error: "rate limited"}, nil
		}
		data, err := provider.Read(ctx, pctx, relPath, req.Offset, req.Length)
		if err != nil {
			return &pb.SourceReadResponse{Ok: false, Error: err.Error()}, nil
		}
		return &pb.SourceReadResponse{
			Ok:       true,
			Data:     data,
			CostHint: s.passthroughCostHint(ctx, integration, "", relPath, relPath, data),
		}, nil
	}

	return &pb.SourceReadResponse{Ok: false, Error: "file not found"}, nil
}

func (s *SourceService) Readlink(ctx context.Context, req *pb.SourceReadlinkRequest) (*pb.SourceReadlinkResponse, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.SourceReadlinkResponse{Ok: false, Error: err.Error()}, nil
	}

	p := cleanPath(req.Path)
	if p == "" {
		return &pb.SourceReadlinkResponse{Ok: false, Error: "not a symlink"}, nil
	}

	integration, relPath := splitIntegrationPath(p)
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

// ---------------------------------------------------------------------------
// ReadDir helpers
// ---------------------------------------------------------------------------

func (s *SourceService) readDirIntegrationRoot(ctx context.Context, pctx *sources.ProviderContext, integration string, connected bool) (*pb.SourceReadDirResponse, error) {
	scope := ""
	if connected && pctx.Credentials != nil {
		scope = "shared"
	}
	statusData := sources.GenerateSourceReadme(integration, connected, scope, "")

	entries := []*pb.SourceDirEntry{
		{Name: types.SourceStatusFile, Mode: sources.ModeFile, Size: int64(len(statusData)), Mtime: sources.NowUnix()},
	}

	// Smart queries for this integration.
	parentPath := types.PathSources + "/" + integration
	queries, err := s.fsStore.ListQueries(ctx, pctx.WorkspaceId, parentPath)
	if err != nil {
		log.Warn().Err(err).Str("path", parentPath).Msg("failed to list smart queries")
	} else {
		for _, q := range queries {
			if q.Path == parentPath {
				continue
			}
			name := strings.TrimPrefix(q.Path, parentPath+"/")
			if strings.Contains(name, "/") {
				continue // skip nested entries
			}

			if q.OutputFormat == types.ViewOutputFolder {
				entries = append(entries, &pb.SourceDirEntry{
					Name: name, Mode: sources.ModeDir, IsDir: true,
					Mtime: q.UpdatedAt.Unix(), ChildCount: int32(s.getQueryChildCount(ctx, pctx.WorkspaceId, q.Path)),
				})
				continue
			}

			// Single-file query.
			filename := q.Name
			if q.FileExt != "" {
				filename = q.Name + q.FileExt
			}
			entries = append(entries, &pb.SourceDirEntry{
				Name: filename, Mode: sources.ModeFile, Mtime: q.UpdatedAt.Unix(),
			})
			queryMeta := s.generateQueryMetaJSON(q)
			entries = append(entries, &pb.SourceDirEntry{
				Name: "." + filename + ".query.as", Mode: sources.ModeFile | 0444,
				Size: int64(len(queryMeta)), Mtime: q.UpdatedAt.Unix(),
			})
		}
	}

	// Merge NativeBrowsable root entries.
	provider := s.registry.Get(integration)
	if provider != nil && connected {
		if nb, ok := provider.(sources.NativeBrowsable); ok && nb.IsNativeBrowsable() {
			cacheKey := sources.CacheKey(pctx.WorkspaceId, integration, "", "readdir")
			nativeEntries, cacheHit := s.cache.GetEntries(cacheKey)

			if !cacheHit {
				if err := s.rateLimiter.Wait(ctx, pctx.WorkspaceId, integration); err == nil {
					if result, err := s.cache.DoOnce(cacheKey, func() (any, error) {
						return provider.ReadDir(ctx, pctx, "")
					}); err == nil {
						nativeEntries = result.([]sources.DirEntry)
						s.cache.SetEntries(cacheKey, nativeEntries)
					}
				}
			}

			existing := make(map[string]bool, len(entries))
			for _, e := range entries {
				existing[e.Name] = true
			}
			for _, ne := range nativeEntries {
				if !existing[ne.Name] {
					entries = append(entries, &pb.SourceDirEntry{
						Name: ne.Name, Mode: ne.Mode, IsDir: ne.IsDir, Size: ne.Size, Mtime: ne.Mtime,
					})
				}
			}
		}
	}

	return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
}

func (s *SourceService) readDirView(ctx context.Context, pctx *sources.ProviderContext, query *types.FilesystemQuery, connected bool) (*pb.SourceReadDirResponse, error) {
	queryMeta := s.generateQueryMetaJSON(query)
	entries := []*pb.SourceDirEntry{
		{Name: ".query.as", Mode: sources.ModeFile | 0444, Size: int64(len(queryMeta)), Mtime: query.UpdatedAt.Unix()},
	}

	if !connected {
		return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
	}

	results, err := s.getOrExecuteQuery(ctx, pctx, query)
	if err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to execute source view query")
		return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
	}

	for _, r := range results {
		entries = append(entries, &pb.SourceDirEntry{
			Name: r.Filename, Mode: sources.ModeFile, Size: r.Size, Mtime: r.Mtime, ResultId: r.ID,
		})
	}
	return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
}

func (s *SourceService) getQueryChildCount(ctx context.Context, workspaceId uint, queryPath string) int {
	if s.fsStore == nil {
		return 1
	}
	results, err := s.fsStore.GetQueryResults(ctx, workspaceId, queryPath)
	if err != nil || results == nil {
		return 1
	}
	return len(results) + 1
}

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

// ---------------------------------------------------------------------------
// Read helpers
// ---------------------------------------------------------------------------

// readViewResult reads content from a source view result, optionally
// compressing via the compression middleware if enabled.
func (s *SourceService) readViewResult(ctx context.Context, pctx *sources.ProviderContext, queryPath, filename string, offset, length int64) (*pb.SourceReadResponse, error) {
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

	// Compression intercept.
	strategy, session := s.compressionMeta(ctx)
	if strategy != "" {
		if s.compressor != nil {
			log.Debug().Str("strategy", strategy).Str("file", filename).Msg("compression: entering compressed read path")
			return s.readWithCompression(ctx, pctx, executor, query.Integration, queryPath, filename, resultID, query.QuerySpec, offset, length, strategy, session)
		}
		log.Warn().Str("strategy", strategy).Msg("compression: requested but compressor not initialized")
	}

	// Standard read (no compression).
	if content, err := s.fsStore.GetResultContent(ctx, pctx.WorkspaceId, queryPath, resultID); err == nil && len(content) > 0 {
		resp := readSlice(content, offset, length)
		resp.CostHint = s.passthroughCostHint(ctx, query.Integration, queryPath, resultID, filename, resp.Data)
		return resp, nil
	}

	content, err := executor.ReadResult(ctx, pctx, resultID)
	if err != nil {
		return &pb.SourceReadResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.fsStore.StoreResultContent(ctx, pctx.WorkspaceId, queryPath, resultID, content); err != nil {
		log.Warn().Err(err).Str("path", queryPath).Str("result", resultID).Msg("failed to cache result content")
	}
	resp := readSlice(content, offset, length)
	resp.CostHint = s.passthroughCostHint(ctx, query.Integration, queryPath, resultID, filename, resp.Data)
	return resp, nil
}

// compressionMeta extracts compression strategy and session from gRPC metadata.
func (s *SourceService) compressionMeta(ctx context.Context) (strategy, session string) {
	md, ok := grpcmd.FromIncomingContext(ctx)
	if !ok {
		return "", ""
	}
	if vals := md.Get("x-airstore-compression"); len(vals) > 0 {
		strategy = vals[0]
	}
	if vals := md.Get("x-airstore-session"); len(vals) > 0 {
		session = vals[0]
	}
	return strategy, session
}

func isFuseAccessOrigin(ctx context.Context) bool {
	md, ok := grpcmd.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	vals := md.Get("x-airstore-access-origin")
	return len(vals) > 0 && vals[0] == "fuse"
}

func (s *SourceService) getPassthroughCompressor() compression.ContextCompressor {
	s.passthroughOnce.Do(func() {
		cfg := s.compressionCfg
		if cfg.TokenEncoding == "" {
			cfg.TokenEncoding = compression.DefaultConfig().TokenEncoding
		}
		comp, err := compression.NewCompressor(compression.CompressionStrategyPassthrough, cfg)
		if err == nil {
			s.passthroughComp = comp
		}
	})
	return s.passthroughComp
}

func (s *SourceService) passthroughCostHint(
	ctx context.Context,
	integration, queryPath, resultID, filename string,
	content []byte,
) *pb.SourceReadCostHint {
	sourceURI := ""
	if integration != "" && resultID != "" {
		sourceURI = integration + "://" + resultID
	}
	hint := &pb.SourceReadCostHint{
		Integration:      integration,
		SourceUri:        sourceURI,
		QueryPath:        queryPath,
		ResultId:         resultID,
		Strategy:         string(compression.CompressionStrategyPassthrough),
		Outcome:          string(compression.OutcomePassthrough),
		OriginalBytes:    int64(len(content)),
		CompressedBytes:  int64(len(content)),
		OriginalTokens:   0,
		CompressedTokens: 0,
		CompressionMs:    0,
	}

	comp := s.getPassthroughCompressor()
	if comp == nil {
		return hint
	}

	res, err := comp.Compress(ctx, content, compression.ContentMeta{
		Integration: integration,
		QueryPath:   queryPath,
		ResultID:    resultID,
		Filename:    filename,
	})
	if err != nil || res == nil {
		return hint
	}
	hint.Strategy = string(res.Strategy)
	hint.Outcome = string(res.Outcome)
	hint.OriginalTokens = int64(res.OriginalTokens)
	hint.CompressedTokens = int64(res.CompressedTokens)
	hint.CompressionMs = res.DurationMs
	return hint
}

// findQueryAndFilename walks up the path to find the parent source view folder.
// Returns ("", "") if relPath is not inside a source view.
func (s *SourceService) findQueryAndFilename(ctx context.Context, workspaceId uint, integration, relPath string) (queryPath, filename string) {
	parts := strings.Split(relPath, "/")
	for i := len(parts) - 1; i >= 0; i-- {
		candidate := types.PathSources + "/" + integration
		if i > 0 {
			candidate += "/" + strings.Join(parts[:i], "/")
		}
		q, err := s.fsStore.GetQuery(ctx, workspaceId, candidate)
		if err == nil && q != nil && q.OutputFormat == types.ViewOutputFolder {
			return q.Path, strings.Join(parts[i:], "/")
		}
	}
	return "", ""
}

// ---------------------------------------------------------------------------
// Auth & credentials
// ---------------------------------------------------------------------------

func (s *SourceService) providerContext(ctx context.Context) (*sources.ProviderContext, error) {
	rc := auth.AuthInfoFromContext(ctx)
	if rc == nil {
		return &sources.ProviderContext{}, nil
	}
	return &sources.ProviderContext{
		WorkspaceId: auth.WorkspaceId(ctx),
		MemberId:    auth.MemberId(ctx),
	}, nil
}

func (s *SourceService) loadCredentials(ctx context.Context, pctx *sources.ProviderContext, integration string) (*sources.ProviderContext, bool) {
	// Built-in integrations (AuthNone) carry their own credentials (e.g. global
	// API key from config). No per-workspace DB connection needed.
	if meta, ok := types.GetIntegrationMeta(types.IntegrationName(integration)); ok && meta.AuthType == types.AuthNone {
		return pctx, true
	}

	if s.backend == nil || pctx.WorkspaceId == 0 {
		return pctx, false
	}

	cacheKey := fmt.Sprintf("%d:%s", pctx.WorkspaceId, integration)
	if cached, ok := s.credCache.Load(cacheKey); ok {
		c := cached.(*cachedCreds)
		if time.Now().Before(c.expiresAt) {
			pctx.Credentials = c.creds
			return pctx, true
		}
		s.credCache.Delete(cacheKey)
	}

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

	// Refresh OAuth token if needed.
	if s.oauthRegistry != nil && oauth.NeedsRefresh(creds) {
		if provider, err := s.oauthRegistry.GetProviderForIntegration(integration); err == nil {
			refreshed, err := provider.Refresh(ctx, creds.RefreshToken)
			if err != nil {
				log.Warn().Str("integration", integration).Str("provider", provider.Name()).Err(err).Msg("token refresh failed")
			} else {
				if _, err := s.backend.SaveConnection(ctx, conn.WorkspaceId, conn.MemberId, integration, refreshed, conn.Scope); err != nil {
					log.Warn().Str("integration", integration).Err(err).Msg("failed to persist refreshed token")
				}
				creds = refreshed
			}
		}
	}

	s.credCache.Store(cacheKey, &cachedCreds{creds: creds, expiresAt: time.Now().Add(credCacheTTL)})
	pctx.Credentials = creds
	return pctx, true
}

// connectedIntegrations returns the set of connected integration types for a
// workspace. Cached briefly. Returns nil when filtering is not possible.
func (s *SourceService) connectedIntegrations(ctx context.Context, workspaceId uint) map[string]bool {
	if s.backend == nil || workspaceId == 0 {
		return nil
	}

	if v, ok := s.connCache.Load(workspaceId); ok {
		cc := v.(*cachedConnSet)
		if time.Now().Before(cc.expiresAt) {
			return cc.set
		}
		s.connCache.Delete(workspaceId)
	}

	conns, err := s.backend.ListConnections(ctx, workspaceId)
	if err != nil {
		log.Warn().Err(err).Uint("workspace", workspaceId).Msg("failed to list connections for source filtering")
		return nil
	}

	set := make(map[string]bool, len(conns))
	for _, c := range conns {
		set[c.IntegrationType] = true
	}
	s.connCache.Store(workspaceId, &cachedConnSet{set: set, expiresAt: time.Now().Add(connCacheTTL)})
	return set
}

func (s *SourceService) InvalidateConnectionCache(workspaceId uint) {
	s.connCache.Delete(workspaceId)
}

func (s *SourceService) isIntegrationVisible(ctx context.Context, workspaceId uint, integration string) bool {
	// Built-in integrations (AuthNone) are always visible — no connection needed.
	if meta, ok := types.GetIntegrationMeta(types.IntegrationName(integration)); ok && meta.AuthType == types.AuthNone {
		return true
	}
	connSet := s.connectedIntegrations(ctx, workspaceId)
	return connSet == nil || connSet[integration]
}

// ---------------------------------------------------------------------------
// Direct source read by URI
// ---------------------------------------------------------------------------

// ReadBySourceURI fetches content directly from a provider using a source URI
// of the form "integration://resultID". This bypasses the source-view layer
// entirely, so it works even if the query results have changed since the
// original read was recorded.
func (s *SourceService) ReadBySourceURI(ctx context.Context, workspaceId uint, memberId uint, sourceURI string) ([]byte, error) {
	integration, resultID, err := ParseSourceURI(sourceURI)
	if err != nil {
		return nil, err
	}

	provider := s.registry.Get(integration)
	if provider == nil {
		return nil, fmt.Errorf("unknown integration: %s", integration)
	}

	executor, ok := provider.(sources.QueryExecutor)
	if !ok {
		return nil, fmt.Errorf("integration %s does not support direct reads", integration)
	}

	pctx := &sources.ProviderContext{
		WorkspaceId: workspaceId,
		MemberId:    memberId,
	}
	pctx, connected := s.loadCredentials(ctx, pctx, integration)
	if !connected || pctx.Credentials == nil {
		return nil, fmt.Errorf("no credentials for integration %s", integration)
	}

	return executor.ReadResult(ctx, pctx, resultID)
}

// ParseSourceURI splits "integration://resultID" into its parts.
func ParseSourceURI(uri string) (integration, resultID string, err error) {
	idx := strings.Index(uri, "://")
	if idx <= 0 || idx+3 >= len(uri) {
		return "", "", fmt.Errorf("invalid source_uri: %q", uri)
	}
	return uri[:idx], uri[idx+3:], nil
}

// ---------------------------------------------------------------------------
// Path & response helpers
// ---------------------------------------------------------------------------

func cleanPath(path string) string {
	return strings.Trim(path, "/")
}

func splitIntegrationPath(path string) (integration, relPath string) {
	parts := strings.SplitN(path, "/", 2)
	integration = parts[0]
	if len(parts) > 1 {
		relPath = parts[1]
	}
	return
}

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

func errorToCode(err error) int {
	switch err {
	case sources.ErrNotFound:
		return int(syscall.ENOENT)
	case sources.ErrNotConnected:
		return int(syscall.EACCES)
	case sources.ErrNotDir:
		return int(syscall.ENOTDIR)
	case sources.ErrIsDir:
		return int(syscall.EISDIR)
	default:
		return int(syscall.EIO)
	}
}
