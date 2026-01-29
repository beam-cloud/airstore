package services

import (
	"context"
	"strings"
	"syscall"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/index"
	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/sources"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
)

// SourceService implements the gRPC SourceService for read-only integration access.
type SourceService struct {
	pb.UnimplementedSourceServiceServer
	registry     *sources.Registry
	backend      repository.BackendRepository
	cache        *sources.SourceCache
	rateLimiter  *sources.RateLimiter
	googleOAuth  *oauth.GoogleClient
	indexStore   index.IndexStore // Index store for fast reads (optional)
	indexEnabled bool
}

// NewSourceService creates a new SourceService
func NewSourceService(registry *sources.Registry, backend repository.BackendRepository) *SourceService {
	return &SourceService{
		registry:    registry,
		backend:     backend,
		cache:       sources.NewSourceCache(sources.DefaultCacheTTL, sources.DefaultCacheSize),
		rateLimiter: sources.NewRateLimiter(sources.DefaultRateLimitConfig()),
	}
}

// NewSourceServiceWithOAuth creates a SourceService with OAuth refresh support
func NewSourceServiceWithOAuth(registry *sources.Registry, backend repository.BackendRepository, googleOAuth *oauth.GoogleClient) *SourceService {
	return &SourceService{
		registry:    registry,
		backend:     backend,
		cache:       sources.NewSourceCache(sources.DefaultCacheTTL, sources.DefaultCacheSize),
		rateLimiter: sources.NewRateLimiter(sources.DefaultRateLimitConfig()),
		googleOAuth: googleOAuth,
	}
}

// SetIndexStore configures the index store for fast reads
func (s *SourceService) SetIndexStore(store index.IndexStore) {
	s.indexStore = store
	s.indexEnabled = store != nil
	if s.indexEnabled {
		log.Info().Msg("index store enabled for SourceService - reads will use local index")
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
		log.Debug().Str("integration", integration).Bool("connected", connected).Str("scope", scope).Msg("status.json stat")
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

	// Try index first if enabled
	if s.indexEnabled && s.indexStore != nil {
		entry, err := s.indexStore.GetByPath(ctx, relPath)
		if err == nil && entry != nil {
			log.Debug().Str("path", relPath).Msg("index hit - stat from local index")
			mode := uint32(sources.ModeFile)
			isDir := entry.IsDir()
			if isDir {
				mode = uint32(sources.ModeDir)
			}
			return &pb.SourceStatResponse{
				Ok: true,
				Info: &pb.SourceFileInfo{
					Size:  entry.Size,
					Mode:  mode,
					Mtime: entry.ModTime.Unix(),
					IsDir: isDir,
				},
			}, nil
		}
		// Also check if this is a virtual directory by looking for children
		if err == nil && entry == nil {
			entries, err := s.indexStore.List(ctx, integration, relPath)
			if err == nil && len(entries) > 0 {
				log.Debug().Str("path", relPath).Int("children", len(entries)).Msg("index hit - virtual dir from local index")
				return &pb.SourceStatResponse{
					Ok: true,
					Info: &pb.SourceFileInfo{
						Mode:  uint32(sources.ModeDir),
						IsDir: true,
						Mtime: sources.NowUnix(),
					},
				}, nil
			}
		}
		log.Debug().Str("path", relPath).Msg("index miss - falling back to provider for stat")
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

	// Root /sources directory - list all integrations (no caching needed)
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

	// Handle root of integration (e.g., /sources/github)
	if relPath == "" {
		// Generate status.json to get its size
		scope := ""
		if connected && pctx.Credentials != nil {
			scope = "shared"
		}
		statusData := sources.GenerateStatusJSON(integration, connected, scope, "")

		// Always show status.json with correct size
		entries := []*pb.SourceDirEntry{
			{Name: "status.json", Mode: sources.ModeFile, Size: int64(len(statusData)), Mtime: sources.NowUnix()},
		}

		// If connected, also list the provider's root (with caching)
		if connected {
			cacheKey := sources.CacheKey(pctx.WorkspaceId, integration, "", "readdir")
			if cachedEntries, ok := s.cache.GetEntries(cacheKey); ok {
				for _, e := range cachedEntries {
					entries = append(entries, &pb.SourceDirEntry{
						Name:  e.Name,
						Mode:  e.Mode,
						IsDir: e.IsDir,
						Size:  e.Size,
						Mtime: e.Mtime,
					})
				}
			} else {
				// Rate limit and fetch
				if err := s.rateLimiter.Wait(ctx, pctx.WorkspaceId, integration); err != nil {
					// Return just status.json on rate limit
					return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
				}

				result, err := s.cache.DoOnce(cacheKey, func() (any, error) {
					return provider.ReadDir(ctx, pctx, "")
				})
				if err == nil {
					providerEntries := result.([]sources.DirEntry)
					s.cache.SetEntries(cacheKey, providerEntries)
					for _, e := range providerEntries {
						entries = append(entries, &pb.SourceDirEntry{
							Name:  e.Name,
							Mode:  e.Mode,
							IsDir: e.IsDir,
							Size:  e.Size,
							Mtime: e.Mtime,
						})
					}
				}
			}
		}

		return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
	}

	// Try index first if enabled
	if s.indexEnabled && s.indexStore != nil {
		indexEntries, err := s.indexStore.List(ctx, integration, relPath)
		if err == nil && len(indexEntries) > 0 {
			log.Debug().Str("path", relPath).Int("count", len(indexEntries)).Msg("index hit - readdir from local index")

			// Deduplicate entries (index may have multiple entries for same name)
			seen := make(map[string]bool)
			entries := make([]*pb.SourceDirEntry, 0, len(indexEntries))

			for _, e := range indexEntries {
				if seen[e.Name] {
					continue
				}
				seen[e.Name] = true

				mode := uint32(sources.ModeFile)
				isDir := e.IsDir()
				if isDir {
					mode = uint32(sources.ModeDir)
				}

				entries = append(entries, &pb.SourceDirEntry{
					Name:  e.Name,
					Mode:  mode,
					IsDir: isDir,
					Size:  e.Size,
					Mtime: e.ModTime.Unix(),
				})
			}

			return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
		}
		log.Debug().Str("path", relPath).Msg("index miss - falling back to provider for readdir")
	}

	// Check cache first
	cacheKey := sources.CacheKey(pctx.WorkspaceId, integration, relPath, "readdir")
	if cachedEntries, ok := s.cache.GetEntries(cacheKey); ok {
		entries := make([]*pb.SourceDirEntry, len(cachedEntries))
		for i, e := range cachedEntries {
			entries[i] = &pb.SourceDirEntry{
				Name:  e.Name,
				Mode:  e.Mode,
				IsDir: e.IsDir,
				Size:  e.Size,
				Mtime: e.Mtime,
			}
		}
		return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
	}

	// Rate limit check
	if err := s.rateLimiter.Wait(ctx, pctx.WorkspaceId, integration); err != nil {
		return &pb.SourceReadDirResponse{Ok: false, Error: "rate limited"}, nil
	}

	// Use singleflight to coalesce concurrent requests
	result, err := s.cache.DoOnce(cacheKey, func() (any, error) {
		return provider.ReadDir(ctx, pctx, relPath)
	})
	if err != nil {
		return &pb.SourceReadDirResponse{Ok: false, Error: err.Error()}, nil
	}

	dirEntries := result.([]sources.DirEntry)

	// Cache the result
	s.cache.SetEntries(cacheKey, dirEntries)

	entries := make([]*pb.SourceDirEntry, len(dirEntries))
	for i, e := range dirEntries {
		entries[i] = &pb.SourceDirEntry{
			Name:  e.Name,
			Mode:  e.Mode,
			IsDir: e.IsDir,
			Size:  e.Size,
			Mtime: e.Mtime,
		}
	}

	return &pb.SourceReadDirResponse{Ok: true, Entries: entries}, nil
}

// Read reads file content
func (s *SourceService) Read(ctx context.Context, req *pb.SourceReadRequest) (*pb.SourceReadResponse, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.SourceReadResponse{Ok: false, Error: err.Error()}, nil
	}

	path := cleanPath(req.Path)

	if path == "" {
		return &pb.SourceReadResponse{Ok: false, Error: "is a directory"}, nil
	}

	// Parse integration name from path
	integration, relPath := splitIntegrationPath(path)

	provider := s.registry.Get(integration)
	if provider == nil {
		return &pb.SourceReadResponse{Ok: false, Error: "integration not found"}, nil
	}

	// Get credentials for this integration
	pctx, connected := s.loadCredentials(ctx, pctx, integration)

	// Handle status.json specially (no caching needed, cheap to generate)
	if relPath == "status.json" {
		workspaceId := ""
		scope := ""
		if connected && pctx.Credentials != nil {
			scope = "shared"
		}
		data := sources.GenerateStatusJSON(integration, connected, scope, workspaceId)
		return readSlice(data, req.Offset, req.Length), nil
	}

	if relPath == "" {
		return &pb.SourceReadResponse{Ok: false, Error: "is a directory"}, nil
	}

	// Try index first if enabled - this provides instant reads for grep/find
	if s.indexEnabled && s.indexStore != nil {
		entry, err := s.indexStore.GetByPath(ctx, relPath)
		if err == nil && entry != nil && entry.Body != "" {
			log.Debug().Str("path", relPath).Msg("index hit - serving from local index")
			data := []byte(entry.Body)

			// Handle offset/length
			if req.Offset >= int64(len(data)) {
				return &pb.SourceReadResponse{Ok: true, Data: nil}, nil
			}

			end := int64(len(data))
			if req.Length > 0 && req.Offset+req.Length < end {
				end = req.Offset + req.Length
			}

			return &pb.SourceReadResponse{Ok: true, Data: data[req.Offset:end]}, nil
		}
		// Index miss - fall through to provider
		if err == nil && entry == nil {
			log.Debug().Str("path", relPath).Msg("index miss - falling back to provider")
		}
	}

	// For reads at offset 0 with no length (full file), try cache first
	// Skip caching for partial reads to avoid complexity
	useCache := req.Offset == 0 && req.Length == 0
	var cacheKey string

	if useCache {
		cacheKey = sources.CacheKey(pctx.WorkspaceId, integration, relPath, "read")
		if cachedData, ok := s.cache.GetData(cacheKey); ok {
			return &pb.SourceReadResponse{Ok: true, Data: cachedData}, nil
		}
	}

	// Rate limit check
	if err := s.rateLimiter.Wait(ctx, pctx.WorkspaceId, integration); err != nil {
		return &pb.SourceReadResponse{Ok: false, Error: "rate limited"}, nil
	}

	// Use singleflight for full file reads only
	var data []byte
	if useCache {
		result, err := s.cache.DoOnce(cacheKey, func() (any, error) {
			return provider.Read(ctx, pctx, relPath, 0, 0)
		})
		if err != nil {
			return &pb.SourceReadResponse{Ok: false, Error: err.Error()}, nil
		}
		data = result.([]byte)
		s.cache.SetData(cacheKey, data)
	} else {
		// Direct read for partial requests
		var err error
		data, err = provider.Read(ctx, pctx, relPath, req.Offset, req.Length)
		if err != nil {
			return &pb.SourceReadResponse{Ok: false, Error: err.Error()}, nil
		}
	}

	return &pb.SourceReadResponse{Ok: true, Data: data}, nil
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
		log.Debug().Msg("providerContext: no auth context (unauthenticated)")
		return &sources.ProviderContext{}, nil
	}

	log.Debug().Uint("workspace_id", rc.WorkspaceId).Uint("member_id", rc.MemberId).Msg("providerContext: authenticated request")
	return &sources.ProviderContext{
		WorkspaceId: rc.WorkspaceId,
		MemberId:    rc.MemberId,
	}, nil
}

// loadCredentials fetches integration credentials from the backend
// and refreshes Google OAuth tokens if expired
func (s *SourceService) loadCredentials(ctx context.Context, pctx *sources.ProviderContext, integration string) (*sources.ProviderContext, bool) {
	if s.backend == nil {
		log.Debug().Str("integration", integration).Msg("loadCredentials: no backend configured")
		return pctx, false
	}
	if pctx.WorkspaceId == 0 {
		log.Debug().Str("integration", integration).Msg("loadCredentials: no workspace ID in context (unauthenticated request)")
		return pctx, false
	}

	log.Debug().Str("integration", integration).Uint("workspace_id", pctx.WorkspaceId).Msg("loadCredentials: looking up connection")

	conn, err := s.backend.GetConnection(ctx, pctx.WorkspaceId, pctx.MemberId, integration)
	if err != nil {
		log.Warn().Str("integration", integration).Err(err).Msg("connection lookup failed")
		return pctx, false
	}
	if conn == nil {
		log.Debug().Str("integration", integration).Uint("workspace_id", pctx.WorkspaceId).Msg("loadCredentials: no connection found")
		return pctx, false
	}

	log.Debug().Str("integration", integration).Str("connection_id", conn.ExternalId).Msg("loadCredentials: connection found")

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
			} else {
				log.Debug().Str("integration", integration).Msg("token refreshed successfully")
			}
			creds = refreshed
		}
	}

	pctx.Credentials = creds
	return pctx, true
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

