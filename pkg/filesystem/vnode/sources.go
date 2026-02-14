// Package vnode provides virtual filesystem nodes for the FUSE layer.
//
// SourcesVNode handles /sources/{integration}/ paths as a view-based filesystem.
// Content is accessed ONLY through source views - native provider content
// (like messages/, labels/) is not exposed directly.
//
// Usage:
//
//	mkdir /sources/gmail/unread-emails    <- creates view via LLM inference
//	ls /sources/gmail/unread-emails/      <- executes view query, shows results
//	cat /sources/gmail/unread-emails/.query.as <- shows view definition
//	cat /sources/gmail/unread-emails/msg.txt <- reads materialized result
//
// Structure:
//
//	/sources/                            <- lists available integrations
//	/sources/gmail/                      <- lists user-created views only
//	/sources/gmail/unread-emails/        <- view folder (mkdir creates)
//	  .query.as                          <- view definition (JSON)
//	  2026-01-28_invoice_abc.txt         <- materialized search results
package vnode

import (
	"context"
	"encoding/json"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const sourcesTimeout = 30 * time.Second

// resultsCacheTTL is the hard upper bound on how long results are served from
// the in-memory cache. After this, the next ReadDir forces a fresh ExecuteView RPC.
const resultsCacheTTL = 45 * time.Second

// resultsCacheRefreshAge is the soft threshold after which a background refresh
// is triggered while still serving cached data (stale-while-revalidate pattern).
const resultsCacheRefreshAge = 30 * time.Second

// backgroundRefreshInterval is the tick interval for proactive cache warming.
const backgroundRefreshInterval = 15 * time.Second

// queryDefCacheTTL controls how long view definitions (name, query_spec,
// LastExecuted, etc.) are cached. Shorter than resultsCacheTTL so that the
// FUSE layer detects syncs faster: after a sync, the gateway updates
// LastExecuted in Postgres. Once the query-def cache expires (at most this
// duration), the FUSE layer re-fetches the definition, sees LastExecuted >
// cachedAt, and invalidates the stale results cache.
const queryDefCacheTTL = 15 * time.Second

const queryMetaName = ".query.as"

// cachedQueryResult holds cached query execution results
type cachedQueryResult struct {
	entries   []*pb.SourceDirEntry
	expiresAt time.Time
	cachedAt  time.Time // When this entry was cached (for refresh triggering)
}

// cachedQuery holds a cached query definition
type cachedQuery struct {
	query     *types.SourceView
	expiresAt time.Time
}

// cachedIntegration holds cached integration metadata
type cachedIntegration struct {
	mtime     int64
	expiresAt time.Time
}

// cachedStat holds cached stat metadata for a path
type cachedStat struct {
	info      *FileInfo
	expiresAt time.Time
}

// cachedContent holds open file content with a reference count.
type cachedContent struct {
	data     []byte
	hint     *pb.SourceReadCostHint
	cachedAt time.Time
	fetchMs  int64 // how long the content fetch took (ms)
	refs     int
}

// SourcesVNode handles /sources/ - both native content and source views.
// SourcesVNodeOption configures optional fields on a SourcesVNode.
type SourcesVNodeOption func(*SourcesVNode)

// WithCompression sets the compression strategy (e.g. "strip") forwarded to the
// gateway via the x-airstore-compression gRPC metadata header.
func WithCompression(strategy string) SourcesVNodeOption {
	return func(v *SourcesVNode) { v.compression = strategy }
}

type SourcesVNode struct {
	SourceViewBase
	client      pb.SourceServiceClient
	token       string
	bearerToken string // precomputed auth header value
	compression string // compression strategy to pass via gRPC metadata

	// Cache for query results to avoid repeated ExecuteView calls
	// during Readdir->Getattr cycles
	resultsMu sync.RWMutex
	results   map[string]*cachedQueryResult // path -> cached results

	// Cache for view definitions to avoid repeated GetView calls
	// during Readdir->Getattr cycles
	queriesMu sync.RWMutex
	queries   map[string]*cachedQuery // path -> cached query definition

	// Cache for integration list to avoid per-integration Stat RPCs
	integrationsMu sync.RWMutex
	integrations   map[string]*cachedIntegration // integration name -> cached mtime

	// Cache for stat metadata to avoid N+1 Getattr RPCs after Readdir
	statsMu sync.RWMutex
	stats   map[string]*cachedStat // full path -> cached stat

	// Open content cache for accurate sizing on open
	openMu      sync.RWMutex
	openContent map[string]*cachedContent // full path -> content
	openHandles map[FileHandle]string     // handle -> full path
	nextHandle  FileHandle

	// Recently accessed directories for background refresh
	recentDirsMu sync.RWMutex
	recentDirs   map[string]time.Time // directory path -> last access time

	// Background refresh control
	stopRefresh chan struct{}
}

// NewSourcesVNode creates a new SourcesVNode.
func NewSourcesVNode(conn *grpc.ClientConn, token string, opts ...SourcesVNodeOption) *SourcesVNode {
	v := &SourcesVNode{
		client:       pb.NewSourceServiceClient(conn),
		token:        token,
		bearerToken:  BearerToken(token),
		results:      make(map[string]*cachedQueryResult),
		queries:      make(map[string]*cachedQuery),
		integrations: make(map[string]*cachedIntegration),
		stats:        make(map[string]*cachedStat),
		openContent:  make(map[string]*cachedContent),
		openHandles:  make(map[FileHandle]string),
		nextHandle:   1,
		recentDirs:   make(map[string]time.Time),
		stopRefresh:  make(chan struct{}),
	}
	for _, opt := range opts {
		opt(v)
	}
	go v.backgroundRefreshLoop()
	return v
}

// Cleanup stops background goroutines. Called when filesystem is unmounted.
func (v *SourcesVNode) Cleanup() {
	close(v.stopRefresh)
}

func (v *SourcesVNode) Prefix() string { return SourcesPath }

func (v *SourcesVNode) ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), sourcesTimeout)
	if v.bearerToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", v.bearerToken)
	}
	if v.compression != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-airstore-compression", v.compression)
	}
	return ctx, cancel
}

func cloneCostHint(hint *pb.SourceReadCostHint) *pb.SourceReadCostHint {
	if hint == nil {
		return nil
	}
	cloned, ok := proto.Clone(hint).(*pb.SourceReadCostHint)
	if !ok {
		return nil
	}
	return cloned
}

// rel strips the /sources prefix
func (v *SourcesVNode) rel(path string) string {
	return strings.TrimPrefix(strings.TrimPrefix(path, SourcesPath), "/")
}

// parsePath splits "/sources/gmail/foo" into ("gmail", "foo")
func (v *SourcesVNode) parsePath(path string) (integration, subpath string) {
	rel := v.rel(path)
	if rel == "" {
		return "", ""
	}
	parts := strings.SplitN(rel, "/", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

// Getattr returns file/directory attributes.
func (v *SourcesVNode) Getattr(path string) (*FileInfo, error) {
	// Normalize path to match what Mkdir caches
	path = filepath.Clean(path)

	// /sources root
	if path == SourcesPath {
		return NewDirInfo(PathIno(path)), nil
	}

	integration, subpath := v.parsePath(path)

	// Early return for macOS system files (AppleDouble, .DS_Store, etc.)
	// This catches both integration-level (._gmail) and subpath-level (gmail/._foo)
	if isSystemFile(integration) {
		return nil, fs.ErrNotExist
	}
	if subpath != "" && isSystemFile(filepath.Base(subpath)) {
		return nil, fs.ErrNotExist
	}

	// Fast path: check stat cache first.
	// When compression is on, the stat cache may hold raw (uncompressed)
	// sizes from Readdir. Only trust it for directories or when we have
	// prefetched content with the accurate size.
	if info := v.getCachedStat(path); info != nil {
		isDir := info.Mode&syscall.S_IFDIR != 0
		_, _, hasContent := v.getOpenContent(path)
		if v.compression == "" || isDir || hasContent {
			v.applyOpenContentSize(path, info)
			return info, nil
		}
	}

	// /sources/{integration}
	if subpath == "" {
		// Fast path: check integration cache (populated by listIntegrations)
		if cached := v.getCachedIntegration(integration); cached != nil {
			info := NewDirInfo(PathIno(path))
			if cached.mtime > 0 {
				t := time.Unix(cached.mtime, 0)
				info.Atime, info.Mtime, info.Ctime = t, t, t
			}
			return info, nil
		}

		// Fallback: RPC to gateway
		ctx, cancel := v.ctx()
		defer cancel()
		resp, err := v.client.Stat(ctx, &pb.SourceStatRequest{Path: integration})
		if err != nil || !resp.Ok {
			return nil, fs.ErrNotExist
		}
		info := NewDirInfo(PathIno(path))
		if resp.Info != nil && resp.Info.Mtime > 0 {
			t := time.Unix(resp.Info.Mtime, 0)
			info.Atime, info.Mtime, info.Ctime = t, t, t
		}
		// Cache for future calls
		v.setCachedIntegration(integration, resp.Info.GetMtime())
		return info, nil
	}

	ctx, cancel := v.ctx()
	defer cancel()

	// README.md at integration root
	if subpath == types.SourceStatusFile {
		resp, err := v.client.Stat(ctx, &pb.SourceStatRequest{Path: integration + "/" + types.SourceStatusFile})
		if err != nil || resp == nil || !resp.Ok || resp.Info == nil {
			return nil, fs.ErrNotExist
		}
		info := v.protoToFileInfo(path, resp.Info)
		v.applyOpenContentSize(path, info)
		return info, nil
	}

	// Query metadata files (.query.as and .{name}.query.as)
	if data, mtime, isMeta, err := v.queryMetaContent(ctx, path); isMeta {
		if err != nil {
			return nil, fs.ErrNotExist
		}
		info := NewFileInfo(PathIno(path), int64(len(data)), 0444)
		info.Mtime = mtime
		info.Ctime = mtime
		v.applyOpenContentSize(path, info)
		return info, nil
	}

	// Is this inside a source view folder? (materialized result)
	//
	// File sizing strategy:
	//   1. openContent cache — already prefetched or open. Always accurate.
	//   2. readdir metadata   — fast path for ls -la (no-compression only).
	//   3. prefetch via RPC   — fetches content, caches for Open/Read reuse.
	//
	// With compression, step 2 is skipped because metadata sizes are raw
	// (uncompressed) and would cause null-byte padding from FUSE.
	parentPath := filepath.Dir(path)
	if q := v.getQuery(ctx, parentPath); q != nil && q.OutputFormat == types.ViewOutputFolder {
		// Tier 1: already open or pre-fetched content — always accurate.
		if data, _, ok := v.getOpenContent(path); ok {
			info := NewFileInfo(PathIno(path), int64(len(data)), 0644)
			_, mtime, _ := v.getQueryResultMetaCached(q.Path, filepath.Base(path))
			if mtime > 0 {
				t := time.Unix(mtime, 0)
				info.Mtime = t
				info.Ctime = t
			}
			return info, nil
		}

		filename := filepath.Base(path)

		// Fast path (no compression): use readdir cache size for ls -la.
		if v.compression == "" {
			size, mtime, ok := v.getQueryResultMetaCached(q.Path, filename)
			if ok && size > 0 {
				info := NewFileInfo(PathIno(path), size, 0644)
				if mtime > 0 {
					t := time.Unix(mtime, 0)
					info.Mtime = t
					info.Ctime = t
				}
				return info, nil
			}
		}

		// Slow path (prefetch): fetch actual content to determine the real size.
		// macOS FUSE uses the file size from Getattr to bound reads — a
		// wrong size causes null-byte padding or empty reads. By fetching
		// here and caching in openContent, we report the exact byte count
		// and avoid a redundant fetch in Open/Read.
		data, hint, err := v.fetchQueryResultContent(ctx, q, filename)
		if err != nil || len(data) == 0 {
			return nil, fs.ErrNotExist
		}
		// Cache for Open/Read to reuse (refs=0; retainOpenContent increments).
		v.prefetchContent(path, data, hint)
		info := NewFileInfo(PathIno(path), int64(len(data)), 0644)
		// Try to get mtime from the readdir cache.
		if _, cachedMtime, ok := v.getQueryResultMetaCached(q.Path, filename); ok && cachedMtime > 0 {
			t := time.Unix(cachedMtime, 0)
			info.Mtime = t
			info.Ctime = t
		}
		v.setCachedStat(path, info)
		return info, nil
	}

	// Is this path a source view?
	if q := v.getQuery(ctx, path); q != nil {
		if q.OutputFormat == types.ViewOutputFolder {
			info := NewDirInfo(PathIno(path))
			// Set Nlink based on cached results count for better UX
			// Standard Unix convention: Nlink = 2 + subdirectory count
			// For source view folders, we use result count to show child items
			if cached := v.getCachedResultsNoRefresh(q.Path); cached != nil {
				info.Nlink = uint32(2 + len(cached))
			}
			qt := viewMtime(q)
			info.Mtime = qt
			info.Ctime = qt
			return info, nil
		}
		info := NewFileInfo(PathIno(path), 0, 0644)
		qt := viewMtime(q)
		info.Mtime = qt
		info.Ctime = qt
		v.applyOpenContentSize(path, info)
		return info, nil
	}

	// No native content fallback - paths inside integrations must be queries
	return nil, fs.ErrNotExist
}

func viewMtime(q *types.SourceView) time.Time {
	if q == nil {
		return time.Now()
	}
	if !q.UpdatedAt.IsZero() {
		return q.UpdatedAt
	}
	if !q.CreatedAt.IsZero() {
		return q.CreatedAt
	}
	return time.Now()
}

// Readdir lists directory contents.
func (v *SourcesVNode) Readdir(path string) ([]DirEntry, error) {
	ctx, cancel := v.ctx()
	defer cancel()

	// /sources root - list available integrations
	if path == SourcesPath {
		return v.listIntegrations(ctx)
	}

	integration, subpath := v.parsePath(path)

	// Is this a source view folder? Execute it.
	if q := v.getQuery(ctx, path); q != nil {
		if q.OutputFormat == types.ViewOutputFolder {
			return v.executeQueryAsDir(ctx, q)
		}
	}

	// /sources/{integration} - list README.md + source views
	if subpath == "" {
		return v.listIntegration(ctx, path, integration)
	}

	// Paths inside an integration but not a query folder = not found
	return nil, fs.ErrNotExist
}

// listIntegration returns integration root entries: README.md + source views.
// Native provider content (messages/, labels/, etc.) is not exposed directly.
func (v *SourcesVNode) listIntegration(ctx context.Context, path, integration string) ([]DirEntry, error) {
	v.trackRecentDir(path) // Track for background refresh

	// Use gateway ReadDir so we include README.md and query entries consistently
	resp, err := v.client.ReadDir(ctx, &pb.SourceReadDirRequest{Path: integration})
	if err != nil || resp == nil || !resp.Ok {
		return nil, nil
	}

	entries := make([]DirEntry, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		childPath := path + "/" + e.Name
		ino := PathIno(childPath)
		entries = append(entries, DirEntry{
			Name:  e.Name,
			Mode:  e.Mode,
			Ino:   ino,
			Size:  e.Size,
			Mtime: e.Mtime,
		})

		// Cache stat metadata to avoid N+1 Getattr RPCs after this Readdir
		v.cacheStatFromEntry(childPath, e)
	}
	return entries, nil
}

// listIntegrations lists available integrations at the /sources root.
// This is the ONLY place we list native content - just the integration names.
func (v *SourcesVNode) listIntegrations(ctx context.Context) ([]DirEntry, error) {
	resp, err := v.client.ReadDir(ctx, &pb.SourceReadDirRequest{Path: ""})
	if err != nil || !resp.Ok {
		return nil, nil
	}

	entries := make([]DirEntry, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		// Only include directories (integrations like gmail, gdrive, etc.)
		if e.IsDir {
			childPath := SourcesPath + "/" + e.Name
			entries = append(entries, DirEntry{
				Name:  e.Name,
				Mode:  e.Mode,
				Ino:   PathIno(childPath),
				Mtime: e.Mtime,
			})

			// Cache integration metadata to avoid per-integration Stat RPCs
			v.setCachedIntegration(e.Name, e.Mtime)

			// Also cache as stat for Getattr
			v.cacheStatFromEntry(childPath, e)
		}
	}
	return entries, nil
}

// executeQueryAsDir executes a source view query and returns results as directory entries.
func (v *SourcesVNode) executeQueryAsDir(ctx context.Context, q *types.SourceView) ([]DirEntry, error) {
	// Always include the .query.as file
	queryMeta, _ := json.MarshalIndent(q, "", "  ")
	queryMtime := int64(viewMtime(q).Unix())
	entries := []DirEntry{{
		Name:  queryMetaName,
		Mode:  syscall.S_IFREG | 0444,
		Ino:   PathIno(q.Path + "/" + queryMetaName),
		Size:  int64(len(queryMeta)),
		Mtime: queryMtime,
	}}

	// Check cache first. If the view was synced (LastExecuted) after the cache was
	// populated, skip the cache so we pick up fresh results from the gateway.
	if cached := v.getCachedResultsIfFresh(q); cached != nil {
		for _, e := range cached {
			childPath := q.Path + "/" + e.Name
			entries = append(entries, DirEntry{
				Name:  e.Name,
				Mode:  e.Mode,
				Ino:   PathIno(childPath),
				Size:  listingSize(e.Size),
				Mtime: e.Mtime,
			})
			v.cacheStatFromEntry(childPath, e)
		}
		return entries, nil
	}

	// Execute via gateway RPC
	resp, err := v.client.ExecuteView(ctx, &pb.ExecuteViewRequest{Path: q.Path})
	if err != nil {
		log.Warn().Err(err).Str("path", q.Path).Msg("query execution failed")
		return entries, nil // Return just .query.as on failure
	}
	if !resp.Ok {
		log.Warn().Str("path", q.Path).Str("error", resp.Error).Msg("query execution returned not ok")
		return entries, nil
	}

	// Cache the results
	v.setCachedResults(q.Path, resp.Entries)

	for _, e := range resp.Entries {
		childPath := q.Path + "/" + e.Name
		entries = append(entries, DirEntry{
			Name:  e.Name,
			Mode:  e.Mode,
			Ino:   PathIno(childPath),
			Size:  listingSize(e.Size),
			Mtime: e.Mtime,
		})
		v.cacheStatFromEntry(childPath, e)
	}
	return entries, nil
}

func copyFromOffset(buf []byte, data []byte, off int64) int {
	if off >= int64(len(data)) {
		return 0
	}
	return copy(buf, data[off:])
}

func (v *SourcesVNode) readReadme(ctx context.Context, integration string, off int64, length int64) ([]byte, error) {
	resp, err := v.client.Read(ctx, &pb.SourceReadRequest{
		Path:   integration + "/" + types.SourceStatusFile,
		Offset: off,
		Length: length,
	})
	if err != nil || resp == nil || !resp.Ok {
		return nil, fs.ErrNotExist
	}
	return resp.Data, nil
}

func (v *SourcesVNode) queryMetaContent(ctx context.Context, path string) ([]byte, time.Time, bool, error) {
	base := filepath.Base(path)

	if base == queryMetaName {
		queryPath := filepath.Dir(path)
		q := v.getQuery(ctx, queryPath)
		if q == nil {
			return nil, time.Time{}, true, fs.ErrNotExist
		}
		data, _ := json.MarshalIndent(q, "", "  ")
		return data, viewMtime(q), true, nil
	}

	if strings.HasPrefix(base, ".") && strings.HasSuffix(base, queryMetaName) {
		queryFileName := strings.TrimPrefix(strings.TrimSuffix(base, queryMetaName), ".")
		queryPath := filepath.Join(filepath.Dir(path), queryFileName)
		q := v.getQuery(ctx, queryPath)
		if q == nil || q.OutputFormat != types.ViewOutputFile {
			return nil, time.Time{}, true, fs.ErrNotExist
		}
		data, _ := json.MarshalIndent(q, "", "  ")
		return data, viewMtime(q), true, nil
	}

	return nil, time.Time{}, false, nil
}

// Open opens a file.
func (v *SourcesVNode) Open(path string, flags int) (FileHandle, error) {
	path = filepath.Clean(path)

	// Reuse cached open content when possible
	if data, _, fh, ok := v.retainOpenContent(path); ok {
		v.cacheOpenStat(path, int64(len(data)), 0, time.Time{})
		return fh, nil
	}

	ctx, cancel := v.ctx()
	defer cancel()

	fetchStart := time.Now()
	data, hint, mode, mtime, ok, err := v.fetchContentForOpen(ctx, path)
	fetchMs := time.Since(fetchStart).Milliseconds()
	if !ok {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	fh := v.addOpenContentWithFetchMs(path, data, hint, fetchMs)
	v.cacheOpenStat(path, int64(len(data)), mode, mtime)
	return fh, nil
}

// Release closes a file handle.
func (v *SourcesVNode) Release(path string, fh FileHandle) error {
	v.releaseOpenContent(fh)
	return nil
}

// Read reads file data.
func (v *SourcesVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	n, _, err := v.ReadWithAttribution(path, buf, off, fh)
	return n, err
}

func (v *SourcesVNode) ReadWithAttribution(path string, buf []byte, off int64, fh FileHandle) (int, *ReadAttribution, error) {
	// Serve from open content cache when available
	if data, hint, fetchMs, ok := v.getOpenContentWithFetchMs(path); ok {
		attr := AttributionFromCostHint(CacheSourceOpenContent, hint)
		attr.FetchMs = fetchMs
		return copyFromOffset(buf, data, off), attr, nil
	}

	ctx, cancel := v.ctx()
	defer cancel()

	// README.md at integration root
	integration, subpath := v.parsePath(path)
	if integration != "" && subpath == types.SourceStatusFile {
		data, err := v.readReadme(ctx, integration, off, int64(len(buf)))
		if err != nil {
			return 0, nil, err
		}
		return copy(buf, data), AttributionForCache(CacheSourceSynthetic), nil
	}

	// Query metadata files (.query.as and .{name}.query.as)
	if data, _, isMeta, err := v.queryMetaContent(ctx, path); isMeta {
		if err != nil {
			return 0, nil, err
		}
		return copyFromOffset(buf, data, off), AttributionForCache(CacheSourceMetadata), nil
	}

	// Source view file (single-file mode)
	if q := v.getQuery(ctx, path); q != nil && q.OutputFormat == types.ViewOutputFile {
		return v.readQueryFileWithAttribution(ctx, q, buf, off)
	}

	// File inside source view folder (materialized result)
	parentPath := filepath.Dir(path)
	if q := v.getQuery(ctx, parentPath); q != nil && q.OutputFormat == types.ViewOutputFolder {
		return v.readQueryResultWithAttribution(ctx, q, filepath.Base(path), buf, off)
	}

	// No native content fallback - only view results are readable
	return 0, nil, fs.ErrNotExist
}

// readQueryFile reads a single-file source view result.
func (v *SourcesVNode) readQueryFile(ctx context.Context, q *types.SourceView, buf []byte, off int64) (int, error) {
	n, _, err := v.readQueryFileWithAttribution(ctx, q, buf, off)
	return n, err
}

func (v *SourcesVNode) readQueryFileWithAttribution(ctx context.Context, q *types.SourceView, buf []byte, off int64) (int, *ReadAttribution, error) {
	data, hint, err := v.fetchQueryFileContent(ctx, q)
	if err != nil {
		return 0, nil, fs.ErrNotExist
	}
	return copyFromOffset(buf, data, off), AttributionFromCostHint(CacheSourceBackendRPC, hint), nil
}

// readQueryResult reads a specific file from query results.
func (v *SourcesVNode) readQueryResult(ctx context.Context, q *types.SourceView, filename string, buf []byte, off int64) (int, error) {
	n, _, err := v.readQueryResultWithAttribution(ctx, q, filename, buf, off)
	return n, err
}

func (v *SourcesVNode) readQueryResultWithAttribution(ctx context.Context, q *types.SourceView, filename string, buf []byte, off int64) (int, *ReadAttribution, error) {
	data, hint, err := v.fetchQueryResultContent(ctx, q, filename)
	if err != nil {
		return 0, nil, fs.ErrNotExist
	}
	return copyFromOffset(buf, data, off), AttributionFromCostHint(CacheSourceBackendRPC, hint), nil
}

// fetchContentViaRead calls the Read RPC on the gateway. All content reads
// are routed through this method so the server-side access log interceptor
// can record them, and the compression middleware can intercept when active.
func (v *SourcesVNode) fetchContentViaRead(ctx context.Context, readPath string) ([]byte, *pb.SourceReadCostHint, error) {
	resp, err := v.client.Read(ctx, &pb.SourceReadRequest{Path: readPath})
	if err != nil || resp == nil || !resp.Ok || len(resp.Data) == 0 {
		return nil, nil, fs.ErrNotExist
	}
	return resp.Data, cloneCostHint(resp.CostHint), nil
}

func (v *SourcesVNode) fetchQueryFileContent(ctx context.Context, q *types.SourceView) ([]byte, *pb.SourceReadCostHint, error) {
	return v.fetchContentViaRead(ctx, strings.TrimPrefix(q.Path, SourcesPath+"/"))
}

func (v *SourcesVNode) fetchQueryResultContent(ctx context.Context, q *types.SourceView, filename string) ([]byte, *pb.SourceReadCostHint, error) {
	return v.fetchContentViaRead(ctx, strings.TrimPrefix(q.Path, SourcesPath+"/")+"/"+filename)
}

func (v *SourcesVNode) readmeOpenInfo(path string) (uint32, time.Time) {
	mode := uint32(syscall.S_IFREG | 0644)
	mtime := time.Now()
	if cached := v.getCachedStat(path); cached != nil {
		if cached.Mode != 0 {
			mode = cached.Mode
		}
		if !cached.Mtime.IsZero() {
			mtime = cached.Mtime
		}
	}
	return mode, mtime
}

func (v *SourcesVNode) queryResultCachedMeta(queryPath, filename string) (uint32, time.Time, bool) {
	cached := v.getCachedResultsNoRefresh(queryPath)
	if cached == nil {
		return 0, time.Time{}, false
	}
	for _, e := range cached {
		if e.Name == filename {
			mtime := time.Time{}
			if e.Mtime > 0 {
				mtime = time.Unix(e.Mtime, 0)
			}
			return e.Mode, mtime, true
		}
	}
	return 0, time.Time{}, false
}

func (v *SourcesVNode) getQueryResultMetaCached(queryPath, filename string) (size int64, mtime int64, ok bool) {
	cached := v.getCachedResultsNoRefresh(queryPath)
	if cached == nil {
		return 0, 0, false
	}
	for _, e := range cached {
		if e.Name == filename {
			size = e.Size
			// Keep size as 0 when unknown; kernel reads until EOF.
			// A non-zero placeholder causes null bytes or truncation.
			return size, e.Mtime, true
		}
	}
	return 0, 0, false
}

func (v *SourcesVNode) fetchContentForOpen(ctx context.Context, path string) ([]byte, *pb.SourceReadCostHint, uint32, time.Time, bool, error) {
	integration, subpath := v.parsePath(path)
	if integration == "" {
		return nil, nil, 0, time.Time{}, false, nil
	}

	// Ignore macOS system files
	if isSystemFile(integration) || (subpath != "" && isSystemFile(filepath.Base(subpath))) {
		return nil, nil, 0, time.Time{}, true, fs.ErrNotExist
	}

	// README.md at integration root
	if subpath == types.SourceStatusFile {
		data, err := v.readReadme(ctx, integration, 0, 0)
		if err != nil {
			return nil, nil, 0, time.Time{}, true, err
		}
		mode, mtime := v.readmeOpenInfo(path)
		return data, nil, mode, mtime, true, nil
	}

	// Query metadata files (.query.as and .{name}.query.as)
	if data, mtime, isMeta, err := v.queryMetaContent(ctx, path); isMeta {
		if err != nil {
			return nil, nil, 0, time.Time{}, true, err
		}
		return data, nil, syscall.S_IFREG | 0444, mtime, true, nil
	}

	// Source view file (single-file mode)
	if q := v.getQuery(ctx, path); q != nil && q.OutputFormat == types.ViewOutputFile {
		data, hint, err := v.fetchQueryFileContent(ctx, q)
		if err != nil {
			return nil, nil, 0, time.Time{}, true, err
		}
		return data, hint, syscall.S_IFREG | 0644, viewMtime(q), true, nil
	}

	// File inside source view folder (materialized result)
	parentPath := filepath.Dir(path)
	if q := v.getQuery(ctx, parentPath); q != nil && q.OutputFormat == types.ViewOutputFolder {
		filename := filepath.Base(path)
		data, hint, err := v.fetchQueryResultContent(ctx, q, filename)
		if err != nil {
			return nil, nil, 0, time.Time{}, true, err
		}
		mode := uint32(syscall.S_IFREG | 0644)
		mtime := time.Time{}
		if cachedMode, cachedMtime, ok := v.queryResultCachedMeta(q.Path, filename); ok {
			if cachedMode != 0 {
				mode = cachedMode
			}
			if !cachedMtime.IsZero() {
				mtime = cachedMtime
			}
		}
		if mtime.IsZero() {
			mtime = time.Now()
		}
		return data, hint, mode, mtime, true, nil
	}

	return nil, nil, 0, time.Time{}, false, nil
}

func (v *SourcesVNode) cacheOpenStat(path string, size int64, mode uint32, mtime time.Time) {
	if mode == 0 || mtime.IsZero() {
		if cached := v.getCachedStat(path); cached != nil {
			if mode == 0 {
				mode = cached.Mode
			}
			if mtime.IsZero() {
				mtime = cached.Mtime
			}
		}
	}
	if mode == 0 {
		mode = syscall.S_IFREG | 0644
	}

	info := NewFileInfo(PathIno(path), size, mode&0777)
	info.Mode = mode
	if !mtime.IsZero() {
		info.Mtime = mtime
		info.Ctime = mtime
	}
	v.setCachedStat(path, info)
}

// isSystemFile returns true if the filename is a system/metadata file that should be ignored.
// This includes macOS AppleDouble files (._*), .DS_Store, etc.
func isSystemFile(name string) bool {
	if strings.HasPrefix(name, "._") {
		return true // macOS AppleDouble extended attributes
	}
	if strings.HasPrefix(name, ".fuse_hidden") {
		return true // FUSE-T deferred deletes
	}
	switch name {
	case ".DS_Store", ".Spotlight-V100", ".Trashes", ".fseventsd", ".TemporaryItems":
		return true // macOS system files
	case ".git", ".gitignore", ".gitmodules", ".gitattributes", ".hg", ".svn":
		return true // VCS (never in a query-based filesystem)
	case ".rgignore", ".ignore", "libinfo.dylib",
		".eslintrc", ".eslintrc.json", ".eslintrc.js",
		".prettierrc", ".prettierrc.json",
		".editorconfig", ".clang-format", ".clang-tidy",
		".envrc", ".env", ".tool-versions", ".node-version", ".ruby-version", ".python-version", ".nvmrc":
		return true // Tool probes (never in a query-based filesystem)
	}
	return false
}

// Mkdir creates a source view folder.
func (v *SourcesVNode) Mkdir(path string, mode uint32) error {
	path = filepath.Clean(path)
	integration, subpath := v.parsePath(path)
	if integration == "" || subpath == "" || strings.Contains(subpath, "/") {
		log.Debug().Str("path", path).Str("integration", integration).Str("subpath", subpath).Msg("mkdir denied: invalid path")
		return syscall.EPERM
	}

	// Ignore macOS system files
	if isSystemFile(subpath) {
		log.Debug().Str("path", path).Str("subpath", subpath).Msg("mkdir ignored: system file")
		return syscall.EPERM
	}

	ctx, cancel := v.ctx()
	defer cancel()

	resp, err := v.client.CreateView(ctx, &pb.CreateViewRequest{
		Integration: integration, Name: subpath, OutputFormat: "folder",
	})
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("mkdir failed")
		return syscall.EIO
	}
	if !resp.Ok {
		log.Error().Str("error", resp.Error).Msg("mkdir failed")
		return syscall.EIO
	}

	// Cache the newly created query so subsequent Getattr calls can find it immediately
	v.setCachedQuery(path, protoToSourceView(resp.View))

	log.Info().Str("path", path).Str("query", resp.View.QuerySpec).Msg("created source view")
	return nil
}

// Create creates a source view file.
func (v *SourcesVNode) Create(path string, flags int, mode uint32) (FileHandle, error) {
	path = filepath.Clean(path)
	integration, subpath := v.parsePath(path)
	if integration == "" || subpath == "" || strings.Contains(subpath, "/") {
		return 0, syscall.EPERM
	}

	// Ignore macOS system files
	if isSystemFile(subpath) {
		return 0, syscall.EPERM
	}

	name := subpath
	ext := filepath.Ext(subpath)
	if ext != "" {
		name = strings.TrimSuffix(subpath, ext)
	}

	ctx, cancel := v.ctx()
	defer cancel()

	resp, err := v.client.CreateView(ctx, &pb.CreateViewRequest{
		Integration: integration, Name: name, OutputFormat: "file", FileExt: ext,
	})
	if err != nil || !resp.Ok {
		return 0, syscall.EIO
	}

	// Cache the newly created query so subsequent Getattr calls can find it immediately
	v.setCachedQuery(path, protoToSourceView(resp.View))

	log.Info().Str("path", path).Str("query", resp.View.QuerySpec).Msg("created source view file")
	return 0, nil
}

// Readlink reads symlink target.
// Note: Symlinks are not supported in the query-only model.
func (v *SourcesVNode) Readlink(path string) (string, error) {
	return "", fs.ErrNotExist
}

// getQuery retrieves a source view by path, returns nil if not found.
// Uses local cache to avoid repeated GetView RPCs.
func (v *SourcesVNode) getQuery(ctx context.Context, path string) *types.SourceView {
	// Check cache first
	if cached, found := v.getCachedQuery(path); found {
		return cached
	}

	resp, err := v.client.GetView(ctx, &pb.GetViewRequest{Path: path})
	if err != nil {
		return nil
	}

	if resp == nil || !resp.Ok || resp.View == nil {
		// Cache negative result too (path is not a query)
		v.setCachedQuery(path, nil)
		return nil
	}

	query := protoToSourceView(resp.View)
	v.setCachedQuery(path, query)
	return query
}

// protoToSourceView converts a proto SourceView to the internal SourceView type.
func protoToSourceView(v *pb.SourceView) *types.SourceView {
	if v == nil {
		return nil
	}
	q := &types.SourceView{
		ExternalId:   v.ExternalId,
		Integration:  v.Integration,
		Path:         v.Path,
		Name:         v.Name,
		QuerySpec:    v.QuerySpec,
		Guidance:     v.Guidance,
		OutputFormat: types.ViewOutputFormat(v.OutputFormat),
		FileExt:      v.FileExt,
		CacheTTL:     int(v.CacheTtl),
		CreatedAt:    time.Unix(v.CreatedAt, 0),
		UpdatedAt:    time.Unix(v.UpdatedAt, 0),
	}
	if v.LastExecuted != 0 {
		t := time.Unix(v.LastExecuted, 0)
		q.LastExecuted = &t
	}
	return q
}

// defaultUnknownFileSize is used when file size is unknown.
// Must be large enough for FUSE to read all content (diffs can be several MB).
const defaultUnknownFileSize = 10 * 1024 * 1024 // 10MB

// listingPlaceholderSize avoids per-entry Getattr calls during listings.
const listingPlaceholderSize = 4 * 1024 // 4KB

func listingSize(size int64) int64 {
	if size <= 0 {
		return listingPlaceholderSize
	}
	return size
}

func (v *SourcesVNode) getQueryResultMeta(ctx context.Context, queryPath, filename string) (size int64, mtime int64, ok bool) {
	// Check local cache first (populated by executeQueryAsDir during Readdir)
	if cached := v.getCachedResultsNoRefresh(queryPath); cached != nil {
		for _, e := range cached {
			if e.Name == filename {
				size = e.Size
				if size <= 0 {
					size = defaultUnknownFileSize
				}
				return size, e.Mtime, true
			}
		}
	}

	// Cache miss - execute query via RPC (should be cached on gateway side)
	resp, err := v.client.ExecuteView(ctx, &pb.ExecuteViewRequest{Path: queryPath})
	if err != nil {
		log.Warn().Err(err).Str("path", queryPath).Msg("getQueryResultMeta RPC failed")
		return 0, 0, false
	}
	if resp == nil || !resp.Ok {
		log.Warn().Str("path", queryPath).Str("error", resp.GetError()).Msg("getQueryResultMeta RPC returned not ok")
		return 0, 0, false
	}

	// Cache the results for future lookups
	v.setCachedResults(queryPath, resp.Entries)

	// Find matching entry
	for _, e := range resp.Entries {
		if e.Name == filename {
			size = e.Size
			if size <= 0 {
				size = defaultUnknownFileSize
			}
			return size, e.Mtime, true
		}
	}

	return 0, 0, false
}

func (v *SourcesVNode) protoToFileInfo(path string, info *pb.SourceFileInfo) *FileInfo {
	now := time.Now()
	mtime := now
	if info.Mtime > 0 {
		mtime = time.Unix(info.Mtime, 0)
	}
	uid, gid := GetOwner()
	return &FileInfo{
		Ino: PathIno(path), Size: info.Size, Mode: info.Mode, Nlink: 1,
		Uid: uid, Gid: gid,
		Atime: now, Mtime: mtime, Ctime: mtime,
	}
}

// getCachedResultsNoRefresh returns cached results without triggering refresh.
func (v *SourcesVNode) getCachedResultsNoRefresh(queryPath string) []*pb.SourceDirEntry {
	v.resultsMu.RLock()
	defer v.resultsMu.RUnlock()

	if cached, ok := v.results[queryPath]; ok && time.Now().Before(cached.expiresAt) {
		return cached.entries
	}
	return nil
}

// getCachedResultsIfFresh returns cached results only if they are still valid
// AND the view has not been synced since the cache was populated.
// If the view's LastExecuted is newer than the cache, the stale results and any
// associated openContent entries are evicted so the caller fetches fresh data.
func (v *SourcesVNode) getCachedResultsIfFresh(q *types.SourceView) []*pb.SourceDirEntry {
	v.resultsMu.RLock()
	cached, ok := v.results[q.Path]
	if !ok || !time.Now().Before(cached.expiresAt) {
		v.resultsMu.RUnlock()
		return nil
	}

	staleBySync := q.LastExecuted != nil && q.LastExecuted.After(cached.cachedAt)
	shouldRefresh := time.Since(cached.cachedAt) > resultsCacheRefreshAge
	entries := cached.entries
	v.resultsMu.RUnlock()

	if staleBySync {
		// Evict stale listing/content caches and force fresh ExecuteView.
		v.evictViewCaches(q.Path)
		return nil
	}
	if shouldRefresh {
		// Stale-while-revalidate.
		go v.triggerQueryRefresh(q.Path)
	}
	return entries
}

// evictViewCaches removes the results cache and all openContent entries that
// are children of the given view path. Called when a sync is detected.
func (v *SourcesVNode) evictViewCaches(viewPath string) {
	// Evict results listing
	v.resultsMu.Lock()
	delete(v.results, viewPath)
	v.resultsMu.Unlock()

	// Evict cached stat entries for children
	prefix := viewPath + "/"
	v.statsMu.Lock()
	for k := range v.stats {
		if strings.HasPrefix(k, prefix) {
			delete(v.stats, k)
		}
	}
	v.statsMu.Unlock()

	// Evict open content entries for children so file reads get fresh data
	v.openMu.Lock()
	for k := range v.openContent {
		if strings.HasPrefix(k, prefix) {
			delete(v.openContent, k)
		}
	}
	v.openMu.Unlock()

	log.Debug().Str("path", viewPath).Msg("evicted stale view caches after sync detection")
}

// triggerQueryRefresh refreshes the query results in the background
func (v *SourcesVNode) triggerQueryRefresh(queryPath string) {
	ctx, cancel := v.ctx()
	defer cancel()

	resp, err := v.client.ExecuteView(ctx, &pb.ExecuteViewRequest{Path: queryPath})
	if err != nil || !resp.Ok {
		return
	}

	v.setCachedResults(queryPath, resp.Entries)
}

// setCachedResults stores query results in the cache
func (v *SourcesVNode) setCachedResults(queryPath string, entries []*pb.SourceDirEntry) {
	v.resultsMu.Lock()
	defer v.resultsMu.Unlock()

	now := time.Now()
	v.results[queryPath] = &cachedQueryResult{
		entries:   entries,
		expiresAt: now.Add(resultsCacheTTL),
		cachedAt:  now,
	}
}

// getCachedQuery retrieves cached query definition if still valid
// Returns (query, true) if found in cache, (nil, false) if not found
func (v *SourcesVNode) getCachedQuery(path string) (*types.SourceView, bool) {
	v.queriesMu.RLock()
	defer v.queriesMu.RUnlock()

	if cached, ok := v.queries[path]; ok && time.Now().Before(cached.expiresAt) {
		return cached.query, true // query may be nil (negative cache)
	}
	return nil, false
}

// setCachedQuery stores view definition in the cache (nil for negative cache).
func (v *SourcesVNode) setCachedQuery(path string, query *types.SourceView) {
	v.queriesMu.Lock()
	defer v.queriesMu.Unlock()

	v.queries[path] = &cachedQuery{
		query:     query,
		expiresAt: time.Now().Add(queryDefCacheTTL),
	}
}

// Integration cache helpers

func (v *SourcesVNode) getCachedIntegration(name string) *cachedIntegration {
	v.integrationsMu.RLock()
	defer v.integrationsMu.RUnlock()

	if cached, ok := v.integrations[name]; ok && time.Now().Before(cached.expiresAt) {
		return cached
	}
	return nil
}

func (v *SourcesVNode) setCachedIntegration(name string, mtime int64) {
	v.integrationsMu.Lock()
	defer v.integrationsMu.Unlock()

	v.integrations[name] = &cachedIntegration{
		mtime:     mtime,
		expiresAt: time.Now().Add(resultsCacheTTL),
	}
}

// Stat cache helpers

func (v *SourcesVNode) getCachedStat(path string) *FileInfo {
	v.statsMu.RLock()
	defer v.statsMu.RUnlock()

	if cached, ok := v.stats[path]; ok && time.Now().Before(cached.expiresAt) && cached.info != nil {
		// Return a copy to avoid mutation
		info := *cached.info
		return &info
	}
	return nil
}

func (v *SourcesVNode) setCachedStat(path string, info *FileInfo) {
	v.statsMu.Lock()
	defer v.statsMu.Unlock()

	v.stats[path] = &cachedStat{
		info:      info,
		expiresAt: time.Now().Add(resultsCacheTTL),
	}
}

// Open content cache helpers

func (v *SourcesVNode) getOpenContent(path string) ([]byte, *pb.SourceReadCostHint, bool) {
	v.openMu.RLock()
	defer v.openMu.RUnlock()

	if cached, ok := v.openContent[path]; ok {
		return cached.data, cloneCostHint(cached.hint), true
	}
	return nil, nil, false
}

func (v *SourcesVNode) getOpenContentWithFetchMs(path string) ([]byte, *pb.SourceReadCostHint, int64, bool) {
	v.openMu.RLock()
	defer v.openMu.RUnlock()

	if cached, ok := v.openContent[path]; ok {
		return cached.data, cloneCostHint(cached.hint), cached.fetchMs, true
	}
	return nil, nil, 0, false
}

func (v *SourcesVNode) addOpenContent(path string, data []byte, hint *pb.SourceReadCostHint) FileHandle {
	return v.addOpenContentWithFetchMs(path, data, hint, 0)
}

func (v *SourcesVNode) addOpenContentWithFetchMs(path string, data []byte, hint *pb.SourceReadCostHint, fetchMs int64) FileHandle {
	v.openMu.Lock()
	defer v.openMu.Unlock()

	fh := v.nextHandle
	v.nextHandle++
	v.openHandles[fh] = path

	if cached, ok := v.openContent[path]; ok {
		cached.refs++
		if data != nil {
			cached.data = data
			cached.hint = cloneCostHint(hint)
			cached.cachedAt = time.Now()
			cached.fetchMs = fetchMs
		}
		return fh
	}

	v.openContent[path] = &cachedContent{
		data:     data,
		hint:     cloneCostHint(hint),
		cachedAt: time.Now(),
		fetchMs:  fetchMs,
		refs:     1,
	}
	return fh
}

func (v *SourcesVNode) retainOpenContent(path string) ([]byte, *pb.SourceReadCostHint, FileHandle, bool) {
	v.openMu.Lock()
	defer v.openMu.Unlock()

	cached, ok := v.openContent[path]
	if !ok {
		return nil, nil, 0, false
	}

	// Don't reuse stale prefetched entries; let Open fetch fresh content.
	if cached.refs == 0 && time.Since(cached.cachedAt) > prefetchTTL {
		delete(v.openContent, path)
		return nil, nil, 0, false
	}

	fh := v.nextHandle
	v.nextHandle++
	v.openHandles[fh] = path
	cached.refs++
	return cached.data, cloneCostHint(cached.hint), fh, true
}

func (v *SourcesVNode) releaseOpenContent(fh FileHandle) {
	if fh == 0 {
		return
	}

	v.openMu.Lock()
	defer v.openMu.Unlock()

	path, ok := v.openHandles[fh]
	if !ok {
		return
	}
	delete(v.openHandles, fh)

	if cached, ok := v.openContent[path]; ok {
		if cached.refs > 1 {
			cached.refs--
			return
		}
		delete(v.openContent, path)
	}
}

// prefetchTTL is how long a prefetched entry (refs=0) survives without being
// opened. Prevents unbounded accumulation from directory listings.
const prefetchTTL = 30 * time.Second

// prefetchContent stores content in the open content cache without creating a
// file handle. Used by Getattr to pre-fetch content for accurate size reporting.
// Open's retainOpenContent will find and reuse this cached content.
//
// Stale prefetched entries (refs=0, older than prefetchTTL) are lazily evicted
// on each call to avoid unbounded memory growth from directory listings.
func (v *SourcesVNode) prefetchContent(path string, data []byte, hint *pb.SourceReadCostHint) {
	v.openMu.Lock()
	defer v.openMu.Unlock()

	// Lazy eviction: remove stale prefetched entries that were never opened.
	now := time.Now()
	for p, c := range v.openContent {
		if c.refs == 0 && now.Sub(c.cachedAt) > prefetchTTL {
			delete(v.openContent, p)
		}
	}

	if _, ok := v.openContent[path]; !ok {
		v.openContent[path] = &cachedContent{
			data:     data,
			hint:     cloneCostHint(hint),
			cachedAt: now,
			refs:     0, // No active Open; retainOpenContent increments to 1
		}
	}
}

func (v *SourcesVNode) applyOpenContentSize(path string, info *FileInfo) {
	if info == nil {
		return
	}
	if data, _, ok := v.getOpenContent(path); ok {
		info.Size = int64(len(data))
	}
}

// cacheStatFromEntry creates and caches a FileInfo from a SourceDirEntry
func (v *SourcesVNode) cacheStatFromEntry(path string, e *pb.SourceDirEntry) {
	if e == nil {
		return
	}

	ino := PathIno(path)
	var info *FileInfo
	if e.IsDir {
		info = NewDirInfo(ino)
	} else {
		info = NewFileInfo(ino, e.Size, e.Mode&0777)
	}
	info.Mode = e.Mode
	info.Size = e.Size

	if e.Mtime > 0 {
		t := time.Unix(e.Mtime, 0)
		info.Atime, info.Mtime, info.Ctime = t, t, t
	}

	v.setCachedStat(path, info)
}

// Background refresh

// trackRecentDir records a directory as recently accessed for background refresh
func (v *SourcesVNode) trackRecentDir(path string) {
	v.recentDirsMu.Lock()
	v.recentDirs[path] = time.Now()
	v.recentDirsMu.Unlock()
}

// getRecentDirs returns directories accessed in the last 2 minutes
func (v *SourcesVNode) getRecentDirs() []string {
	v.recentDirsMu.RLock()
	defer v.recentDirsMu.RUnlock()

	cutoff := time.Now().Add(-2 * time.Minute)
	dirs := make([]string, 0, len(v.recentDirs))
	for path, accessed := range v.recentDirs {
		if accessed.After(cutoff) {
			dirs = append(dirs, path)
		}
	}
	return dirs
}

// cleanupOldRecentDirs removes directories not accessed recently
func (v *SourcesVNode) cleanupOldRecentDirs() {
	v.recentDirsMu.Lock()
	defer v.recentDirsMu.Unlock()

	cutoff := time.Now().Add(-5 * time.Minute)
	for path, accessed := range v.recentDirs {
		if accessed.Before(cutoff) {
			delete(v.recentDirs, path)
		}
	}
}

// backgroundRefreshLoop periodically refreshes caches for frequently accessed paths
func (v *SourcesVNode) backgroundRefreshLoop() {
	ticker := time.NewTicker(backgroundRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-v.stopRefresh:
			return
		case <-ticker.C:
			v.doBackgroundRefresh()
		}
	}
}

// doBackgroundRefresh refreshes integration list and recently accessed directories
func (v *SourcesVNode) doBackgroundRefresh() {
	ctx, cancel := v.ctx()
	defer cancel()

	// Always refresh integration list (cheap, high value)
	v.refreshIntegrations(ctx)

	// Refresh recently accessed directories
	for _, path := range v.getRecentDirs() {
		integration, subpath := v.parsePath(path)
		if integration != "" && subpath == "" {
			// This is an integration root like /sources/gmail
			v.refreshIntegrationDir(ctx, path, integration)
		}
	}

	// Cleanup old tracking data
	v.cleanupOldRecentDirs()
}

// refreshIntegrations refreshes the integration list cache
func (v *SourcesVNode) refreshIntegrations(ctx context.Context) {
	resp, err := v.client.ReadDir(ctx, &pb.SourceReadDirRequest{Path: ""})
	if err != nil || !resp.Ok {
		return
	}

	for _, e := range resp.Entries {
		if e.IsDir {
			childPath := SourcesPath + "/" + e.Name
			v.setCachedIntegration(e.Name, e.Mtime)
			v.cacheStatFromEntry(childPath, e)
		}
	}
}

// refreshIntegrationDir refreshes the cache for an integration directory
func (v *SourcesVNode) refreshIntegrationDir(ctx context.Context, path, integration string) {
	resp, err := v.client.ReadDir(ctx, &pb.SourceReadDirRequest{Path: integration})
	if err != nil || resp == nil || !resp.Ok {
		return
	}

	for _, e := range resp.Entries {
		childPath := path + "/" + e.Name
		v.cacheStatFromEntry(childPath, e)
	}
}
