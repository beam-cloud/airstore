// Package vnode provides virtual filesystem nodes for the FUSE layer.
//
// SourcesVNode handles /sources/{integration}/ paths as a view-based filesystem.
// Content is accessed ONLY through source views - native provider content
// (like messages/, labels/) is not exposed directly.
//
// Usage:
//
//	ls /sources/gmail/unread-emails/      <- executes view query, shows results
//	cat /sources/gmail/unread-emails/.query.as <- shows view definition
//	cat /sources/gmail/unread-emails/msg.txt <- reads materialized result
//
// Structure:
//
//	/sources/                            <- lists available integrations
//	/sources/gmail/                      <- lists user-created views only
//	/sources/gmail/unread-emails/        <- materialized view folder (read-only)
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

const queryMetaName = ".query.as"

// SourcesVNode handles /sources/ - both native content and source views.
// SourcesVNodeOption configures optional fields on a SourcesVNode.
type SourcesVNodeOption func(*SourcesVNode)

// WithCompression sets the compression strategy (e.g. "strip") forwarded to the
// gateway via the x-airstore-compression gRPC metadata header.
func WithCompression(strategy string) SourcesVNodeOption {
	return func(v *SourcesVNode) { v.compression = strategy }
}

type SourcesVNode struct {
	ReadOnlyBase
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
			v.trackRecentDir(path)
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

	// Directories cannot be opened for reading — return EISDIR per POSIX.
	if info, err := v.Getattr(path); err == nil && info.Mode&syscall.S_IFDIR != 0 {
		return 0, syscall.EISDIR
	}

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

// Mkdir is not supported for /sources in the mounted VFS.
// Source views are managed by gateway-side APIs, not filesystem writes.
func (v *SourcesVNode) Mkdir(path string, mode uint32) error {
	return ErrReadOnly
}

// Create is not supported for /sources in the mounted VFS.
// Source views are managed by gateway-side APIs, not filesystem writes.
func (v *SourcesVNode) Create(path string, flags int, mode uint32) (FileHandle, error) {
	return 0, ErrReadOnly
}

// Readlink reads symlink target.
// Note: Symlinks are not supported in the query-only model.
func (v *SourcesVNode) Readlink(path string) (string, error) {
	return "", fs.ErrNotExist
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
