// Package vnode provides virtual filesystem nodes for the FUSE layer.
//
// SourcesVNode handles /sources/{integration}/ paths as a query-based filesystem.
// Content is accessed ONLY through filesystem queries - native provider content
// (like messages/, labels/) is not exposed directly.
//
// Usage:
//
//	mkdir /sources/gmail/unread-emails    <- creates query via LLM inference
//	ls /sources/gmail/unread-emails/      <- executes query, shows results
//	cat /sources/gmail/unread-emails/.query.as <- shows query definition
//	cat /sources/gmail/unread-emails/msg.txt <- reads materialized result
//
// Structure:
//
//	/sources/                            <- lists available integrations
//	/sources/gmail/                      <- lists user-created queries only
//	/sources/gmail/unread-emails/        <- query folder (mkdir creates)
//	  .query.as                          <- query definition (JSON)
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
)

const sourcesTimeout = 30 * time.Second
const resultsCacheTTL = 45 * time.Second           // Cache query results to avoid repeated API calls (max staleness)
const resultsCacheRefreshAge = 30 * time.Second    // Trigger background refresh when cache older than this
const backgroundRefreshInterval = 15 * time.Second // Background cache refresh interval
const queryMetaName = ".query.as"

// cachedQueryResult holds cached query execution results
type cachedQueryResult struct {
	entries   []*pb.SourceDirEntry
	expiresAt time.Time
	cachedAt  time.Time // When this entry was cached (for refresh triggering)
}

// cachedQuery holds a cached query definition
type cachedQuery struct {
	query     *types.SmartQuery
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
	cachedAt time.Time
	refs     int
}

// SourcesVNode handles /sources/ - both native content and smart queries.
type SourcesVNode struct {
	SmartQueryBase
	client pb.SourceServiceClient
	token  string

	// Cache for query results to avoid repeated ExecuteSmartQuery calls
	// during Readdir->Getattr cycles
	resultsMu sync.RWMutex
	results   map[string]*cachedQueryResult // path -> cached results

	// Cache for query definitions to avoid repeated GetSmartQuery calls
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
func NewSourcesVNode(conn *grpc.ClientConn, token string) *SourcesVNode {
	v := &SourcesVNode{
		client:       pb.NewSourceServiceClient(conn),
		token:        token,
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
	if v.token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+v.token)
	}
	return ctx, cancel
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

	// Fast path: check stat cache first
	if info := v.getCachedStat(path); info != nil {
		v.applyOpenContentSize(path, info)
		return info, nil
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

	// Is this inside a smart query folder? (materialized result)
	parentPath := filepath.Dir(path)
	if q := v.getQuery(ctx, parentPath); q != nil && q.OutputFormat == types.SmartQueryOutputFolder {
		filename := filepath.Base(path)
		size, mtime, ok := v.getQueryResultMetaCached(q.Path, filename)
		if !ok {
			size = listingPlaceholderSize // Use placeholder if not cached
		}
		info := NewFileInfo(PathIno(path), size, 0644)
		if mtime > 0 {
			t := time.Unix(mtime, 0)
			info.Mtime = t
			info.Ctime = t
		}
		// If file is open, use accurate size from cached content
		v.applyOpenContentSize(path, info)
		return info, nil
	}

	// Is this path a smart query?
	if q := v.getQuery(ctx, path); q != nil {
		if q.OutputFormat == types.SmartQueryOutputFolder {
			info := NewDirInfo(PathIno(path))
			// Set Nlink based on cached results count for better UX
			// Standard Unix convention: Nlink = 2 + subdirectory count
			// For smart query folders, we use result count to show child items
			if cached := v.getCachedResultsNoRefresh(q.Path); cached != nil {
				info.Nlink = uint32(2 + len(cached))
			}
			qt := smartQueryMtime(q)
			info.Mtime = qt
			info.Ctime = qt
			return info, nil
		}
		info := NewFileInfo(PathIno(path), 0, 0644)
		qt := smartQueryMtime(q)
		info.Mtime = qt
		info.Ctime = qt
		v.applyOpenContentSize(path, info)
		return info, nil
	}

	// No native content fallback - paths inside integrations must be queries
	return nil, fs.ErrNotExist
}

func smartQueryMtime(q *types.SmartQuery) time.Time {
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

	// Is this a smart query folder? Execute it.
	if q := v.getQuery(ctx, path); q != nil {
		if q.OutputFormat == types.SmartQueryOutputFolder {
			return v.executeQueryAsDir(ctx, q)
		}
	}

	// /sources/{integration} - list README.md + smart queries
	if subpath == "" {
		return v.listIntegration(ctx, path, integration)
	}

	// Paths inside an integration but not a query folder = not found
	return nil, fs.ErrNotExist
}

// listIntegration returns integration root entries: README.md + smart queries.
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

// executeQueryAsDir executes a smart query and returns results as directory entries.
func (v *SourcesVNode) executeQueryAsDir(ctx context.Context, q *types.SmartQuery) ([]DirEntry, error) {
	// Always include the .query.as file
	queryMeta, _ := json.MarshalIndent(q, "", "  ")
	queryMtime := int64(smartQueryMtime(q).Unix())
	entries := []DirEntry{{
		Name:  queryMetaName,
		Mode:  syscall.S_IFREG | 0444,
		Ino:   PathIno(q.Path + "/" + queryMetaName),
		Size:  int64(len(queryMeta)),
		Mtime: queryMtime,
	}}

	// Check cache first
	if cached := v.getCachedResults(q.Path); cached != nil {
		for _, e := range cached {
			entries = append(entries, DirEntry{
				Name:  e.Name,
				Mode:  e.Mode,
				Ino:   PathIno(q.Path + "/" + e.Name),
				Size:  listingSize(e.Size),
				Mtime: e.Mtime,
			})
		}
		return entries, nil
	}

	// Execute via gateway RPC
	resp, err := v.client.ExecuteSmartQuery(ctx, &pb.ExecuteSmartQueryRequest{Path: q.Path})
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
		entries = append(entries, DirEntry{
			Name:  e.Name,
			Mode:  e.Mode,
			Ino:   PathIno(q.Path + "/" + e.Name),
			Size:  listingSize(e.Size),
			Mtime: e.Mtime,
		})
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
		return data, smartQueryMtime(q), true, nil
	}

	if strings.HasPrefix(base, ".") && strings.HasSuffix(base, queryMetaName) {
		queryFileName := strings.TrimPrefix(strings.TrimSuffix(base, queryMetaName), ".")
		queryPath := filepath.Join(filepath.Dir(path), queryFileName)
		q := v.getQuery(ctx, queryPath)
		if q == nil || q.OutputFormat != types.SmartQueryOutputFile {
			return nil, time.Time{}, true, fs.ErrNotExist
		}
		data, _ := json.MarshalIndent(q, "", "  ")
		return data, smartQueryMtime(q), true, nil
	}

	return nil, time.Time{}, false, nil
}

// Open opens a file.
func (v *SourcesVNode) Open(path string, flags int) (FileHandle, error) {
	path = filepath.Clean(path)

	// Reuse cached open content when possible
	if data, fh, ok := v.retainOpenContent(path); ok {
		v.cacheOpenStat(path, int64(len(data)), 0, time.Time{})
		return fh, nil
	}

	ctx, cancel := v.ctx()
	defer cancel()

	data, mode, mtime, ok, err := v.fetchContentForOpen(ctx, path)
	if !ok {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	fh := v.addOpenContent(path, data)
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
	// Serve from open content cache when available
	if data, ok := v.getOpenContent(path); ok {
		return copyFromOffset(buf, data, off), nil
	}

	ctx, cancel := v.ctx()
	defer cancel()

	// README.md at integration root
	integration, subpath := v.parsePath(path)
	if integration != "" && subpath == types.SourceStatusFile {
		data, err := v.readReadme(ctx, integration, off, int64(len(buf)))
		if err != nil {
			return 0, err
		}
		return copy(buf, data), nil
	}

	// Query metadata files (.query.as and .{name}.query.as)
	if data, _, isMeta, err := v.queryMetaContent(ctx, path); isMeta {
		if err != nil {
			return 0, err
		}
		return copyFromOffset(buf, data, off), nil
	}

	// Smart query file (single-file mode)
	if q := v.getQuery(ctx, path); q != nil && q.OutputFormat == types.SmartQueryOutputFile {
		return v.readQueryFile(ctx, q, buf, off)
	}

	// File inside smart query folder (materialized result)
	parentPath := filepath.Dir(path)
	if q := v.getQuery(ctx, parentPath); q != nil && q.OutputFormat == types.SmartQueryOutputFolder {
		return v.readQueryResult(ctx, q, filepath.Base(path), buf, off)
	}

	// No native content fallback - only query results are readable
	return 0, fs.ErrNotExist
}

// readQueryFile reads a single-file smart query result.
func (v *SourcesVNode) readQueryFile(ctx context.Context, q *types.SmartQuery, buf []byte, off int64) (int, error) {
	data, err := v.fetchQueryFileContent(ctx, q)
	if err != nil {
		return 0, fs.ErrNotExist
	}
	return copyFromOffset(buf, data, off), nil
}

// readQueryResult reads a specific file from query results.
func (v *SourcesVNode) readQueryResult(ctx context.Context, q *types.SmartQuery, filename string, buf []byte, off int64) (int, error) {
	data, err := v.fetchQueryResultContent(ctx, q, filename)
	if err != nil {
		return 0, fs.ErrNotExist
	}
	return copyFromOffset(buf, data, off), nil
}

func (v *SourcesVNode) fetchQueryFileContent(ctx context.Context, q *types.SmartQuery) ([]byte, error) {
	resp, err := v.client.ExecuteSmartQuery(ctx, &pb.ExecuteSmartQueryRequest{Path: q.Path})
	if err != nil || !resp.Ok || len(resp.FileData) == 0 {
		return nil, fs.ErrNotExist
	}
	return resp.FileData, nil
}

func (v *SourcesVNode) fetchQueryResultContent(ctx context.Context, q *types.SmartQuery, filename string) ([]byte, error) {
	// Look up the result_id from cache for more reliable fetching
	resultId := v.getResultIdFromCache(q.Path, filename)

	resp, err := v.client.ExecuteSmartQuery(ctx, &pb.ExecuteSmartQueryRequest{
		Path:     q.Path,
		Filename: filename,
		ResultId: resultId,
	})
	if err != nil || !resp.Ok || len(resp.FileData) == 0 {
		return nil, fs.ErrNotExist
	}
	return resp.FileData, nil
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
			if size <= 0 {
				size = listingPlaceholderSize // Use small placeholder when size unknown
			}
			return size, e.Mtime, true
		}
	}
	return 0, 0, false
}

func (v *SourcesVNode) fetchContentForOpen(ctx context.Context, path string) ([]byte, uint32, time.Time, bool, error) {
	integration, subpath := v.parsePath(path)
	if integration == "" {
		return nil, 0, time.Time{}, false, nil
	}

	// Ignore macOS system files
	if isSystemFile(integration) || (subpath != "" && isSystemFile(filepath.Base(subpath))) {
		return nil, 0, time.Time{}, true, fs.ErrNotExist
	}

	// README.md at integration root
	if subpath == types.SourceStatusFile {
		data, err := v.readReadme(ctx, integration, 0, 0)
		if err != nil {
			return nil, 0, time.Time{}, true, err
		}
		mode, mtime := v.readmeOpenInfo(path)
		return data, mode, mtime, true, nil
	}

	// Query metadata files (.query.as and .{name}.query.as)
	if data, mtime, isMeta, err := v.queryMetaContent(ctx, path); isMeta {
		if err != nil {
			return nil, 0, time.Time{}, true, err
		}
		return data, syscall.S_IFREG | 0444, mtime, true, nil
	}

	// Smart query file (single-file mode)
	if q := v.getQuery(ctx, path); q != nil && q.OutputFormat == types.SmartQueryOutputFile {
		data, err := v.fetchQueryFileContent(ctx, q)
		if err != nil {
			return nil, 0, time.Time{}, true, err
		}
		return data, syscall.S_IFREG | 0644, smartQueryMtime(q), true, nil
	}

	// File inside smart query folder (materialized result)
	parentPath := filepath.Dir(path)
	if q := v.getQuery(ctx, parentPath); q != nil && q.OutputFormat == types.SmartQueryOutputFolder {
		filename := filepath.Base(path)
		data, err := v.fetchQueryResultContent(ctx, q, filename)
		if err != nil {
			return nil, 0, time.Time{}, true, err
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
		return data, mode, mtime, true, nil
	}

	return nil, 0, time.Time{}, false, nil
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

// getResultIdFromCache looks up the result_id for a filename from the cached query results
func (v *SourcesVNode) getResultIdFromCache(queryPath, filename string) string {
	if cached := v.getCachedResultsNoRefresh(queryPath); cached != nil {
		for _, e := range cached {
			if e.Name == filename && e.ResultId != "" {
				return e.ResultId
			}
		}
	}
	return ""
}

// isSystemFile returns true if the filename is a system/metadata file that should be ignored.
// This includes macOS AppleDouble files (._*), .DS_Store, etc.
func isSystemFile(name string) bool {
	if strings.HasPrefix(name, "._") {
		return true // macOS AppleDouble extended attributes
	}
	if name == ".DS_Store" || name == ".Spotlight-V100" || name == ".Trashes" {
		return true // macOS system files
	}
	return false
}

// Mkdir creates a smart query folder.
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

	resp, err := v.client.CreateSmartQuery(ctx, &pb.CreateSmartQueryRequest{
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
	query := &types.SmartQuery{
		ExternalId:   resp.Query.ExternalId,
		Integration:  resp.Query.Integration,
		Path:         resp.Query.Path,
		Name:         resp.Query.Name,
		QuerySpec:    resp.Query.QuerySpec,
		Guidance:     resp.Query.Guidance,
		OutputFormat: types.SmartQueryOutputFormat(resp.Query.OutputFormat),
		FileExt:      resp.Query.FileExt,
		CacheTTL:     int(resp.Query.CacheTtl),
		CreatedAt:    time.Unix(resp.Query.CreatedAt, 0),
		UpdatedAt:    time.Unix(resp.Query.UpdatedAt, 0),
	}
	v.setCachedQuery(path, query)

	log.Info().Str("path", path).Str("query", resp.Query.QuerySpec).Msg("created smart query")
	return nil
}

// Create creates a smart query file.
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

	resp, err := v.client.CreateSmartQuery(ctx, &pb.CreateSmartQueryRequest{
		Integration: integration, Name: name, OutputFormat: "file", FileExt: ext,
	})
	if err != nil || !resp.Ok {
		return 0, syscall.EIO
	}

	// Cache the newly created query so subsequent Getattr calls can find it immediately
	query := &types.SmartQuery{
		ExternalId:   resp.Query.ExternalId,
		Integration:  resp.Query.Integration,
		Path:         resp.Query.Path,
		Name:         resp.Query.Name,
		QuerySpec:    resp.Query.QuerySpec,
		Guidance:     resp.Query.Guidance,
		OutputFormat: types.SmartQueryOutputFormat(resp.Query.OutputFormat),
		FileExt:      resp.Query.FileExt,
		CacheTTL:     int(resp.Query.CacheTtl),
		CreatedAt:    time.Unix(resp.Query.CreatedAt, 0),
		UpdatedAt:    time.Unix(resp.Query.UpdatedAt, 0),
	}
	v.setCachedQuery(path, query)

	log.Info().Str("path", path).Str("query", resp.Query.QuerySpec).Msg("created smart query file")
	return 0, nil
}

// Readlink reads symlink target.
// Note: Symlinks are not supported in the query-only model.
func (v *SourcesVNode) Readlink(path string) (string, error) {
	return "", fs.ErrNotExist
}

// getQuery retrieves a smart query by path, returns nil if not found.
// Uses local cache to avoid repeated GetSmartQuery RPCs.
func (v *SourcesVNode) getQuery(ctx context.Context, path string) *types.SmartQuery {
	// Check cache first
	if cached, found := v.getCachedQuery(path); found {
		return cached
	}

	resp, err := v.client.GetSmartQuery(ctx, &pb.GetSmartQueryRequest{Path: path})
	if err != nil {
		return nil
	}

	if resp == nil || !resp.Ok || resp.Query == nil {
		// Cache negative result too (path is not a query)
		v.setCachedQuery(path, nil)
		return nil
	}

	query := &types.SmartQuery{
		ExternalId:   resp.Query.ExternalId,
		Integration:  resp.Query.Integration,
		Path:         resp.Query.Path,
		Name:         resp.Query.Name,
		QuerySpec:    resp.Query.QuerySpec,
		Guidance:     resp.Query.Guidance,
		OutputFormat: types.SmartQueryOutputFormat(resp.Query.OutputFormat),
		FileExt:      resp.Query.FileExt,
		CacheTTL:     int(resp.Query.CacheTtl),
		CreatedAt:    time.Unix(resp.Query.CreatedAt, 0),
		UpdatedAt:    time.Unix(resp.Query.UpdatedAt, 0),
	}

	v.setCachedQuery(path, query)
	return query
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
	resp, err := v.client.ExecuteSmartQuery(ctx, &pb.ExecuteSmartQueryRequest{Path: queryPath})
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

// getQueryResultSize looks up the size of a file in query results.
// Uses local cache first (populated by Readdir), falls back to RPC.
func (v *SourcesVNode) getQueryResultSize(ctx context.Context, queryPath, filename string) int64 {
	size, _, ok := v.getQueryResultMeta(ctx, queryPath, filename)
	if !ok || size <= 0 {
		return defaultUnknownFileSize
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

// getCachedResults retrieves cached query results if still valid.
// If the cache is older than resultsCacheRefreshAge, triggers a background refresh
// while still returning the cached data (stale-while-revalidate pattern).
func (v *SourcesVNode) getCachedResults(queryPath string) []*pb.SourceDirEntry {
	v.resultsMu.RLock()
	defer v.resultsMu.RUnlock()

	if cached, ok := v.results[queryPath]; ok && time.Now().Before(cached.expiresAt) {
		// If cache is older than refresh threshold, trigger background refresh
		// This ensures new entries appear within ~45s + kernel timeout (1s) = 46s < 60s
		if time.Since(cached.cachedAt) > resultsCacheRefreshAge {
			go v.triggerQueryRefresh(queryPath)
		}
		return cached.entries
	}
	return nil
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

// triggerQueryRefresh refreshes the query results in the background
func (v *SourcesVNode) triggerQueryRefresh(queryPath string) {
	ctx, cancel := v.ctx()
	defer cancel()

	resp, err := v.client.ExecuteSmartQuery(ctx, &pb.ExecuteSmartQueryRequest{Path: queryPath})
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
func (v *SourcesVNode) getCachedQuery(path string) (*types.SmartQuery, bool) {
	v.queriesMu.RLock()
	defer v.queriesMu.RUnlock()

	if cached, ok := v.queries[path]; ok && time.Now().Before(cached.expiresAt) {
		return cached.query, true // query may be nil (negative cache)
	}
	return nil, false
}

// setCachedQuery stores query definition in the cache (nil for negative cache)
func (v *SourcesVNode) setCachedQuery(path string, query *types.SmartQuery) {
	v.queriesMu.Lock()
	defer v.queriesMu.Unlock()

	v.queries[path] = &cachedQuery{
		query:     query,
		expiresAt: time.Now().Add(resultsCacheTTL),
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

func (v *SourcesVNode) getOpenContent(path string) ([]byte, bool) {
	v.openMu.RLock()
	defer v.openMu.RUnlock()

	if cached, ok := v.openContent[path]; ok {
		return cached.data, true
	}
	return nil, false
}

func (v *SourcesVNode) addOpenContent(path string, data []byte) FileHandle {
	v.openMu.Lock()
	defer v.openMu.Unlock()

	fh := v.nextHandle
	v.nextHandle++
	v.openHandles[fh] = path

	if cached, ok := v.openContent[path]; ok {
		cached.refs++
		if data != nil {
			cached.data = data
			cached.cachedAt = time.Now()
		}
		return fh
	}

	v.openContent[path] = &cachedContent{
		data:     data,
		cachedAt: time.Now(),
		refs:     1,
	}
	return fh
}

func (v *SourcesVNode) retainOpenContent(path string) ([]byte, FileHandle, bool) {
	v.openMu.Lock()
	defer v.openMu.Unlock()

	cached, ok := v.openContent[path]
	if !ok {
		return nil, 0, false
	}

	fh := v.nextHandle
	v.nextHandle++
	v.openHandles[fh] = path
	cached.refs++
	return cached.data, fh, true
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

func (v *SourcesVNode) applyOpenContentSize(path string, info *FileInfo) {
	if info == nil {
		return
	}
	if data, ok := v.getOpenContent(path); ok {
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
