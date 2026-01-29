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
const resultsCacheTTL = 30 * time.Second // Cache query results to avoid repeated API calls

const queryMetaName = ".query.as"

// cachedQueryResult holds cached query execution results
type cachedQueryResult struct {
	entries   []*pb.SourceDirEntry
	expiresAt time.Time
}

// cachedQuery holds a cached query definition
type cachedQuery struct {
	query     *types.SmartQuery
	expiresAt time.Time
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
}

func NewSourcesVNode(conn *grpc.ClientConn, token string) *SourcesVNode {
	return &SourcesVNode{
		client:  pb.NewSourceServiceClient(conn),
		token:   token,
		results: make(map[string]*cachedQueryResult),
		queries: make(map[string]*cachedQuery),
	}
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
	log.Debug().Str("path", path).Msg("SourcesVNode.Getattr called")

	// /sources root
	if path == SourcesPath {
		return NewDirInfo(PathIno(path)), nil
	}

	integration, subpath := v.parsePath(path)
	ctx, cancel := v.ctx()
	defer cancel()

	// /sources/{integration}
	if subpath == "" {
		resp, err := v.client.Stat(ctx, &pb.SourceStatRequest{Path: integration})
		if err != nil || !resp.Ok {
			return nil, fs.ErrNotExist
		}
		return NewDirInfo(PathIno(path)), nil
	}

	// status.json at integration root
	if subpath == "status.json" {
		resp, err := v.client.Stat(ctx, &pb.SourceStatRequest{Path: integration + "/status.json"})
		if err != nil || resp == nil || !resp.Ok || resp.Info == nil {
			return nil, fs.ErrNotExist
		}
		return v.protoToFileInfo(path, resp.Info), nil
	}

	// Is this a .query.as file inside a smart query folder?
	if filepath.Base(path) == queryMetaName {
		queryPath := filepath.Dir(path)
		if q := v.getQuery(ctx, queryPath); q != nil {
			data, _ := json.MarshalIndent(q, "", "  ")
			info := NewFileInfo(PathIno(path), int64(len(data)), 0444)
			qt := smartQueryMtime(q)
			info.Mtime = qt
			info.Ctime = qt
			return info, nil
		}
		return nil, fs.ErrNotExist
	}

	// Is this a .{name}.query.as file (sibling metadata for single-file queries)?
	// Pattern: .all-receipts.json.query.as -> query for all-receipts.json
	base := filepath.Base(path)
	if strings.HasPrefix(base, ".") && strings.HasSuffix(base, queryMetaName) {
		// Extract the query filename: .all-receipts.json.query.as -> all-receipts.json
		queryFileName := strings.TrimPrefix(strings.TrimSuffix(base, queryMetaName), ".")
		queryPath := filepath.Join(filepath.Dir(path), queryFileName)
		if q := v.getQuery(ctx, queryPath); q != nil && q.OutputFormat == types.SmartQueryOutputFile {
			data, _ := json.MarshalIndent(q, "", "  ")
			info := NewFileInfo(PathIno(path), int64(len(data)), 0444)
			qt := smartQueryMtime(q)
			info.Mtime = qt
			info.Ctime = qt
			return info, nil
		}
		return nil, fs.ErrNotExist
	}

	// Is this inside a smart query folder? (materialized result)
	parentPath := filepath.Dir(path)
	if q := v.getQuery(ctx, parentPath); q != nil && q.OutputFormat == types.SmartQueryOutputFolder {
		filename := filepath.Base(path)
		size, mtime, ok := v.getQueryResultMeta(ctx, q.Path, filename)
		if !ok {
			// Not found; return a best-effort file info (will likely be treated as ENOENT later)
			return nil, fs.ErrNotExist
		}
		info := NewFileInfo(PathIno(path), size, 0644)
		if mtime > 0 {
			t := time.Unix(mtime, 0)
			info.Mtime = t
			info.Ctime = t
		}
		return info, nil
	}

	// Is this path a smart query?
	if q := v.getQuery(ctx, path); q != nil {
		if q.OutputFormat == types.SmartQueryOutputFolder {
			info := NewDirInfo(PathIno(path))
			qt := smartQueryMtime(q)
			info.Mtime = qt
			info.Ctime = qt
			return info, nil
		}
		info := NewFileInfo(PathIno(path), 0, 0644)
		qt := smartQueryMtime(q)
		info.Mtime = qt
		info.Ctime = qt
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
	log.Debug().Str("path", path).Msg("SourcesVNode.Readdir called")

	ctx, cancel := v.ctx()
	defer cancel()

	// /sources root - list available integrations
	if path == SourcesPath {
		log.Debug().Msg("Readdir: listing integrations")
		return v.listIntegrations(ctx)
	}

	integration, subpath := v.parsePath(path)
	log.Debug().Str("integration", integration).Str("subpath", subpath).Msg("Readdir: parsed path")

	// Is this a smart query folder? Execute it.
	log.Debug().Str("path", path).Msg("Readdir: checking if path is smart query")
	q := v.getQuery(ctx, path)
	if q != nil {
		log.Debug().Str("path", path).Str("outputFormat", string(q.OutputFormat)).Str("querySpec", q.QuerySpec).Msg("Readdir: found query")
		if q.OutputFormat == types.SmartQueryOutputFolder {
			log.Debug().Str("path", path).Msg("Readdir: executing as folder query")
			return v.executeQueryAsDir(ctx, q)
		}
		log.Debug().Str("path", path).Str("outputFormat", string(q.OutputFormat)).Msg("Readdir: query is not a folder type")
	} else {
		log.Debug().Str("path", path).Msg("Readdir: no query found for path")
	}

	// /sources/{integration} - list status.json + smart queries
	if subpath == "" {
		log.Debug().Str("integration", integration).Msg("Readdir: listing integration queries")
		return v.listIntegration(ctx, path, integration)
	}

	// Paths inside an integration but not a query folder = not found
	log.Debug().Str("path", path).Msg("Readdir: path not found")
	return nil, fs.ErrNotExist
}

// listIntegration returns integration root entries: status.json + smart queries.
// Native provider content (messages/, labels/, etc.) is not exposed directly.
func (v *SourcesVNode) listIntegration(ctx context.Context, path, integration string) ([]DirEntry, error) {
	// Use gateway ReadDir so we include status.json and query entries consistently
	resp, err := v.client.ReadDir(ctx, &pb.SourceReadDirRequest{Path: integration})
	if err != nil || resp == nil || !resp.Ok {
		return nil, nil
	}

	entries := make([]DirEntry, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		entries = append(entries, DirEntry{
			Name:  e.Name,
			Mode:  e.Mode,
			Ino:   PathIno(path + "/" + e.Name),
			Size:  e.Size,
			Mtime: e.Mtime,
		})
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
			entries = append(entries, DirEntry{Name: e.Name, Mode: e.Mode, Ino: PathIno(SourcesPath + "/" + e.Name)})
		}
	}
	return entries, nil
}

// executeQueryAsDir executes a smart query and returns results as directory entries.
func (v *SourcesVNode) executeQueryAsDir(ctx context.Context, q *types.SmartQuery) ([]DirEntry, error) {
	log.Debug().Str("path", q.Path).Msg("executeQueryAsDir: starting")

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
	log.Debug().Str("path", q.Path).Msg("executeQueryAsDir: checking cache")
	cached := v.getCachedResults(q.Path)
	if cached != nil {
		log.Debug().Str("path", q.Path).Int("count", len(cached)).Msg("executeQueryAsDir: using cached results")
		for _, e := range cached {
			entries = append(entries, DirEntry{
				Name:  e.Name,
				Mode:  e.Mode,
				Ino:   PathIno(q.Path + "/" + e.Name),
				Size:  e.Size,
				Mtime: e.Mtime,
			})
		}
		return entries, nil
	}

	// Execute via gateway RPC
	log.Debug().Str("path", q.Path).Msg("executeQueryAsDir: cache miss, calling ExecuteSmartQuery RPC")
	resp, err := v.client.ExecuteSmartQuery(ctx, &pb.ExecuteSmartQueryRequest{Path: q.Path})
	log.Debug().Str("path", q.Path).Err(err).Msg("executeQueryAsDir: RPC returned")
	if err != nil {
		log.Warn().Err(err).Str("path", q.Path).Msg("executeQueryAsDir: query execution failed")
		return entries, nil // Return just .query.as on failure
	}
	if !resp.Ok {
		log.Warn().Str("path", q.Path).Str("error", resp.Error).Msg("executeQueryAsDir: query execution returned not ok")
		return entries, nil
	}

	// Cache the results
	log.Debug().Str("path", q.Path).Int("count", len(resp.Entries)).Msg("executeQueryAsDir: caching results")
	v.setCachedResults(q.Path, resp.Entries)

	for _, e := range resp.Entries {
		entries = append(entries, DirEntry{
			Name:  e.Name,
			Mode:  e.Mode,
			Ino:   PathIno(q.Path + "/" + e.Name),
			Size:  e.Size,
			Mtime: e.Mtime,
		})
	}
	log.Debug().Str("path", q.Path).Int("count", len(resp.Entries)).Msg("executeQueryAsDir: completed successfully")
	return entries, nil
}

// Open opens a file.
func (v *SourcesVNode) Open(path string, flags int) (FileHandle, error) {
	return 0, nil
}

// Read reads file data.
func (v *SourcesVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	ctx, cancel := v.ctx()
	defer cancel()

	// status.json at integration root
	if integration, subpath := v.parsePath(path); integration != "" && subpath == "status.json" {
		resp, err := v.client.Read(ctx, &pb.SourceReadRequest{
			Path:   integration + "/status.json",
			Offset: off,
			Length: int64(len(buf)),
		})
		if err != nil || resp == nil || !resp.Ok {
			return 0, fs.ErrNotExist
		}
		return copy(buf, resp.Data), nil
	}

	// .query.as file inside a folder - return query definition
	if filepath.Base(path) == queryMetaName {
		queryPath := filepath.Dir(path)
		if q := v.getQuery(ctx, queryPath); q != nil {
			data, _ := json.MarshalIndent(q, "", "  ")
			if off >= int64(len(data)) {
				return 0, nil
			}
			return copy(buf, data[off:]), nil
		}
		return 0, fs.ErrNotExist
	}

	// .{name}.query.as file (sibling metadata for single-file queries)
	base := filepath.Base(path)
	if strings.HasPrefix(base, ".") && strings.HasSuffix(base, queryMetaName) {
		// Extract the query filename: .all-receipts.json.query.as -> all-receipts.json
		queryFileName := strings.TrimPrefix(strings.TrimSuffix(base, queryMetaName), ".")
		queryPath := filepath.Join(filepath.Dir(path), queryFileName)
		if q := v.getQuery(ctx, queryPath); q != nil && q.OutputFormat == types.SmartQueryOutputFile {
			data, _ := json.MarshalIndent(q, "", "  ")
			if off >= int64(len(data)) {
				return 0, nil
			}
			return copy(buf, data[off:]), nil
		}
		return 0, fs.ErrNotExist
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
	resp, err := v.client.ExecuteSmartQuery(ctx, &pb.ExecuteSmartQueryRequest{Path: q.Path})
	if err != nil || !resp.Ok || len(resp.FileData) == 0 {
		return 0, fs.ErrNotExist
	}
	if off >= int64(len(resp.FileData)) {
		return 0, nil
	}
	return copy(buf, resp.FileData[off:]), nil
}

// readQueryResult reads a specific file from query results.
func (v *SourcesVNode) readQueryResult(ctx context.Context, q *types.SmartQuery, filename string, buf []byte, off int64) (int, error) {
	resp, err := v.client.ExecuteSmartQuery(ctx, &pb.ExecuteSmartQueryRequest{Path: q.Path, Filename: filename})
	if err != nil || !resp.Ok || len(resp.FileData) == 0 {
		return 0, fs.ErrNotExist
	}
	if off >= int64(len(resp.FileData)) {
		return 0, nil
	}
	return copy(buf, resp.FileData[off:]), nil
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
		log.Debug().Str("path", path).Bool("isNil", cached == nil).Msg("getQuery: cache hit")
		return cached
	}

	log.Debug().Str("path", path).Msg("getQuery: cache miss, calling RPC")
	resp, err := v.client.GetSmartQuery(ctx, &pb.GetSmartQueryRequest{Path: path})
	if err != nil {
		log.Debug().Err(err).Str("path", path).Msg("getQuery: gRPC error")
		return nil
	}

	if resp == nil || !resp.Ok || resp.Query == nil {
		log.Debug().Str("path", path).Bool("respNil", resp == nil).Msg("getQuery: returned nil/not ok")
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

	log.Debug().
		Str("path", path).
		Str("queryPath", resp.Query.Path).
		Str("outputFormat", resp.Query.OutputFormat).
		Str("convertedFormat", string(query.OutputFormat)).
		Msg("getQuery: found query, caching")
	v.setCachedQuery(path, query)
	return query
}

func (v *SourcesVNode) getQueryResultMeta(ctx context.Context, queryPath, filename string) (size int64, mtime int64, ok bool) {
	// Check local cache first (populated by executeQueryAsDir during Readdir)
	if cached := v.getCachedResults(queryPath); cached != nil {
		for _, e := range cached {
			if e.Name == filename {
				size = e.Size
				if size <= 0 {
					size = 4096 // Default size - FUSE will read to get actual size
				}
				return size, e.Mtime, true
			}
		}
	}

	// Cache miss - execute query via RPC (should be cached on gateway side)
	log.Debug().Str("path", queryPath).Str("filename", filename).Msg("getQueryResultMeta cache miss, calling RPC")
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
				size = 4096 // Default size - FUSE will read to get actual size
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
		return 4096
	}
	return size
}

func (v *SourcesVNode) protoToFileInfo(path string, info *pb.SourceFileInfo) *FileInfo {
	now := time.Now()
	mtime := now
	if info.Mtime > 0 {
		mtime = time.Unix(info.Mtime, 0)
	}
	return &FileInfo{
		Ino: PathIno(path), Size: info.Size, Mode: info.Mode, Nlink: 1,
		Uid: uint32(syscall.Getuid()), Gid: uint32(syscall.Getgid()),
		Atime: now, Mtime: mtime, Ctime: mtime,
	}
}

// getCachedResults retrieves cached query results if still valid
func (v *SourcesVNode) getCachedResults(queryPath string) []*pb.SourceDirEntry {
	v.resultsMu.RLock()
	defer v.resultsMu.RUnlock()

	if cached, ok := v.results[queryPath]; ok && time.Now().Before(cached.expiresAt) {
		return cached.entries
	}
	return nil
}

// setCachedResults stores query results in the cache
func (v *SourcesVNode) setCachedResults(queryPath string, entries []*pb.SourceDirEntry) {
	v.resultsMu.Lock()
	defer v.resultsMu.Unlock()

	v.results[queryPath] = &cachedQueryResult{
		entries:   entries,
		expiresAt: time.Now().Add(resultsCacheTTL),
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
