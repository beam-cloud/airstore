package vnode

import (
	"context"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/hashicorp/golang-lru/v2/expirable"

	pb "github.com/beam-cloud/airstore/proto"
)

const (
	cacheSize           = 10000
	cacheTTL            = 60 * time.Second // S3 metadata is stable; longer TTL reduces API calls
	negativeCacheTTL    = 2 * time.Second  // Missing files may appear soon (kept short for external creates)
	contentCacheEntries = 1024
	contentCacheTTL     = 2 * time.Minute
	smallFileMaxSize    = 128 * 1024 // Cache small files only (128KB)
)

// MetadataCache wraps an LRU cache for filesystem metadata with parent-child awareness.
type MetadataCache struct {
	entries  *expirable.LRU[string, *CacheEntry]
	negative *expirable.LRU[string, struct{}]
}

// CacheEntry holds cached metadata
type CacheEntry struct {
	Info      *FileInfo
	Children  []DirEntry
	ChildMeta map[string]*FileInfo
}

// ContentEntry holds cached file contents with mtime validation.
type ContentEntry struct {
	Data     []byte
	Mtime    int64
	Size     int64
	CachedAt time.Time
}

// ContentCache is a small in-memory cache for file contents.
// Entries are served only if the current mtime matches.
type ContentCache struct {
	entries *expirable.LRU[string, *ContentEntry]
	mu      sync.Mutex
}

// NewMetadataCache creates a new cache
func NewMetadataCache() *MetadataCache {
	return &MetadataCache{
		entries:  expirable.NewLRU[string, *CacheEntry](cacheSize, nil, cacheTTL),
		negative: expirable.NewLRU[string, struct{}](cacheSize, nil, negativeCacheTTL),
	}
}

// NewContentCache creates a new content cache for small files.
func NewContentCache() *ContentCache {
	return &ContentCache{
		entries: expirable.NewLRU[string, *ContentEntry](contentCacheEntries, nil, contentCacheTTL),
	}
}

// Get returns cached content only if mtime matches.
func (c *ContentCache) Get(path string, currentMtime int64) ([]byte, bool) {
	if currentMtime == 0 {
		return nil, false
	}
	c.mu.Lock()
	entry, ok := c.entries.Get(path)
	if !ok {
		c.mu.Unlock()
		return nil, false
	}
	if entry.Mtime != currentMtime {
		c.entries.Remove(path)
		c.mu.Unlock()
		return nil, false
	}
	data := entry.Data
	c.mu.Unlock()
	return data, true
}

// Set stores content if it is small enough to cache.
func (c *ContentCache) Set(path string, data []byte, mtime int64) {
	if mtime == 0 {
		return
	}
	if int64(len(data)) > smallFileMaxSize {
		return
	}
	c.mu.Lock()
	c.entries.Add(path, &ContentEntry{
		Data:     data,
		Mtime:    mtime,
		Size:     int64(len(data)),
		CachedAt: time.Now(),
	})
	c.mu.Unlock()
}

// Invalidate removes a cached content entry for a path.
func (c *ContentCache) Invalidate(path string) {
	c.mu.Lock()
	c.entries.Remove(path)
	c.mu.Unlock()
}

// Get returns cached entry or nil
func (c *MetadataCache) Get(p string) *CacheEntry {
	if entry, ok := c.entries.Get(p); ok {
		return entry
	}
	return nil
}

// GetInfo returns FileInfo from cache, checking parent's ChildMeta on miss
func (c *MetadataCache) GetInfo(p string) *FileInfo {
	if entry, ok := c.entries.Get(p); ok {
		return entry.Info
	}
	// Check parent's child metadata (from enriched readdir)
	if parent, ok := c.entries.Get(path.Dir(p)); ok && parent.ChildMeta != nil {
		return parent.ChildMeta[path.Base(p)]
	}
	return nil
}

// IsNegative returns true if path is known to not exist
func (c *MetadataCache) IsNegative(p string) bool {
	_, ok := c.negative.Get(p)
	return ok
}

// Set caches metadata for a path
func (c *MetadataCache) Set(p string, info *FileInfo) {
	c.negative.Remove(p)
	c.entries.Add(p, &CacheEntry{Info: info})
}

// SetWithChildren caches directory with enriched child metadata
func (c *MetadataCache) SetWithChildren(p string, children []DirEntry, childMeta map[string]*FileInfo) {
	c.negative.Remove(p)
	for _, child := range children {
		c.negative.Remove(path.Join(p, child.Name))
	}
	c.entries.Add(p, &CacheEntry{
		Info:      NewDirInfo(PathIno(p)),
		Children:  children,
		ChildMeta: childMeta,
	})
}

// SetNegative marks path as non-existent
func (c *MetadataCache) SetNegative(p string) {
	c.negative.Add(p, struct{}{})
}

// Invalidate removes path and parent's children cache
func (c *MetadataCache) Invalidate(p string) {
	c.entries.Remove(p)
	c.negative.Remove(p)
	if parent, ok := c.entries.Get(path.Dir(p)); ok {
		parent.Children = nil
		parent.ChildMeta = nil
	}
}

// InvalidateChild removes a specific child from parent's cache without invalidating siblings.
// This is more efficient than Invalidate when only one child has changed.
func (c *MetadataCache) InvalidateChild(parentPath, childName string) {
	// Remove the child's own entry
	childPath := parentPath + "/" + childName
	if parentPath == "/" {
		childPath = "/" + childName
	}
	c.entries.Remove(childPath)
	c.negative.Remove(childPath)

	// Update parent's cached children list to remove the specific child
	if parent, ok := c.entries.Get(parentPath); ok {
		// Remove from ChildMeta
		if parent.ChildMeta != nil {
			delete(parent.ChildMeta, childName)
		}

		// Remove from Children slice
		if parent.Children != nil {
			filtered := make([]DirEntry, 0, len(parent.Children))
			for _, entry := range parent.Children {
				if entry.Name != childName {
					filtered = append(filtered, entry)
				}
			}
			parent.Children = filtered
		}
	}
}

// AddChild adds a new child entry to parent's cache without invalidating existing siblings.
// This is useful after creating a new file/directory.
func (c *MetadataCache) AddChild(parentPath string, child DirEntry, childInfo *FileInfo) {
	if parent, ok := c.entries.Get(parentPath); ok {
		// Check if already exists in Children
		exists := false
		for _, entry := range parent.Children {
			if entry.Name == child.Name {
				exists = true
				break
			}
		}
		if !exists && parent.Children != nil {
			parent.Children = append(parent.Children, child)
		}

		// Add to ChildMeta
		if parent.ChildMeta != nil && childInfo != nil {
			parent.ChildMeta[child.Name] = childInfo
		}
	}
}

// -----------------------------------------------------------------------------
// Sources VNode cache + refresh coordinator
// -----------------------------------------------------------------------------

// Cache TTLs
const (
	resultsCacheTTL        = 45 * time.Second // hard expiry for listing caches
	resultsCacheRefreshAge = 30 * time.Second // background loop refreshes listings older than this
	queryDefCacheTTL       = 15 * time.Second // view definition cache; shorter so syncs are detected quickly
	prefetchTTL            = 30 * time.Second // unreferenced prefetch entries are evicted after this
)

// Background refresh tuning
const (
	backgroundRefreshInterval = 15 * time.Second // fixed cadence for proactive refresh work
	recentDirWindow           = 2 * time.Minute  // how long a directory is considered "active"
	recentDirRetention        = 5 * time.Minute  // when to evict old activity tracking entries
	maxQueryRefreshPerTick    = 4                // bounds provider load per tick
)

// cachedQueryResult holds cached query execution results.
type cachedQueryResult struct {
	entries   []*pb.SourceDirEntry
	expiresAt time.Time
	cachedAt  time.Time
}

// cachedQuery holds a cached query definition.
type cachedQuery struct {
	query     *types.SourceView
	expiresAt time.Time
}

// cachedIntegration holds cached integration metadata.
type cachedIntegration struct {
	mtime     int64
	expiresAt time.Time
}

// cachedStat holds cached stat metadata for a path.
type cachedStat struct {
	info      *FileInfo
	expiresAt time.Time
}

// cachedContent holds open file content with a reference count.
type cachedContent struct {
	data     []byte
	hint     *pb.SourceReadCostHint
	cachedAt time.Time
	fetchMs  int64
	refs     int
}

// getQuery retrieves a source view by path using local query-definition cache.
func (v *SourcesVNode) getQuery(ctx context.Context, path string) *types.SourceView {
	if cached, found := v.getCachedQuery(path); found {
		return cached
	}

	resp, err := v.client.GetView(ctx, &pb.GetViewRequest{Path: path})
	if err != nil {
		return nil
	}
	if resp == nil || !resp.Ok || resp.View == nil {
		// Only negatively cache paths that are NOT system-managed
		// followup views. Followup views are frequently deleted and
		// recreated during source watch re-registration; negative
		// caching would make them appear "not found" for up to
		// queryDefCacheTTL after re-creation.
		if !strings.Contains(path, "__followup__") {
			v.setCachedQuery(path, nil)
		}
		return nil
	}

	query := protoToSourceView(resp.View)
	v.setCachedQuery(path, query)
	return query
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
// and the view has not been synced since the cache was populated.
func (v *SourcesVNode) getCachedResultsIfFresh(q *types.SourceView) []*pb.SourceDirEntry {
	v.resultsMu.RLock()
	cached, ok := v.results[q.Path]
	if !ok || !time.Now().Before(cached.expiresAt) {
		v.resultsMu.RUnlock()
		return nil
	}

	staleBySync := q.LastExecuted != nil && q.LastExecuted.After(cached.cachedAt)
	entries := cached.entries
	v.resultsMu.RUnlock()

	if staleBySync {
		v.evictViewCaches(q.Path)
		return nil
	}
	return entries
}

// evictViewCaches removes result, stat, and open-content cache entries for a view.
func (v *SourcesVNode) evictViewCaches(viewPath string) {
	v.resultsMu.Lock()
	delete(v.results, viewPath)
	v.resultsMu.Unlock()

	prefix := viewPath + "/"
	v.statsMu.Lock()
	for k := range v.stats {
		if strings.HasPrefix(k, prefix) {
			delete(v.stats, k)
		}
	}
	v.statsMu.Unlock()

	v.openMu.Lock()
	for k := range v.openContent {
		if strings.HasPrefix(k, prefix) {
			delete(v.openContent, k)
		}
	}
	v.openMu.Unlock()
}

// triggerQueryRefresh refreshes a source-view listing in the background.
func (v *SourcesVNode) triggerQueryRefresh(queryPath string) {
	ctx, cancel := v.ctx()
	defer cancel()

	resp, err := v.client.ExecuteView(ctx, &pb.ExecuteViewRequest{Path: queryPath})
	if err != nil || !resp.Ok {
		return
	}
	v.setCachedResults(queryPath, resp.Entries)
}

// shouldRefreshQueryPath reports whether a cached query listing should be
// proactively refreshed by the background loop.
func (v *SourcesVNode) shouldRefreshQueryPath(queryPath string) bool {
	v.resultsMu.RLock()
	defer v.resultsMu.RUnlock()

	cached, ok := v.results[queryPath]
	if !ok {
		return false
	}
	now := time.Now()
	if now.After(cached.expiresAt) {
		return true
	}
	return now.Sub(cached.cachedAt) > resultsCacheRefreshAge
}

// setCachedResults stores query results in cache.
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

// getCachedQuery retrieves a cached query definition.
func (v *SourcesVNode) getCachedQuery(path string) (*types.SourceView, bool) {
	v.queriesMu.RLock()
	defer v.queriesMu.RUnlock()

	if cached, ok := v.queries[path]; ok && time.Now().Before(cached.expiresAt) {
		return cached.query, true
	}
	return nil, false
}

// setCachedQuery stores a query definition (nil allowed for negative cache).
func (v *SourcesVNode) setCachedQuery(path string, query *types.SourceView) {
	v.queriesMu.Lock()
	defer v.queriesMu.Unlock()

	v.queries[path] = &cachedQuery{
		query:     query,
		expiresAt: time.Now().Add(queryDefCacheTTL),
	}
}

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

func (v *SourcesVNode) getCachedStat(path string) *FileInfo {
	v.statsMu.RLock()
	defer v.statsMu.RUnlock()

	if cached, ok := v.stats[path]; ok && time.Now().Before(cached.expiresAt) && cached.info != nil {
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

// prefetchContent stores content in the open-content cache without creating a
// file handle. Used by Getattr to pre-fetch content for accurate size reporting.
func (v *SourcesVNode) prefetchContent(path string, data []byte, hint *pb.SourceReadCostHint) {
	v.openMu.Lock()
	defer v.openMu.Unlock()

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
			refs:     0,
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

// cacheStatFromEntry creates and caches FileInfo from a SourceDirEntry.
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

// trackRecentDir records a directory as recently accessed for background refresh.
func (v *SourcesVNode) trackRecentDir(path string) {
	v.recentDirsMu.Lock()
	v.recentDirs[path] = time.Now()
	v.recentDirsMu.Unlock()
}

// getRecentDirs returns directories accessed in the recent active window.
func (v *SourcesVNode) getRecentDirs() []string {
	v.recentDirsMu.RLock()
	defer v.recentDirsMu.RUnlock()

	cutoff := time.Now().Add(-recentDirWindow)
	dirs := make([]string, 0, len(v.recentDirs))
	for path, accessed := range v.recentDirs {
		if accessed.After(cutoff) {
			dirs = append(dirs, path)
		}
	}
	return dirs
}

// cleanupOldRecentDirs removes directories not accessed recently.
func (v *SourcesVNode) cleanupOldRecentDirs() {
	v.recentDirsMu.Lock()
	defer v.recentDirsMu.Unlock()

	cutoff := time.Now().Add(-recentDirRetention)
	for path, accessed := range v.recentDirs {
		if accessed.Before(cutoff) {
			delete(v.recentDirs, path)
		}
	}
}

// backgroundRefreshLoop periodically refreshes caches for frequently accessed paths.
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

// doBackgroundRefresh refreshes integration and source-view listings for
// recently accessed directories at a fixed cadence.
func (v *SourcesVNode) doBackgroundRefresh() {
	ctx, cancel := v.ctx()
	defer cancel()

	v.refreshIntegrations(ctx)

	queryRefreshes := 0
	for _, path := range v.getRecentDirs() {
		integration, subpath := v.parsePath(path)
		if integration == "" {
			continue
		}
		if subpath == "" {
			v.refreshIntegrationDir(ctx, path, integration)
			continue
		}

		if queryRefreshes >= maxQueryRefreshPerTick {
			continue
		}
		q := v.getQuery(ctx, path)
		if q == nil || q.OutputFormat != types.ViewOutputFolder {
			continue
		}
		if v.shouldRefreshQueryPath(q.Path) {
			v.triggerQueryRefresh(q.Path)
			queryRefreshes++
		}
	}

	v.cleanupOldRecentDirs()
	v.cleanupExpiredCaches()
}

// cleanupExpiredCaches removes expired entries from unbounded maps that only
// check TTL on read but never evict. Without this, maps grow indefinitely
// during long-running sessions.
func (v *SourcesVNode) cleanupExpiredCaches() {
	now := time.Now()

	v.resultsMu.Lock()
	for k, r := range v.results {
		if now.After(r.expiresAt) {
			delete(v.results, k)
		}
	}
	v.resultsMu.Unlock()

	v.queriesMu.Lock()
	for k, q := range v.queries {
		if now.After(q.expiresAt) {
			delete(v.queries, k)
		}
	}
	v.queriesMu.Unlock()

	v.integrationsMu.Lock()
	for k, i := range v.integrations {
		if now.After(i.expiresAt) {
			delete(v.integrations, k)
		}
	}
	v.integrationsMu.Unlock()

	v.statsMu.Lock()
	for k, s := range v.stats {
		if now.After(s.expiresAt) {
			delete(v.stats, k)
		}
	}
	v.statsMu.Unlock()

	v.openMu.Lock()
	for k, c := range v.openContent {
		if c.refs == 0 && now.Sub(c.cachedAt) > prefetchTTL {
			delete(v.openContent, k)
		}
	}
	v.openMu.Unlock()
}

// refreshIntegrations refreshes the integration list cache.
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

// refreshIntegrationDir refreshes the cache for an integration directory.
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
