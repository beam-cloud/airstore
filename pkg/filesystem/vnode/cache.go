package vnode

import (
	"path"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

const (
	cacheSize        = 10000
	cacheTTL         = 60 * time.Second // S3 metadata is stable; longer TTL reduces API calls
	negativeCacheTTL = 5 * time.Second  // Missing files may appear soon

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
