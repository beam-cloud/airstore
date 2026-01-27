package vnode

import (
	"path"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

const (
	cacheSize        = 10000
	cacheTTL         = 60 * time.Second // S3 metadata is stable; longer TTL reduces API calls
	negativeCacheTTL = 5 * time.Second  // Missing files may appear soon
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

// NewMetadataCache creates a new cache
func NewMetadataCache() *MetadataCache {
	return &MetadataCache{
		entries:  expirable.NewLRU[string, *CacheEntry](cacheSize, nil, cacheTTL),
		negative: expirable.NewLRU[string, struct{}](cacheSize, nil, negativeCacheTTL),
	}
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
