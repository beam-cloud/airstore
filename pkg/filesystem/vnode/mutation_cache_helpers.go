package vnode

import "strings"

// invalidatePathCaches clears metadata and content caches for a specific path.
func invalidatePathCaches(cache *MetadataCache, content *ContentCache, path string) {
	if cache != nil {
		cache.Invalidate(path)
	}
	if content != nil {
		content.Invalidate(path)
	}
}

// invalidateParentCache evicts one child entry from the cached parent listing.
func invalidateParentCache(cache *MetadataCache, path string) {
	if cache == nil {
		return
	}

	parentPath := "/"
	childName := strings.TrimPrefix(path, "/")
	if idx := strings.LastIndex(path, "/"); idx > 0 {
		parentPath = path[:idx]
		childName = path[idx+1:]
	}

	cache.InvalidateChild(parentPath, childName)
}
