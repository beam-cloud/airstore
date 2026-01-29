package sources

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	DefaultCacheTTL  = 30 * time.Second // DefaultCacheTTL is the default TTL for cached source data
	DefaultCacheSize = 10000            // DefaultCacheSize is the maximum number of cache entries

)

// CacheEntry holds a cached response with expiration
type CacheEntry struct {
	Data      []byte
	Info      *FileInfo
	Entries   []DirEntry
	ExpiresAt time.Time
}

// IsExpired returns true if the entry has expired
func (e *CacheEntry) IsExpired() bool {
	return time.Now().After(e.ExpiresAt)
}

// SourceCache provides caching and request coalescing for source operations.
// It helps prevent upstream API hammering when multiple agents read the same data.
type SourceCache struct {
	mu      sync.RWMutex
	entries map[string]*CacheEntry
	ttl     time.Duration
	maxSize int

	// Singleflight for request coalescing
	group singleflight.Group
}

// NewSourceCache creates a new source cache
func NewSourceCache(ttl time.Duration, maxSize int) *SourceCache {
	if ttl <= 0 {
		ttl = DefaultCacheTTL
	}
	if maxSize <= 0 {
		maxSize = DefaultCacheSize
	}

	cache := &SourceCache{
		entries: make(map[string]*CacheEntry),
		ttl:     ttl,
		maxSize: maxSize,
	}

	// Start background cleanup goroutine
	go cache.cleanupLoop()

	return cache
}

// CacheKey generates a cache key for a source request
func CacheKey(workspaceId uint, integration, path, operation string) string {
	return fmt.Sprintf("%d:%s:%s:%s", workspaceId, integration, path, operation)
}

// GetData returns cached data if available and not expired
func (c *SourceCache) GetData(key string) ([]byte, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok || entry.IsExpired() {
		return nil, false
	}

	return entry.Data, true
}

// GetInfo returns cached FileInfo if available and not expired
func (c *SourceCache) GetInfo(key string) (*FileInfo, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok || entry.IsExpired() {
		return nil, false
	}

	return entry.Info, true
}

// GetEntries returns cached DirEntry list if available and not expired
func (c *SourceCache) GetEntries(key string) ([]DirEntry, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok || entry.IsExpired() {
		return nil, false
	}

	return entry.Entries, true
}

// SetData caches data with the default TTL
func (c *SourceCache) SetData(key string, data []byte) {
	c.set(key, &CacheEntry{
		Data:      data,
		ExpiresAt: time.Now().Add(c.ttl),
	})
}

// SetInfo caches FileInfo with the default TTL
func (c *SourceCache) SetInfo(key string, info *FileInfo) {
	c.set(key, &CacheEntry{
		Info:      info,
		ExpiresAt: time.Now().Add(c.ttl),
	})
}

// SetEntries caches directory entries with the default TTL
func (c *SourceCache) SetEntries(key string, entries []DirEntry) {
	c.set(key, &CacheEntry{
		Entries:   entries,
		ExpiresAt: time.Now().Add(c.ttl),
	})
}

// SetEntriesWithTTL caches directory entries with a custom TTL
func (c *SourceCache) SetEntriesWithTTL(key string, entries []DirEntry, ttl time.Duration) {
	if ttl <= 0 {
		ttl = c.ttl
	}
	c.set(key, &CacheEntry{
		Entries:   entries,
		ExpiresAt: time.Now().Add(ttl),
	})
}

func (c *SourceCache) set(key string, entry *CacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict if at capacity (simple random eviction)
	if len(c.entries) >= c.maxSize {
		// Remove first 10% of entries
		count := c.maxSize / 10
		for k := range c.entries {
			delete(c.entries, k)
			count--
			if count <= 0 {
				break
			}
		}
	}

	c.entries[key] = entry
}

// DoOnce executes a function only once for concurrent requests with the same key.
// Other callers with the same key will wait and receive the same result.
func (c *SourceCache) DoOnce(key string, fn func() (any, error)) (any, error) {
	result, err, _ := c.group.Do(key, fn)
	return result, err
}

// Invalidate removes a cache entry
func (c *SourceCache) Invalidate(key string) {
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// InvalidatePrefix removes all cache entries matching a prefix
func (c *SourceCache) InvalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.entries {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(c.entries, key)
		}
	}
}

// cleanupLoop periodically removes expired entries
func (c *SourceCache) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	for range ticker.C {
		c.cleanup()
	}
}

func (c *SourceCache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.entries {
		if now.After(entry.ExpiresAt) {
			delete(c.entries, key)
		}
	}
}

// Stats returns cache statistics
func (c *SourceCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expired := 0
	now := time.Now()
	for _, entry := range c.entries {
		if now.After(entry.ExpiresAt) {
			expired++
		}
	}

	return CacheStats{
		Size:    len(c.entries),
		MaxSize: c.maxSize,
		TTL:     c.ttl,
		Expired: expired,
	}
}

// CacheStats contains cache statistics
type CacheStats struct {
	Size    int
	MaxSize int
	TTL     time.Duration
	Expired int
}
