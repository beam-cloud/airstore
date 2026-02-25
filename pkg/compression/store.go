package compression

import (
	"context"
	"encoding/json"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// RedisClient is the minimal Redis interface used by CompressedStore.
// Both *redis.Client and *common.RedisClient satisfy this.
type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Pipeline() redis.Pipeliner
}

// CompressedPointer is small metadata stored in Redis that maps
// a (workspace, queryPath, resultID, strategy) to token counts.
type CompressedPointer struct {
	OriginalTokens   int    `json:"original_tokens"`
	CompressedTokens int    `json:"compressed_tokens"`
	Strategy         string `json:"strategy"`
	CreatedAt        int64  `json:"created_at"`
	Size             int    `json:"size"`
}

// ScanClient is an optional extension for RedisClient that supports
// key scanning. *common.RedisClient satisfies this.
type ScanClient interface {
	Scan(ctx context.Context, pattern string) ([]string, error)
}

// CompressedStore handles read/write of compressed content pointers and
// cached content in Redis, with a per-workspace byte budget.
type CompressedStore struct {
	redis         RedisClient
	cacheMaxBytes int64
	cacheTTL      time.Duration
}

// NewCompressedStore creates a store backed by Redis.
func NewCompressedStore(rdb RedisClient, cfg Config) *CompressedStore {
	maxBytes := cfg.ContentCacheMaxBytes
	if maxBytes <= 0 {
		maxBytes = 10 * 1024 * 1024 // 10 MB
	}
	ttl := cfg.ContentCacheTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &CompressedStore{
		redis:         rdb,
		cacheMaxBytes: maxBytes,
		cacheTTL:      ttl,
	}
}

// GetPointer reads the compression pointer for a result+strategy. Returns nil if not found.
func (s *CompressedStore) GetPointer(ctx context.Context, workspaceId uint, queryPath, resultID, strategy string) *CompressedPointer {
	if s.redis == nil {
		return nil
	}
	key := common.Keys.FsCompressedPointer(workspaceId, queryPath, resultID, strategy)
	data, err := s.redis.Get(ctx, key).Bytes()
	if err != nil {
		return nil
	}
	var ptr CompressedPointer
	if err := json.Unmarshal(data, &ptr); err != nil {
		return nil
	}
	return &ptr
}

// SetPointer writes the compression pointer with the same TTL as content.
// Pointers are small (~200 bytes); they expire alongside the cached content.
func (s *CompressedStore) SetPointer(ctx context.Context, workspaceId uint, queryPath, resultID, strategy string, ptr *CompressedPointer) error {
	if s.redis == nil {
		return nil
	}
	key := common.Keys.FsCompressedPointer(workspaceId, queryPath, resultID, strategy)
	data, err := json.Marshal(ptr)
	if err != nil {
		return err
	}
	if err := s.redis.Set(ctx, key, data, s.cacheTTL).Err(); err != nil {
		return err
	}
	s.trackQueryCacheKey(ctx, workspaceId, queryPath, key)
	return nil
}

// GetContent reads cached compressed content. Returns nil on miss.
func (s *CompressedStore) GetContent(ctx context.Context, workspaceId uint, queryPath, resultID, strategy string) []byte {
	if s.redis == nil {
		return nil
	}
	key := common.Keys.FsCompressedContent(workspaceId, queryPath, resultID, strategy)
	data, err := s.redis.Get(ctx, key).Bytes()
	if err != nil {
		return nil
	}
	return data
}

// SetContent caches compressed content subject to the per-workspace byte budget.
// If adding this entry would exceed the budget, the write is silently skipped.
func (s *CompressedStore) SetContent(ctx context.Context, workspaceId uint, queryPath, resultID, strategy string, content []byte) error {
	if s.redis == nil {
		return nil
	}

	// Check budget
	usageKey := common.Keys.FsCompressedUsage(workspaceId)
	currentUsage, _ := s.redis.Get(ctx, usageKey).Int64()
	if currentUsage+int64(len(content)) > s.cacheMaxBytes {
		log.Debug().
			Int64("current", currentUsage).
			Int("adding", len(content)).
			Int64("max", s.cacheMaxBytes).
			Msg("compressed content cache budget exceeded, skipping cache write")
		return nil // budget exceeded, skip
	}

	key := common.Keys.FsCompressedContent(workspaceId, queryPath, resultID, strategy)
	pipe := s.redis.Pipeline()
	pipe.Set(ctx, key, content, s.cacheTTL)
	pipe.IncrBy(ctx, usageKey, int64(len(content)))
	// Set a TTL on the usage counter too — stale counters self-correct
	pipe.Expire(ctx, usageKey, s.cacheTTL*2)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	s.trackQueryCacheKey(ctx, workspaceId, queryPath, key)
	return nil
}

// trackQueryCacheKey adds a pointer/content cache key to the per-view compressed
// index so invalidation can use SMEMBERS instead of SCAN.
func (s *CompressedStore) trackQueryCacheKey(ctx context.Context, workspaceId uint, queryPath, cacheKey string) {
	indexKey := common.Keys.FsCompressedIndex(workspaceId, queryPath)
	if err := s.redis.SAdd(ctx, indexKey, cacheKey).Err(); err != nil {
		return
	}
	_ = s.redis.Expire(ctx, indexKey, s.cacheTTL*2).Err()
}

// FlushWorkspace deletes all compressed pointers, content, and usage
// keys for a workspace. The RedisClient must implement ScanClient
// (e.g., *common.RedisClient) for pattern-based key discovery.
func (s *CompressedStore) FlushWorkspace(ctx context.Context, workspaceId uint) (int, error) {
	if s.redis == nil {
		return 0, nil
	}

	scanner, ok := s.redis.(ScanClient)
	if !ok {
		// Fallback: just delete the usage counter (pointers/content have TTLs).
		s.redis.Del(ctx, common.Keys.FsCompressedUsage(workspaceId))
		return 1, nil
	}

	patterns := common.Keys.FsCompressedScanPatterns(workspaceId)
	total := 0
	for _, pattern := range patterns {
		keys, err := scanner.Scan(ctx, pattern)
		if err != nil {
			return total, err
		}
		if len(keys) == 0 {
			continue
		}
		deleted, err := s.redis.Del(ctx, keys...).Result()
		total += int(deleted)
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
