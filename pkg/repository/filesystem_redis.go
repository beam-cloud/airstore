package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/redis/go-redis/v9"
)

const (
	keyDirMeta     = "airstore:fs:dir:%s"  // path hash
	keyFileMeta    = "airstore:fs:file:%s" // path hash
	keySymlink     = "airstore:fs:link:%s" // path hash
	keyDirChildren = "airstore:fs:ls:%s"   // path hash
	defaultTTL     = 30 * time.Second
)

// FilesystemRedisRepository implements FilesystemRepository using Redis
type FilesystemRedisRepository struct {
	rdb *common.RedisClient
	ttl time.Duration
}

// NewFilesystemRedisRepository creates a new filesystem repository
func NewFilesystemRedisRepository(rdb *common.RedisClient) FilesystemRepository {
	return &FilesystemRedisRepository{
		rdb: rdb,
		ttl: defaultTTL,
	}
}

// NewFilesystemRedisRepositoryWithTTL creates a repository with custom TTL
func NewFilesystemRedisRepositoryWithTTL(rdb *common.RedisClient, ttl time.Duration) FilesystemRepository {
	return &FilesystemRedisRepository{
		rdb: rdb,
		ttl: ttl,
	}
}

func (r *FilesystemRedisRepository) key(format, path string) string {
	return fmt.Sprintf(format, types.GeneratePathID(path))
}

// GetDirMeta retrieves directory metadata from cache
func (r *FilesystemRedisRepository) GetDirMeta(ctx context.Context, path string) (*types.DirMeta, error) {
	data, err := r.rdb.Get(ctx, r.key(keyDirMeta, path)).Bytes()
	if err == redis.Nil {
		return nil, nil // Cache miss
	}
	if err != nil {
		return nil, err
	}

	var meta types.DirMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// SaveDirMeta stores directory metadata in cache
func (r *FilesystemRedisRepository) SaveDirMeta(ctx context.Context, meta *types.DirMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, r.key(keyDirMeta, meta.Path), data, r.ttl).Err()
}

// DeleteDirMeta removes directory metadata from cache
func (r *FilesystemRedisRepository) DeleteDirMeta(ctx context.Context, path string) error {
	return r.rdb.Del(ctx, r.key(keyDirMeta, path)).Err()
}

// ListDir returns cached directory entries
func (r *FilesystemRedisRepository) ListDir(ctx context.Context, path string) ([]types.DirEntry, error) {
	data, err := r.rdb.Get(ctx, r.key(keyDirChildren, path)).Bytes()
	if err == redis.Nil {
		return nil, nil // Cache miss
	}
	if err != nil {
		return nil, err
	}

	var entries []types.DirEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

// GetFileMeta retrieves file metadata from cache
func (r *FilesystemRedisRepository) GetFileMeta(ctx context.Context, path string) (*types.FileMeta, error) {
	data, err := r.rdb.Get(ctx, r.key(keyFileMeta, path)).Bytes()
	if err == redis.Nil {
		return nil, nil // Cache miss
	}
	if err != nil {
		return nil, err
	}

	var meta types.FileMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// SaveFileMeta stores file metadata in cache
func (r *FilesystemRedisRepository) SaveFileMeta(ctx context.Context, meta *types.FileMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, r.key(keyFileMeta, meta.Path), data, r.ttl).Err()
}

// DeleteFileMeta removes file metadata from cache
func (r *FilesystemRedisRepository) DeleteFileMeta(ctx context.Context, path string) error {
	return r.rdb.Del(ctx, r.key(keyFileMeta, path)).Err()
}

// GetSymlink retrieves symlink target from cache
func (r *FilesystemRedisRepository) GetSymlink(ctx context.Context, path string) (string, error) {
	target, err := r.rdb.Get(ctx, r.key(keySymlink, path)).Result()
	if err == redis.Nil {
		return "", nil // Cache miss
	}
	return target, err
}

// SaveSymlink stores symlink target in cache
func (r *FilesystemRedisRepository) SaveSymlink(ctx context.Context, path, target string) error {
	return r.rdb.Set(ctx, r.key(keySymlink, path), target, r.ttl).Err()
}

// DeleteSymlink removes symlink from cache
func (r *FilesystemRedisRepository) DeleteSymlink(ctx context.Context, path string) error {
	return r.rdb.Del(ctx, r.key(keySymlink, path)).Err()
}

// Invalidate removes a specific path from all caches
func (r *FilesystemRedisRepository) Invalidate(ctx context.Context, path string) error {
	keys := []string{
		r.key(keyDirMeta, path),
		r.key(keyFileMeta, path),
		r.key(keySymlink, path),
		r.key(keyDirChildren, path),
	}
	return r.rdb.Del(ctx, keys...).Err()
}

// InvalidatePrefix removes all paths with a given prefix from cache
func (r *FilesystemRedisRepository) InvalidatePrefix(ctx context.Context, prefix string) error {
	// For efficiency, we invalidate the parent directory listing
	// Individual file caches will expire via TTL
	parent := parentPath(prefix)
	return r.rdb.Del(ctx, r.key(keyDirChildren, parent)).Err()
}

// parentPath returns the parent directory of a path
func parentPath(path string) string {
	if path == "/" || path == "" {
		return "/"
	}
	path = strings.TrimSuffix(path, "/")
	idx := strings.LastIndex(path, "/")
	if idx <= 0 {
		return "/"
	}
	return path[:idx]
}

// SaveDirListing caches a directory listing
func (r *FilesystemRedisRepository) SaveDirListing(ctx context.Context, path string, entries []types.DirEntry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, r.key(keyDirChildren, path), data, r.ttl).Err()
}
