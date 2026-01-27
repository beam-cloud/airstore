package repository

import (
	"context"
	"sync"

	"github.com/beam-cloud/airstore/pkg/types"
)

// FilesystemMemoryRepository implements FilesystemRepository using in-memory storage.
// This is used for local mode where we don't have Redis.
type FilesystemMemoryRepository struct {
	mu       sync.RWMutex
	dirs     map[string]*types.DirMeta
	files    map[string]*types.FileMeta
	symlinks map[string]string
	listings map[string][]types.DirEntry
}

// NewFilesystemMemoryRepository creates a new in-memory filesystem repository
func NewFilesystemMemoryRepository() FilesystemRepository {
	return &FilesystemMemoryRepository{
		dirs:     make(map[string]*types.DirMeta),
		files:    make(map[string]*types.FileMeta),
		symlinks: make(map[string]string),
		listings: make(map[string][]types.DirEntry),
	}
}

func (r *FilesystemMemoryRepository) GetDirMeta(ctx context.Context, path string) (*types.DirMeta, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	meta, ok := r.dirs[path]
	if !ok {
		return nil, nil
	}
	return meta, nil
}

func (r *FilesystemMemoryRepository) SaveDirMeta(ctx context.Context, meta *types.DirMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dirs[meta.Path] = meta
	return nil
}

func (r *FilesystemMemoryRepository) DeleteDirMeta(ctx context.Context, path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.dirs, path)
	return nil
}

func (r *FilesystemMemoryRepository) ListDir(ctx context.Context, path string) ([]types.DirEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entries, ok := r.listings[path]
	if !ok {
		return nil, nil
	}
	return entries, nil
}

func (r *FilesystemMemoryRepository) GetFileMeta(ctx context.Context, path string) (*types.FileMeta, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	meta, ok := r.files[path]
	if !ok {
		return nil, nil
	}
	return meta, nil
}

func (r *FilesystemMemoryRepository) SaveFileMeta(ctx context.Context, meta *types.FileMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.files[meta.Path] = meta
	return nil
}

func (r *FilesystemMemoryRepository) DeleteFileMeta(ctx context.Context, path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.files, path)
	return nil
}

func (r *FilesystemMemoryRepository) GetSymlink(ctx context.Context, path string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	target, ok := r.symlinks[path]
	if !ok {
		return "", nil
	}
	return target, nil
}

func (r *FilesystemMemoryRepository) SaveSymlink(ctx context.Context, path, target string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.symlinks[path] = target
	return nil
}

func (r *FilesystemMemoryRepository) DeleteSymlink(ctx context.Context, path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.symlinks, path)
	return nil
}

func (r *FilesystemMemoryRepository) Invalidate(ctx context.Context, path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.dirs, path)
	delete(r.files, path)
	delete(r.symlinks, path)
	delete(r.listings, path)
	return nil
}

func (r *FilesystemMemoryRepository) InvalidatePrefix(ctx context.Context, prefix string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Invalidate parent directory listing
	parent := parentPath(prefix)
	delete(r.listings, parent)
	return nil
}

// SaveDirListing caches a directory listing
func (r *FilesystemMemoryRepository) SaveDirListing(ctx context.Context, path string, entries []types.DirEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listings[path] = entries
	return nil
}
