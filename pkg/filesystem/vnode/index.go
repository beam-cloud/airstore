package vnode

import (
	"context"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/index"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// IndexVNode is a VirtualNode that reads from a local index instead of calling APIs.
// This provides instant access to pre-synced data for grep/find operations.
type IndexVNode struct {
	ReadOnlyBase // Embed read-only base for write operations

	store       index.IndexStore
	prefix      string // e.g., "/sources/gmail"
	integration string // e.g., "gmail"

	// Credential provider for fetching on-demand content
	getCreds func() *types.IntegrationCredentials

	// Content fetcher for items not fully in index
	fetchContent func(ctx context.Context, creds *types.IntegrationCredentials, entityID string) ([]byte, error)

	// Timeout for fetch operations
	fetchTimeout time.Duration
}

// IndexVNodeConfig configures an IndexVNode
type IndexVNodeConfig struct {
	Store       index.IndexStore
	Prefix      string // e.g., "/sources/gmail"
	Integration string // e.g., "gmail"

	// Optional: for fetching content not in index
	GetCreds     func() *types.IntegrationCredentials
	FetchContent func(ctx context.Context, creds *types.IntegrationCredentials, entityID string) ([]byte, error)
	FetchTimeout time.Duration
}

// NewIndexVNode creates a new index-backed VirtualNode
func NewIndexVNode(cfg IndexVNodeConfig) *IndexVNode {
	if cfg.FetchTimeout == 0 {
		cfg.FetchTimeout = 30 * time.Second
	}
	return &IndexVNode{
		store:        cfg.Store,
		prefix:       cfg.Prefix,
		integration:  cfg.Integration,
		getCreds:     cfg.GetCreds,
		fetchContent: cfg.FetchContent,
		fetchTimeout: cfg.FetchTimeout,
	}
}

// Prefix returns the path prefix this vnode handles
func (v *IndexVNode) Prefix() string {
	return v.prefix
}

// Getattr returns file/directory attributes
func (v *IndexVNode) Getattr(path string) (*FileInfo, error) {
	relPath := v.relativePath(path)

	// Root of this vnode
	if relPath == "" {
		return NewDirInfo(PathIno(path)), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to find entry by path
	entry, err := v.store.GetByPath(ctx, relPath)
	if err != nil {
		log.Warn().Err(err).Str("path", relPath).Msg("index getattr error")
		return nil, syscall.ENOENT
	}

	if entry == nil {
		// Check if this is a virtual directory (parent path exists but not this exact path)
		// by listing entries with this as parent
		entries, err := v.store.List(ctx, v.integration, relPath)
		if err == nil && len(entries) > 0 {
			return NewDirInfo(PathIno(path)), nil
		}
		return nil, syscall.ENOENT
	}

	if entry.IsDir() {
		return NewDirInfo(PathIno(path)), nil
	}

	return &FileInfo{
		Ino:   PathIno(path),
		Size:  entry.Size,
		Mode:  syscall.S_IFREG | 0644,
		Nlink: 1,
		Uid:   uint32(syscall.Getuid()),
		Gid:   uint32(syscall.Getgid()),
		Atime: entry.ModTime,
		Mtime: entry.ModTime,
		Ctime: entry.ModTime,
	}, nil
}

// Readdir lists directory contents
func (v *IndexVNode) Readdir(path string) ([]DirEntry, error) {
	relPath := v.relativePath(path)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entries, err := v.store.List(ctx, v.integration, relPath)
	if err != nil {
		log.Warn().Err(err).Str("path", relPath).Msg("index readdir error")
		return nil, syscall.EIO
	}

	// Build directory entries with deduplication for intermediate paths
	dirEntries := make([]DirEntry, 0, len(entries))
	seen := make(map[string]bool)

	for _, entry := range entries {
		name := entry.Name
		if seen[name] {
			continue
		}
		seen[name] = true

		mode := uint32(syscall.S_IFREG | 0644)
		if entry.IsDir() {
			mode = syscall.S_IFDIR | 0755
		}

		dirEntries = append(dirEntries, DirEntry{
			Name: name,
			Mode: mode,
			Ino:  PathIno(entry.Path),
		})
	}

	return dirEntries, nil
}

// Open opens a file for reading
func (v *IndexVNode) Open(path string, flags int) (FileHandle, error) {
	// Just verify the file exists
	if _, err := v.Getattr(path); err != nil {
		return 0, err
	}
	return 0, nil
}

// Read reads file content
func (v *IndexVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	relPath := v.relativePath(path)

	ctx, cancel := context.WithTimeout(context.Background(), v.fetchTimeout)
	defer cancel()

	// Try to find entry by path
	entry, err := v.store.GetByPath(ctx, relPath)
	if err != nil {
		log.Warn().Err(err).Str("path", relPath).Msg("index read error")
		return 0, syscall.EIO
	}

	if entry == nil {
		return 0, syscall.ENOENT
	}

	if entry.IsDir() {
		return 0, syscall.EISDIR
	}

	var content []byte

	// Check if content is in the index
	if entry.Body != "" {
		content = []byte(entry.Body)
	} else if entry.NeedsFetch() && v.fetchContent != nil && v.getCreds != nil {
		// Fetch content on-demand
		creds := v.getCreds()
		if creds != nil {
			fetched, err := v.fetchContent(ctx, creds, entry.EntityID)
			if err != nil {
				log.Warn().Err(err).Str("entity_id", entry.EntityID).Msg("failed to fetch content")
				return 0, syscall.EIO
			}
			content = fetched
		}
	}

	if content == nil {
		content = []byte{}
	}

	// Handle offset
	if off >= int64(len(content)) {
		return 0, nil
	}

	n := copy(buf, content[off:])
	return n, nil
}

// Readlink reads a symlink target (not used for index entries)
func (v *IndexVNode) Readlink(path string) (string, error) {
	return "", syscall.EINVAL
}

// relativePath strips the prefix from a path
func (v *IndexVNode) relativePath(path string) string {
	rel := strings.TrimPrefix(path, v.prefix)
	rel = strings.TrimPrefix(rel, "/")
	return rel
}

// Search performs a full-text search across the index
// Returns matching paths for use with grep-like operations
func (v *IndexVNode) Search(query string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entries, err := v.store.Search(ctx, v.integration, query)
	if err != nil {
		return nil, err
	}

	paths := make([]string, len(entries))
	for i, entry := range entries {
		paths[i] = v.prefix + "/" + entry.Path
	}

	return paths, nil
}
