// Package vnode provides virtual filesystem nodes for the FUSE layer.
package vnode

import (
	"context"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/index"
	"github.com/rs/zerolog/log"
)

// Syncer triggers lazy sync for integrations
type Syncer interface {
	EnsureSynced(ctx context.Context, integration string)
	IsSynced(integration string) bool
}

// LazySourcesVNode provides local index caching with on-demand sync.
// On first access to an integration, triggers background sync.
// Uses local SQLite for reads when synced, falls back to gateway.
type LazySourcesVNode struct {
	ReadOnlyBase
	store    index.IndexStore
	fallback *SourcesVNodeGRPC
	syncer   Syncer
}

// NewLazySourcesVNode creates a lazy sources vnode
func NewLazySourcesVNode(store index.IndexStore, fallback *SourcesVNodeGRPC, syncer Syncer) *LazySourcesVNode {
	return &LazySourcesVNode{store: store, fallback: fallback, syncer: syncer}
}

func (v *LazySourcesVNode) Prefix() string { return SourcesPath }

func (v *LazySourcesVNode) parse(path string) (integration, rel string) {
	p := strings.TrimPrefix(strings.TrimPrefix(path, SourcesPath), "/")
	if i := strings.Index(p, "/"); i >= 0 {
		return p[:i], p[i+1:]
	}
	return p, ""
}

func (v *LazySourcesVNode) Getattr(path string) (*FileInfo, error) {
	integration, rel := v.parse(path)
	if integration == "" || rel == "" {
		return v.fallback.Getattr(path)
	}

	v.syncer.EnsureSynced(context.Background(), integration)

	if v.syncer.IsSynced(integration) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if e, _ := v.store.GetByPath(ctx, rel); e != nil {
			log.Debug().Str("path", rel).Msg("local hit: getattr")
			return v.entryToInfo(path, e), nil
		}
		if entries, _ := v.store.List(ctx, integration, rel); len(entries) > 0 {
			return NewDirInfo(PathIno(path)), nil
		}
	}
	return v.fallback.Getattr(path)
}

func (v *LazySourcesVNode) Readdir(path string) ([]DirEntry, error) {
	integration, rel := v.parse(path)
	if integration == "" {
		return v.fallback.Readdir(path)
	}

	v.syncer.EnsureSynced(context.Background(), integration)

	if v.syncer.IsSynced(integration) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if entries, _ := v.store.List(ctx, integration, rel); len(entries) > 0 {
			log.Debug().Str("path", rel).Int("n", len(entries)).Msg("local hit: readdir")
			return v.entriesToDir(entries), nil
		}
	}
	return v.fallback.Readdir(path)
}

func (v *LazySourcesVNode) Open(path string, flags int) (FileHandle, error) {
	_, err := v.Getattr(path)
	return 0, err
}

func (v *LazySourcesVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	integration, rel := v.parse(path)
	if integration == "" {
		return 0, syscall.EISDIR
	}

	if v.syncer.IsSynced(integration) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if e, _ := v.store.GetByPath(ctx, rel); e != nil && e.Body != "" {
			log.Debug().Str("path", rel).Msg("local hit: read")
			data := []byte(e.Body)
			if off >= int64(len(data)) {
				return 0, nil
			}
			return copy(buf, data[off:]), nil
		}
	}
	return v.fallback.Read(path, buf, off, fh)
}

func (v *LazySourcesVNode) Readlink(path string) (string, error) {
	return v.fallback.Readlink(path)
}

func (v *LazySourcesVNode) entryToInfo(path string, e *index.IndexEntry) *FileInfo {
	mode := uint32(syscall.S_IFREG | 0644)
	if e.IsDir() {
		mode = syscall.S_IFDIR | 0755
	}
	return &FileInfo{
		Ino: PathIno(path), Size: e.Size, Mode: mode, Nlink: 1,
		Uid: uint32(syscall.Getuid()), Gid: uint32(syscall.Getgid()),
		Atime: e.ModTime, Mtime: e.ModTime, Ctime: e.ModTime,
	}
}

func (v *LazySourcesVNode) entriesToDir(entries []*index.IndexEntry) []DirEntry {
	seen := make(map[string]bool)
	var result []DirEntry
	for _, e := range entries {
		if seen[e.Name] {
			continue
		}
		seen[e.Name] = true
		mode := uint32(syscall.S_IFREG | 0644)
		if e.IsDir() {
			mode = syscall.S_IFDIR | 0755
		}
		result = append(result, DirEntry{Name: e.Name, Mode: mode, Ino: PathIno(e.Path)})
	}
	return result
}
