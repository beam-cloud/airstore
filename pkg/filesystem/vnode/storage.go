package vnode

import (
	"context"
	"io/fs"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const storageRPCTimeout = 30 * time.Second

// StorageVNode handles S3-backed storage for any path via gRPC.
// Used as a fallback for paths not matched by specific vnodes.
type StorageVNode struct {
	client  pb.ContextServiceClient
	token   string
	cache   *MetadataCache
	handles map[FileHandle]string
	nextFH  FileHandle
	mu      sync.Mutex
}

func NewStorageVNode(conn *grpc.ClientConn, token string) *StorageVNode {
	return &StorageVNode{
		client:  pb.NewContextServiceClient(conn),
		token:   token,
		cache:   NewMetadataCache(),
		handles: make(map[FileHandle]string),
		nextFH:  1,
	}
}

func (s *StorageVNode) ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), storageRPCTimeout)
	if s.token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+s.token)
	}
	return ctx, cancel
}

// Prefix returns empty - this is a fallback handler
func (s *StorageVNode) Prefix() string    { return "" }
func (s *StorageVNode) Type() VNodeType { return VNodeWritable }

// rel returns the storage path (leading slash stripped)
func (s *StorageVNode) rel(path string) string {
	return strings.TrimPrefix(path, "/")
}

func (s *StorageVNode) Getattr(path string) (*FileInfo, error) {
	if path == "/" {
		return NewDirInfo(PathIno(path)), nil
	}

	if s.cache.IsNegative(path) {
		return nil, fs.ErrNotExist
	}
	if info := s.cache.GetInfo(path); info != nil {
		return info, nil
	}

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Stat(ctx, &pb.ContextStatRequest{Path: s.rel(path)})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		s.cache.SetNegative(path)
		return nil, fs.ErrNotExist
	}

	info := s.toFileInfo(path, resp.Info)
	s.cache.Set(path, info)
	return info, nil
}

func (s *StorageVNode) Readdir(path string) ([]DirEntry, error) {
	if cached := s.cache.Get(path); cached != nil && cached.Children != nil {
		return cached.Children, nil
	}

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.ReadDir(ctx, &pb.ContextReadDirRequest{Path: s.rel(path)})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, fs.ErrNotExist
	}

	entries := make([]DirEntry, 0, len(resp.Entries))
	childMeta := make(map[string]*FileInfo, len(resp.Entries))
	now := time.Now()

	for _, e := range resp.Entries {
		// Skip macOS AppleDouble files and reserved folders at root
		if strings.HasPrefix(e.Name, "._") {
			continue
		}
		if path == "/" && types.IsReservedFolder(e.Name) {
			continue
		}

		childPath := path + "/" + e.Name
		if path == "/" {
			childPath = "/" + e.Name
		}
		ino := PathIno(childPath)
		entries = append(entries, DirEntry{Name: e.Name, Mode: e.Mode, Ino: ino})

		mtime := now
		if e.Mtime > 0 {
			mtime = time.Unix(e.Mtime, 0)
		}
		childMeta[e.Name] = &FileInfo{
			Ino: ino, Size: e.Size, Mode: e.Mode, Nlink: 1,
			Uid: uint32(syscall.Getuid()), Gid: uint32(syscall.Getgid()),
			Atime: now, Mtime: mtime, Ctime: mtime,
		}
	}

	s.cache.SetWithChildren(path, entries, childMeta)
	return entries, nil
}

func (s *StorageVNode) Open(path string, flags int) (FileHandle, error) {
	if s.cache.IsNegative(path) {
		return 0, fs.ErrNotExist
	}
	if s.cache.GetInfo(path) != nil {
		return s.allocHandle(s.rel(path)), nil
	}

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Stat(ctx, &pb.ContextStatRequest{Path: s.rel(path)})
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		s.cache.SetNegative(path)
		return 0, fs.ErrNotExist
	}

	s.cache.Set(path, s.toFileInfo(path, resp.Info))
	return s.allocHandle(s.rel(path)), nil
}

func (s *StorageVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Read(ctx, &pb.ContextReadRequest{
		Path: s.rel(path), Offset: off, Length: int64(len(buf)),
	})
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		return 0, fs.ErrNotExist
	}
	return copy(buf, resp.Data), nil
}

func (s *StorageVNode) Create(path string, flags int, mode uint32) (FileHandle, error) {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Create(ctx, &pb.ContextCreateRequest{Path: s.rel(path), Mode: mode})
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		return 0, fs.ErrInvalid
	}

	s.cache.Invalidate(path)
	s.invalidateParent(path)
	return s.allocHandle(s.rel(path)), nil
}

func (s *StorageVNode) Write(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Write(ctx, &pb.ContextWriteRequest{
		Path: s.rel(path), Data: buf, Offset: off,
	})
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		return 0, fs.ErrInvalid
	}

	s.cache.Invalidate(path)
	return int(resp.Written), nil
}

func (s *StorageVNode) Truncate(path string, size int64, fh FileHandle) error {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Truncate(ctx, &pb.ContextTruncateRequest{Path: s.rel(path), Size: size})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	s.cache.Invalidate(path)
	return nil
}

func (s *StorageVNode) Mkdir(path string, mode uint32) error {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Mkdir(ctx, &pb.ContextMkdirRequest{Path: s.rel(path), Mode: mode})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	s.cache.Invalidate(path)
	s.invalidateParent(path)
	return nil
}

func (s *StorageVNode) Rmdir(path string) error {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Delete(ctx, &pb.ContextDeleteRequest{Path: s.rel(path)})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	s.cache.Invalidate(path)
	s.invalidateParent(path)
	return nil
}

func (s *StorageVNode) Unlink(path string) error {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Delete(ctx, &pb.ContextDeleteRequest{Path: s.rel(path)})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	s.cache.Invalidate(path)
	s.invalidateParent(path)
	return nil
}

// Rename moves or renames a file or directory
func (s *StorageVNode) Rename(oldpath, newpath string) error {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Rename(ctx, &pb.ContextRenameRequest{
		OldPath: s.rel(oldpath),
		NewPath: s.rel(newpath),
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	s.cache.Invalidate(oldpath)
	s.cache.Invalidate(newpath)
	s.invalidateParent(oldpath)
	s.invalidateParent(newpath)
	return nil
}

func (s *StorageVNode) Symlink(target, linkPath string) error {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Symlink(ctx, &pb.ContextSymlinkRequest{
		Target: target, LinkPath: s.rel(linkPath),
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	s.cache.Invalidate(linkPath)
	s.invalidateParent(linkPath)
	return nil
}

func (s *StorageVNode) Readlink(path string) (string, error) {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Readlink(ctx, &pb.ContextReadlinkRequest{Path: s.rel(path)})
	if err != nil {
		return "", err
	}
	if !resp.Ok {
		return "", fs.ErrInvalid
	}
	return resp.Target, nil
}

func (s *StorageVNode) Release(path string, fh FileHandle) error {
	s.mu.Lock()
	delete(s.handles, fh)
	s.mu.Unlock()
	return nil
}

func (s *StorageVNode) Fsync(path string, fh FileHandle) error {
	return nil
}

// helpers

func (s *StorageVNode) allocHandle(key string) FileHandle {
	s.mu.Lock()
	defer s.mu.Unlock()
	fh := s.nextFH
	s.nextFH++
	s.handles[fh] = key
	return fh
}

func (s *StorageVNode) invalidateParent(path string) {
	if idx := strings.LastIndex(path, "/"); idx > 0 {
		s.cache.Invalidate(path[:idx])
	} else {
		s.cache.Invalidate("/")
	}
}

func (s *StorageVNode) toFileInfo(path string, info *pb.FileInfo) *FileInfo {
	now := time.Now()
	mtime := now
	if info.Mtime > 0 {
		mtime = time.Unix(info.Mtime, 0)
	}
	return &FileInfo{
		Ino: PathIno(path), Size: info.Size, Mode: info.Mode, Nlink: 1,
		Uid: uint32(syscall.Getuid()), Gid: uint32(syscall.Getgid()),
		Atime: now, Mtime: mtime, Ctime: mtime,
	}
}
