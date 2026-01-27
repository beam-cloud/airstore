package vnode

import (
	"context"
	"io/fs"
	"strings"
	"syscall"
	"time"

	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const sourcesRpcTimeout = 30 * time.Second // Per-RPC timeout for source operations

// SourcesVNodeGRPC implements VirtualNode for read-only integration sources.
type SourcesVNodeGRPC struct {
	ReadOnlyBase // Embeds read-only defaults for write operations

	client  pb.SourceServiceClient
	token   string
	cache   *MetadataCache
}

// NewSourcesVNodeGRPC creates a new sources virtual node
func NewSourcesVNodeGRPC(conn *grpc.ClientConn, token string) *SourcesVNodeGRPC {
	return &SourcesVNodeGRPC{
		client: pb.NewSourceServiceClient(conn),
		token:  token,
		cache:  NewMetadataCache(),
	}
}

// ctx returns a context with auth token and timeout for RPC calls
func (s *SourcesVNodeGRPC) ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), sourcesRpcTimeout)
	if s.token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+s.token)
	}
	return ctx, cancel
}

func (s *SourcesVNodeGRPC) Prefix() string { return SourcesPath }

func (s *SourcesVNodeGRPC) rel(path string) string {
	return strings.TrimPrefix(strings.TrimPrefix(path, SourcesPath), "/")
}

// Getattr returns file attributes with caching
func (s *SourcesVNodeGRPC) Getattr(path string) (*FileInfo, error) {
	if s.cache.IsNegative(path) {
		return nil, fs.ErrNotExist
	}
	if info := s.cache.GetInfo(path); info != nil {
		return info, nil
	}

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Stat(ctx, &pb.SourceStatRequest{Path: s.rel(path)})
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

// Readdir returns directory entries, caching child metadata
func (s *SourcesVNodeGRPC) Readdir(path string) ([]DirEntry, error) {
	if cached := s.cache.Get(path); cached != nil && cached.Children != nil {
		return cached.Children, nil
	}

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.ReadDir(ctx, &pb.SourceReadDirRequest{Path: s.rel(path)})
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
		childPath := path + "/" + e.Name
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

// Open opens a file
func (s *SourcesVNodeGRPC) Open(path string, flags int) (FileHandle, error) {
	// Check cache first to avoid unnecessary RPC
	if s.cache.IsNegative(path) {
		return 0, fs.ErrNotExist
	}
	if s.cache.GetInfo(path) != nil {
		return 0, nil // No file handle needed for read-only
	}

	// Cache miss - verify file exists
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Stat(ctx, &pb.SourceStatRequest{Path: s.rel(path)})
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		s.cache.SetNegative(path)
		return 0, fs.ErrNotExist
	}

	// Cache the result
	s.cache.Set(path, s.toFileInfo(path, resp.Info))
	return 0, nil
}

// Read reads file data
func (s *SourcesVNodeGRPC) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Read(ctx, &pb.SourceReadRequest{
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

// Readlink reads symlink target
func (s *SourcesVNodeGRPC) Readlink(path string) (string, error) {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Readlink(ctx, &pb.SourceReadlinkRequest{Path: s.rel(path)})
	if err != nil {
		return "", err
	}
	if !resp.Ok {
		return "", fs.ErrNotExist
	}
	return resp.Target, nil
}

// toFileInfo converts proto FileInfo to vnode FileInfo
func (s *SourcesVNodeGRPC) toFileInfo(path string, info *pb.SourceFileInfo) *FileInfo {
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
