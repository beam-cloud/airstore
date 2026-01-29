package services

import (
	"context"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

// FilesystemService provides gRPC filesystem metadata operations.
// This service is used by the FUSE client for caching metadata.
type FilesystemService struct {
	pb.UnimplementedFilesystemServiceServer
	store repository.FilesystemStore
}

// NewFilesystemService creates a new FilesystemService.
func NewFilesystemService(store repository.FilesystemStore) *FilesystemService {
	return &FilesystemService{store: store}
}

// Stat returns metadata for a path.
func (s *FilesystemService) Stat(ctx context.Context, req *pb.StatRequest) (*pb.StatResponse, error) {
	path := cleanFsPath(req.Path)

	// Try as directory first
	dirMeta, err := s.store.GetDirMeta(ctx, path)
	if err != nil {
		return &pb.StatResponse{Ok: false, Error: err.Error()}, nil
	}
	if dirMeta != nil {
		return &pb.StatResponse{
			Ok: true,
			Info: &pb.FileStat{
				Path:  path,
				Name:  pathName(path),
				Mode:  dirMeta.Mode,
				Mtime: dirMeta.Mtime.Unix(),
				IsDir: true,
			},
		}, nil
	}

	// Try as file
	fileMeta, err := s.store.GetFileMeta(ctx, path)
	if err != nil {
		return &pb.StatResponse{Ok: false, Error: err.Error()}, nil
	}
	if fileMeta != nil {
		return &pb.StatResponse{
			Ok: true,
			Info: &pb.FileStat{
				Path:  path,
				Name:  pathName(path),
				Size:  fileMeta.Size,
				Mode:  fileMeta.Mode,
				Mtime: fileMeta.Mtime.Unix(),
				IsDir: false,
			},
		}, nil
	}

	// Try as symlink
	target, err := s.store.GetSymlink(ctx, path)
	if err != nil {
		return &pb.StatResponse{Ok: false, Error: err.Error()}, nil
	}
	if target != "" {
		return &pb.StatResponse{
			Ok: true,
			Info: &pb.FileStat{
				Path:          path,
				Name:          pathName(path),
				Mode:          syscall.S_IFLNK | 0777,
				IsSymlink:     true,
				SymlinkTarget: target,
			},
		}, nil
	}

	return &pb.StatResponse{Ok: false, Error: "not found"}, nil
}

// ReadDir returns directory entries.
func (s *FilesystemService) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	path := cleanFsPath(req.Path)

	entries, err := s.store.ListDir(ctx, path)
	if err != nil {
		return &pb.ReadDirResponse{Ok: false, Error: err.Error()}, nil
	}
	if entries == nil {
		return &pb.ReadDirResponse{Ok: false, Error: "not found"}, nil
	}

	protoEntries := make([]*pb.DirEntry, len(entries))
	for i, e := range entries {
		protoEntries[i] = &pb.DirEntry{
			Name:  e.Name,
			Mode:  e.Mode,
			Size:  e.Size,
			Mtime: e.Mtime,
			IsDir: e.Mode&syscall.S_IFDIR != 0,
		}
	}

	return &pb.ReadDirResponse{Ok: true, Entries: protoEntries}, nil
}

// Mkdir creates a directory.
func (s *FilesystemService) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.MkdirResponse, error) {
	path := cleanFsPath(req.Path)

	mode := req.Mode
	if mode == 0 {
		mode = syscall.S_IFDIR | 0755
	}

	meta := &types.DirMeta{
		Path:  path,
		Mode:  mode,
		Uid:   uint32(syscall.Getuid()),
		Gid:   uint32(syscall.Getgid()),
		Mtime: time.Now(),
	}

	if err := s.store.SaveDirMeta(ctx, meta); err != nil {
		return &pb.MkdirResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.MkdirResponse{Ok: true}, nil
}

// Create creates a file.
func (s *FilesystemService) Create(ctx context.Context, req *pb.CreateRequest) (*pb.CreateResponse, error) {
	path := cleanFsPath(req.Path)

	mode := req.Mode
	if mode == 0 {
		mode = syscall.S_IFREG | 0644
	}

	meta := &types.FileMeta{
		Path:  path,
		Size:  0,
		Mode:  mode,
		Mtime: time.Now(),
	}

	if err := s.store.SaveFileMeta(ctx, meta); err != nil {
		return &pb.CreateResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.CreateResponse{Ok: true}, nil
}

// Rename renames a file or directory.
func (s *FilesystemService) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.RenameResponse, error) {
	oldPath := cleanFsPath(req.OldPath)
	newPath := cleanFsPath(req.NewPath)

	// Try to rename directory
	dirMeta, err := s.store.GetDirMeta(ctx, oldPath)
	if err != nil {
		return &pb.RenameResponse{Ok: false, Error: err.Error()}, nil
	}
	if dirMeta != nil {
		dirMeta.Path = newPath
		dirMeta.Mtime = time.Now()
		if err := s.store.SaveDirMeta(ctx, dirMeta); err != nil {
			return &pb.RenameResponse{Ok: false, Error: err.Error()}, nil
		}
		if err := s.store.DeleteDirMeta(ctx, oldPath); err != nil {
			return &pb.RenameResponse{Ok: false, Error: err.Error()}, nil
		}
		return &pb.RenameResponse{Ok: true}, nil
	}

	// Try to rename file
	fileMeta, err := s.store.GetFileMeta(ctx, oldPath)
	if err != nil {
		return &pb.RenameResponse{Ok: false, Error: err.Error()}, nil
	}
	if fileMeta != nil {
		fileMeta.Path = newPath
		fileMeta.Mtime = time.Now()
		if err := s.store.SaveFileMeta(ctx, fileMeta); err != nil {
			return &pb.RenameResponse{Ok: false, Error: err.Error()}, nil
		}
		if err := s.store.DeleteFileMeta(ctx, oldPath); err != nil {
			return &pb.RenameResponse{Ok: false, Error: err.Error()}, nil
		}
		return &pb.RenameResponse{Ok: true}, nil
	}

	return &pb.RenameResponse{Ok: false, Error: "not found"}, nil
}

// Remove removes a file or directory.
func (s *FilesystemService) Remove(ctx context.Context, req *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	path := cleanFsPath(req.Path)

	// Try to remove directory
	if err := s.store.DeleteDirMeta(ctx, path); err != nil {
		// Continue - might be a file
	}

	// Try to remove file
	if err := s.store.DeleteFileMeta(ctx, path); err != nil {
		// Continue - might be a symlink
	}

	// Try to remove symlink
	if err := s.store.DeleteSymlink(ctx, path); err != nil {
		return &pb.RemoveResponse{Ok: false, Error: err.Error()}, nil
	}

	// Invalidate path in cache
	_ = s.store.InvalidatePath(ctx, path)

	return &pb.RemoveResponse{Ok: true}, nil
}

// Chmod changes file permissions.
func (s *FilesystemService) Chmod(ctx context.Context, req *pb.ChmodRequest) (*pb.ChmodResponse, error) {
	path := cleanFsPath(req.Path)

	// Try directory
	dirMeta, err := s.store.GetDirMeta(ctx, path)
	if err != nil {
		return &pb.ChmodResponse{Ok: false, Error: err.Error()}, nil
	}
	if dirMeta != nil {
		dirMeta.Mode = (dirMeta.Mode & syscall.S_IFMT) | (req.Mode & 0777)
		dirMeta.Mtime = time.Now()
		if err := s.store.SaveDirMeta(ctx, dirMeta); err != nil {
			return &pb.ChmodResponse{Ok: false, Error: err.Error()}, nil
		}
		return &pb.ChmodResponse{Ok: true}, nil
	}

	// Try file
	fileMeta, err := s.store.GetFileMeta(ctx, path)
	if err != nil {
		return &pb.ChmodResponse{Ok: false, Error: err.Error()}, nil
	}
	if fileMeta != nil {
		fileMeta.Mode = (fileMeta.Mode & syscall.S_IFMT) | (req.Mode & 0777)
		fileMeta.Mtime = time.Now()
		if err := s.store.SaveFileMeta(ctx, fileMeta); err != nil {
			return &pb.ChmodResponse{Ok: false, Error: err.Error()}, nil
		}
		return &pb.ChmodResponse{Ok: true}, nil
	}

	return &pb.ChmodResponse{Ok: false, Error: "not found"}, nil
}

// cleanFsPath normalizes a path for the filesystem.
func cleanFsPath(path string) string {
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return strings.TrimSuffix(path, "/")
}

// pathName returns the last component of a path.
func pathName(path string) string {
	if path == "/" || path == "" {
		return "/"
	}
	parts := strings.Split(strings.TrimSuffix(path, "/"), "/")
	return parts[len(parts)-1]
}
