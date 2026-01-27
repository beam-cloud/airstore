package services

import (
	"context"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

type FilesystemService struct {
	pb.UnimplementedFilesystemServiceServer
	repo repository.FilesystemRepository
}

func NewFilesystemService(repo repository.FilesystemRepository) *FilesystemService {
	return &FilesystemService{repo: repo}
}

// GetDirectoryAccess retrieves directory access metadata
func (s *FilesystemService) GetDirectoryAccess(ctx context.Context, req *pb.GetDirectoryAccessRequest) (*pb.GetDirectoryAccessResponse, error) {
	path := req.Pid + "/" + req.Name
	if req.Pid == "" {
		path = "/" + req.Name
	}

	meta, err := s.repo.GetDirMeta(ctx, path)
	if err != nil {
		return &pb.GetDirectoryAccessResponse{Ok: false, Error: err.Error()}, nil
	}
	if meta == nil {
		return &pb.GetDirectoryAccessResponse{Ok: false, Error: "not found"}, nil
	}

	return &pb.GetDirectoryAccessResponse{
		Ok: true,
		Metadata: &pb.DirectoryAccessMetadata{
			Pid:        req.Pid,
			Id:         types.GeneratePathID(path),
			Permission: meta.Mode,
		},
	}, nil
}

// SaveDirectoryAccess saves directory access metadata
func (s *FilesystemService) SaveDirectoryAccess(ctx context.Context, req *pb.SaveDirectoryAccessRequest) (*pb.SaveDirectoryAccessResponse, error) {
	path := req.Metadata.Pid + "/" + req.Metadata.Id
	meta := &types.DirMeta{
		Path:  path,
		Mode:  req.Metadata.Permission,
		Uid:   uint32(syscall.Getuid()),
		Gid:   uint32(syscall.Getgid()),
		Mtime: time.Now(),
	}

	if err := s.repo.SaveDirMeta(ctx, meta); err != nil {
		return &pb.SaveDirectoryAccessResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.SaveDirectoryAccessResponse{Ok: true}, nil
}

// GetDirectoryContent retrieves directory content metadata
func (s *FilesystemService) GetDirectoryContent(ctx context.Context, req *pb.GetDirectoryContentRequest) (*pb.GetDirectoryContentResponse, error) {
	// The ID is the path hash - we need to look up by path
	// For now, use the ID as-is since this is legacy compatibility
	entries, err := s.repo.ListDir(ctx, req.Id)
	if err != nil {
		return &pb.GetDirectoryContentResponse{Ok: false, Error: err.Error()}, nil
	}

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}

	return &pb.GetDirectoryContentResponse{
		Ok: true,
		Metadata: &pb.DirectoryContentMetadata{
			Id:         req.Id,
			EntryList:  names,
			Timestamps: make(map[string]int64),
		},
	}, nil
}

// SaveDirectoryContent saves directory content metadata
func (s *FilesystemService) SaveDirectoryContent(ctx context.Context, req *pb.SaveDirectoryContentRequest) (*pb.SaveDirectoryContentResponse, error) {
	path := req.Metadata.Id // Using ID as path for legacy compat

	entries := make([]types.DirEntry, len(req.Metadata.EntryList))
	for i, name := range req.Metadata.EntryList {
		entries[i] = types.DirEntry{Name: name, Mode: syscall.S_IFREG | 0644}
	}

	meta := &types.DirMeta{
		Path:     path,
		Mode:     syscall.S_IFDIR | 0755,
		Children: req.Metadata.EntryList,
		Mtime:    time.Now(),
	}

	if err := s.repo.SaveDirMeta(ctx, meta); err != nil {
		return &pb.SaveDirectoryContentResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.SaveDirectoryContentResponse{Ok: true}, nil
}

// GetFileMetadata retrieves file metadata
func (s *FilesystemService) GetFileMetadata(ctx context.Context, req *pb.GetFileMetadataRequest) (*pb.GetFileMetadataResponse, error) {
	path := req.Pid + "/" + req.Name

	meta, err := s.repo.GetFileMeta(ctx, path)
	if err != nil {
		return &pb.GetFileMetadataResponse{Ok: false, Error: err.Error()}, nil
	}
	if meta == nil {
		return &pb.GetFileMetadataResponse{Ok: false, Error: "not found"}, nil
	}

	return &pb.GetFileMetadataResponse{
		Ok: true,
		Metadata: &pb.FileMetadata{
			Id:   types.GeneratePathID(path),
			Pid:  req.Pid,
			Name: req.Name,
		},
	}, nil
}

// SaveFileMetadata saves file metadata
func (s *FilesystemService) SaveFileMetadata(ctx context.Context, req *pb.SaveFileMetadataRequest) (*pb.SaveFileMetadataResponse, error) {
	path := req.Metadata.Pid + "/" + req.Metadata.Name

	meta := &types.FileMeta{
		Path:  path,
		Size:  int64(len(req.Metadata.FileData)),
		Mode:  syscall.S_IFREG | 0644,
		Mtime: time.Now(),
	}

	if err := s.repo.SaveFileMeta(ctx, meta); err != nil {
		return &pb.SaveFileMetadataResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.SaveFileMetadataResponse{Ok: true}, nil
}

// ListDirectory lists directory entries
func (s *FilesystemService) ListDirectory(ctx context.Context, req *pb.ListDirectoryRequest) (*pb.ListDirectoryResponse, error) {
	entries, err := s.repo.ListDir(ctx, req.Path)
	if err != nil {
		return &pb.ListDirectoryResponse{Ok: false, Error: err.Error()}, nil
	}

	protoEntries := make([]*pb.DirEntry, len(entries))
	for i, e := range entries {
		protoEntries[i] = &pb.DirEntry{
			Name: e.Name,
			Mode: e.Mode,
		}
	}

	return &pb.ListDirectoryResponse{Ok: true, Entries: protoEntries}, nil
}

// RenameDirectory renames a directory
func (s *FilesystemService) RenameDirectory(ctx context.Context, req *pb.RenameDirectoryRequest) (*pb.RenameDirectoryResponse, error) {
	oldPath := req.OldPid + "/" + req.OldName
	newPath := req.NewPid + "/" + req.NewName

	// Get old metadata
	meta, err := s.repo.GetDirMeta(ctx, oldPath)
	if err != nil {
		return &pb.RenameDirectoryResponse{Ok: false, Error: err.Error()}, nil
	}
	if meta == nil {
		return &pb.RenameDirectoryResponse{Ok: false, Error: "not found"}, nil
	}

	// Save to new path
	meta.Path = newPath
	if err := s.repo.SaveDirMeta(ctx, meta); err != nil {
		return &pb.RenameDirectoryResponse{Ok: false, Error: err.Error()}, nil
	}

	// Delete old path
	if err := s.repo.DeleteDirMeta(ctx, oldPath); err != nil {
		return &pb.RenameDirectoryResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.RenameDirectoryResponse{Ok: true}, nil
}

// DeleteDirectory deletes a directory
func (s *FilesystemService) DeleteDirectory(ctx context.Context, req *pb.DeleteDirectoryRequest) (*pb.DeleteDirectoryResponse, error) {
	path := req.ParentId + "/" + req.Name

	if err := s.repo.DeleteDirMeta(ctx, path); err != nil {
		return &pb.DeleteDirectoryResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteDirectoryResponse{Ok: true}, nil
}
