package filesystem

import (
	"context"
	"errors"
	"time"

	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// GRPCConfig holds configuration for connecting to the gateway
type GRPCConfig struct {
	GatewayAddr string
	Token       string
}

// GRPCMetadataEngine implements MetadataEngine via gRPC to the gateway
type GRPCMetadataEngine struct {
	conn   *grpc.ClientConn
	client pb.FilesystemServiceClient
	token  string
}

// NewGRPCMetadataEngine creates a new gRPC-based metadata engine
func NewGRPCMetadataEngine(cfg GRPCConfig) (*GRPCMetadataEngine, error) {
	conn, err := grpc.NewClient(
		cfg.GatewayAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &GRPCMetadataEngine{
		conn:   conn,
		client: pb.NewFilesystemServiceClient(conn),
		token:  cfg.Token,
	}, nil
}

func (m *GRPCMetadataEngine) ctx() context.Context {
	ctx := context.Background()
	if m.token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+m.token)
	}
	return ctx
}

func (m *GRPCMetadataEngine) GetDirectoryAccessMetadata(pid, name string) (*DirectoryAccessMetadata, error) {
	resp, err := m.client.GetDirectoryAccess(m.ctx(), &pb.GetDirectoryAccessRequest{
		Pid:  pid,
		Name: name,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, errors.New(resp.Error)
	}

	meta := &DirectoryAccessMetadata{
		PID:        resp.Metadata.Pid,
		ID:         resp.Metadata.Id,
		Permission: resp.Metadata.Permission,
		RenameList: resp.Metadata.RenameList,
	}

	if resp.Metadata.BackPointer != nil {
		meta.BackPointer = &BackPointer{
			BirthParentID: resp.Metadata.BackPointer.BirthParentId,
			NameVersion:   int(resp.Metadata.BackPointer.NameVersion),
		}
	}

	return meta, nil
}

func (m *GRPCMetadataEngine) SaveDirectoryAccessMetadata(meta *DirectoryAccessMetadata) error {
	pbMeta := &pb.DirectoryAccessMetadata{
		Pid:        meta.PID,
		Id:         meta.ID,
		Permission: meta.Permission,
		RenameList: meta.RenameList,
	}

	if meta.BackPointer != nil {
		pbMeta.BackPointer = &pb.BackPointer{
			BirthParentId: meta.BackPointer.BirthParentID,
			NameVersion:   int32(meta.BackPointer.NameVersion),
		}
	}

	resp, err := m.client.SaveDirectoryAccess(m.ctx(), &pb.SaveDirectoryAccessRequest{
		Metadata: pbMeta,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

func (m *GRPCMetadataEngine) GetDirectoryContentMetadata(id string) (*DirectoryContentMetadata, error) {
	resp, err := m.client.GetDirectoryContent(m.ctx(), &pb.GetDirectoryContentRequest{
		Id: id,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, errors.New(resp.Error)
	}

	timestamps := make(map[string]time.Time)
	for k, v := range resp.Metadata.Timestamps {
		timestamps[k] = time.Unix(v, 0)
	}

	return &DirectoryContentMetadata{
		Id:         resp.Metadata.Id,
		EntryList:  resp.Metadata.EntryList,
		Timestamps: timestamps,
	}, nil
}

func (m *GRPCMetadataEngine) SaveDirectoryContentMetadata(meta *DirectoryContentMetadata) error {
	timestamps := make(map[string]int64)
	for k, v := range meta.Timestamps {
		timestamps[k] = v.Unix()
	}

	resp, err := m.client.SaveDirectoryContent(m.ctx(), &pb.SaveDirectoryContentRequest{
		Metadata: &pb.DirectoryContentMetadata{
			Id:         meta.Id,
			EntryList:  meta.EntryList,
			Timestamps: timestamps,
		},
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

func (m *GRPCMetadataEngine) GetFileMetadata(pid, name string) (*FileMetadata, error) {
	resp, err := m.client.GetFileMetadata(m.ctx(), &pb.GetFileMetadataRequest{
		Pid:  pid,
		Name: name,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, errors.New(resp.Error)
	}

	return &FileMetadata{
		ID:       resp.Metadata.Id,
		PID:      resp.Metadata.Pid,
		Name:     resp.Metadata.Name,
		FileData: resp.Metadata.FileData,
	}, nil
}

func (m *GRPCMetadataEngine) SaveFileMetadata(meta *FileMetadata) error {
	resp, err := m.client.SaveFileMetadata(m.ctx(), &pb.SaveFileMetadataRequest{
		Metadata: &pb.FileMetadata{
			Id:       meta.ID,
			Pid:      meta.PID,
			Name:     meta.Name,
			FileData: meta.FileData,
		},
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

func (m *GRPCMetadataEngine) ListDirectory(path string) []DirEntry {
	resp, err := m.client.ListDirectory(m.ctx(), &pb.ListDirectoryRequest{
		Path: path,
	})
	if err != nil || !resp.Ok {
		return nil
	}

	entries := make([]DirEntry, len(resp.Entries))
	for i, e := range resp.Entries {
		entries[i] = DirEntry{Name: e.Name, Mode: e.Mode, Ino: e.Ino}
	}
	return entries
}

func (m *GRPCMetadataEngine) RenameDirectory(oldPID, oldName, newPID, newName string, version int) error {
	resp, err := m.client.RenameDirectory(m.ctx(), &pb.RenameDirectoryRequest{
		OldPid:  oldPID,
		OldName: oldName,
		NewPid:  newPID,
		NewName: newName,
		Version: int32(version),
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

func (m *GRPCMetadataEngine) DeleteDirectory(parentID, name string, version int) error {
	resp, err := m.client.DeleteDirectory(m.ctx(), &pb.DeleteDirectoryRequest{
		ParentId: parentID,
		Name:     name,
		Version:  int32(version),
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

func (m *GRPCMetadataEngine) Close() error {
	return m.conn.Close()
}

// Conn returns the underlying gRPC connection for sharing with other services
func (m *GRPCMetadataEngine) Conn() *grpc.ClientConn {
	return m.conn
}
